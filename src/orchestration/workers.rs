//! Worker loops. Each one is run by a thread each, along with the initial setup
//! function.

use std::collections::VecDeque;

use crossbeam_channel::{Receiver, Select, Sender};
use log::{debug, error, info, warn};

use crate::generator::generator::MatchmakingRequest;
use crate::locations::{MATCHMAKER_EGRESS_SOCKET_PATH, MATCHMAKER_INGRESS_SOCKET_PATH};
use crate::matchmaking::config::MatchmakingQueueParameters;
use crate::matchmaking::ground_allocation::GroundAllocator;
use crate::matchmaking::scheduling::scheduling_table::{SchedulerDiagnostics, SchedulingTable};
use crate::orchestration::config::SetupParameters;
use crate::orchestration::events::{ControlEvent, GroundAllocationEvent, SchedulerEvent};
use crate::orchestration::io::{spawn_socket_listener, spawn_socket_sender};
use crate::validator::validator::MatchmakingAssignment;

pub struct ChannelPair<Tx, Rx> {
    pub tx: Sender<Tx>,
    pub rx: Receiver<Rx>,
}

pub struct SchedulerInitialisation {
    pub ground_allocator_thread_count: usize,
    pub ground_allocation_channel_pairs: Box<[ChannelPair<SchedulerEvent, GroundAllocationEvent>]>,
    pub matchmaking_queue_parameters: MatchmakingQueueParameters,
    pub ingress_receiver: Receiver<Vec<MatchmakingRequest>>,
    control_receiver: Receiver<ControlEvent>,
}

pub struct GroundAllocatorInitialisation {
    pub id: usize,
    pub channel_pair: ChannelPair<GroundAllocationEvent, SchedulerEvent>,
    pub egress_sender: Sender<Vec<MatchmakingAssignment>>,
}

pub fn setup(setup_parameters: SetupParameters) {
    let SetupParameters {
        ground_allocator_thread_count,
        matchmaking_queue_parameters,
    } = setup_parameters;

    // setup and spawn ingress and egress threads
    let (_request_ingress_join_handle, ingress_receiver) =
        spawn_socket_listener(MATCHMAKER_INGRESS_SOCKET_PATH);
    let (_request_egress_join_handle, egress_sender) =
        spawn_socket_sender(MATCHMAKER_EGRESS_SOCKET_PATH);

    // setup and spawn ground allocator threads
    let mut scheduler_channel_pairs = Vec::with_capacity(ground_allocator_thread_count);
    let mut ground_allocator_join_handles = Vec::with_capacity(ground_allocator_thread_count);
    for id in 0..ground_allocator_thread_count {
        let (ground_allocator_sender, scheduler_receiver) =
            crossbeam_channel::bounded::<GroundAllocationEvent>(1);
        let (scheduler_sender, ground_allocator_receiver) =
            crossbeam_channel::bounded::<SchedulerEvent>(1);

        let ground_allocator_channel_pair = ChannelPair {
            tx: ground_allocator_sender,
            rx: ground_allocator_receiver,
        };
        let scheduler_channel_pair = ChannelPair {
            tx: scheduler_sender,
            rx: scheduler_receiver,
        };

        scheduler_channel_pairs.push(scheduler_channel_pair);

        let initialisation = GroundAllocatorInitialisation {
            id,
            channel_pair: ground_allocator_channel_pair,
            egress_sender: egress_sender.clone(),
        };

        ground_allocator_join_handles.push(std::thread::spawn(move || {
            ground_allocator_loop(initialisation)
        }));
        info!("Setup spawned ground allocator {id}.")
    }

    let (_control_sender, control_receiver) = crossbeam_channel::unbounded::<ControlEvent>();

    // start scheduler thread
    let _scheduler_handle = std::thread::spawn(move || {
        scheduler_loop(SchedulerInitialisation {
            ground_allocator_thread_count,
            ground_allocation_channel_pairs: scheduler_channel_pairs.into_boxed_slice(),
            matchmaking_queue_parameters,
            control_receiver,
            ingress_receiver,
        })
    });

    // Controller thread — blocks until process receives SIGINT/SIGTERM.
    // The OS terminates the process; all threads and sockets are cleaned up.
    // Graceful shutdown via ControlEvent::Shutdown is available for a future
    // orchestrator binary with proper signal handling.
    std::thread::park();
}

pub fn scheduler_loop(initialisation: SchedulerInitialisation) {
    let SchedulerInitialisation {
        ground_allocator_thread_count,
        ground_allocation_channel_pairs,
        matchmaking_queue_parameters,
        control_receiver,
        ingress_receiver,
    }: SchedulerInitialisation = initialisation;

    let mut scheduling_table = SchedulingTable::new(matchmaking_queue_parameters);

    // Biased channel selector.
    // In order of priority:
    //    - 0: Controller thread
    //    - 1: Ingress thread
    //    - 2+: Ground allocator threads
    let mut prioritised_rxs = Select::new_biased();

    prioritised_rxs.recv(&control_receiver);
    prioritised_rxs.recv(&ingress_receiver);

    // External contract: the order in which operations are inserted into `Select`
    // determines the index returned when that operation is ready on call to
    // `.select()`.
    // This index is matched against in `ground_allocation_channel_pairs` and is
    // stable (by the external contract), and therefore internally the indices must
    // also be used stably.
    for ChannelPair { rx, .. } in &ground_allocation_channel_pairs {
        prioritised_rxs.recv(rx);
    }

    // main event loop
    let mut waiting_allocators = VecDeque::with_capacity(ground_allocator_thread_count);
    loop {
        let SchedulerDiagnostics {
            total_request_count,
            total_filled_request_count,
            per_bucket_request_count,
            ready_count,
            assigned_count,
            waiting_count,
            contended_count,
            empty_count,
            contention_lane_view,
        } = scheduling_table.diagnostics();
        debug!("Scheduler requests filled: {total_filled_request_count}/{total_request_count}");
        debug!("Bucket active counts: {:?}", per_bucket_request_count);
        debug!(
            "Tickets: {ready_count} ready | {assigned_count} assigned | {waiting_count} waiting | {contended_count} contended | {empty_count} empty"
        );
        debug!("Contention lane: {:?}", contention_lane_view);
        debug!("Waiting allocators: {:?}", waiting_allocators);

        let selected_rx = prioritised_rxs.select();

        let index = selected_rx.index();
        match index {
            0 => {
                match selected_rx.recv(&control_receiver) {
                    Ok(message) => match message {
                        ControlEvent::Shutdown => {
                            info!("Scheduler shutting down (controller signal: Shutdown).");
                            break;
                        }
                    },
                    Err(_) => {
                        scheduler::controller_channel_error();
                        warn!("Scheduler shutting down (lost connection to controller).");
                        prioritised_rxs.remove(index);
                        break; // terminate scheduler
                    }
                }
            }
            1 => match selected_rx.recv(&ingress_receiver) {
                Ok(requests) => {
                    info!("Request batch received.");
                    if let Err(errs) = scheduling_table.post_request_batch(requests) {
                        let failure_count = errs.len();
                        error!("{failure_count} request(s) failed to post.");
                        for err in errs {
                            error!("Failed to post request: {err}.");
                        }
                    }

                    while !waiting_allocators.is_empty() {
                        if let Some(assigned) = scheduling_table.next_assignment() {
                            let index = waiting_allocators.pop_front().unwrap();
                            info!(
                                "Waiting ground allocator {} released from waiting.",
                                index - 2
                            );
                            let channel_pair: &ChannelPair<SchedulerEvent, GroundAllocationEvent> =
                                &ground_allocation_channel_pairs[index - 2];
                            if channel_pair
                                .tx
                                .send(SchedulerEvent::Assignment(assigned))
                                .is_err()
                            {
                                scheduler::allocator_channel_error(index);
                                prioritised_rxs.remove(index);
                            }
                        } else {
                            break;
                        }
                    }
                }
                Err(_) => {
                    scheduler::ingress_channel_error();
                    prioritised_rxs.remove(index);
                }
            },
            2.. => match selected_rx.recv(&ground_allocation_channel_pairs[index - 2].rx) {
                Ok(message) => match message {
                    GroundAllocationEvent::Ready => {
                        info!(
                            "Scheduler received ready signal from ground allocator {}.",
                            index - 2
                        );
                        match scheduling_table.next_assignment() {
                            Some(assigned) => {
                                let channel_pair = &ground_allocation_channel_pairs[index - 2];
                                if channel_pair
                                    .tx
                                    .send(SchedulerEvent::Assignment(assigned))
                                    .is_err()
                                {
                                    scheduler::allocator_channel_error(index);
                                    prioritised_rxs.remove(index);
                                }
                            }
                            None => {
                                info!(
                                    "No job for ground allocator {}, pushed to waiting.",
                                    index - 2
                                );
                                waiting_allocators.push_back(index);
                            }
                        }
                    }
                    GroundAllocationEvent::Complete(completed) => {
                        info!(
                            "Scheduler received completion signal from ground allocator {}.",
                            index - 2
                        );

                        scheduling_table.readmit_completed(completed);

                        while !waiting_allocators.is_empty() {
                            if let Some(assigned) = scheduling_table.next_assignment() {
                                let index = waiting_allocators.pop_front().unwrap();
                                info!(
                                    "Waiting ground allocator {} released from waiting.",
                                    index - 2
                                );
                                let channel_pair: &ChannelPair<
                                    SchedulerEvent,
                                    GroundAllocationEvent,
                                > = &ground_allocation_channel_pairs[index - 2];
                                if channel_pair
                                    .tx
                                    .send(SchedulerEvent::Assignment(assigned))
                                    .is_err()
                                {
                                    scheduler::allocator_channel_error(index);
                                    prioritised_rxs.remove(index);
                                }
                            } else {
                                break;
                            }
                        }
                    }
                },
                Err(_) => {
                    scheduler::allocator_channel_error(index);
                    prioritised_rxs.remove(index);
                }
            },
        }
    }
}

pub fn ground_allocator_loop(initialisation: GroundAllocatorInitialisation) {
    let GroundAllocatorInitialisation {
        id,
        channel_pair,
        egress_sender,
    } = initialisation;

    info!("Ground allocator {id} loop started.");

    // main event loop
    loop {
        if channel_pair.tx.send(GroundAllocationEvent::Ready).is_err() {
            ground_allocation::scheduler_rx_channel_error(id);
            break;
        };

        match channel_pair.rx.recv() {
            Ok(msg) => match msg {
                SchedulerEvent::Assignment(assigned) => {
                    info!(
                        "Ground allocator {id} received assigned ticket {}.",
                        assigned.anchor_bucket_index()
                    );

                    let (assignments, completed) = GroundAllocator::new(assigned).run_allocation();

                    info!(
                        "Ground allocator {id} completed assigned ticket {}.",
                        completed.anchor_bucket_index()
                    );

                    if channel_pair
                        .tx
                        .send(GroundAllocationEvent::Complete(completed))
                        .is_err()
                    {
                        ground_allocation::scheduler_tx_channel_error(id);
                        break;
                    };

                    if egress_sender.send(assignments).is_err() {
                        ground_allocation::egress_channel_error(id);
                        break;
                    }

                    info!("Ground allocator {id} sent completed assignment.")
                }
            },
            Err(_) => {
                ground_allocation::scheduler_rx_channel_error(id);
                break;
            }
        }
    }

    error!("Ground allocator {id} loop dropped.")
}

mod ground_allocation {
    use log::error;

    #[inline]
    pub fn egress_channel_error(id: usize) {
        error!("Ground allocator {id} failed to reach egress handler: egress-side rx closed.")
    }

    #[inline]
    pub fn scheduler_rx_channel_error(id: usize) {
        error!("Ground allocator {id} failed to reach scheduler: scheduler-side rx closed.")
    }

    #[inline]
    pub fn scheduler_tx_channel_error(id: usize) {
        error!("Ground allocator {id} failed to reach scheduler: scheduler-side rx closed.")
    }
}

mod scheduler {
    use log::error;

    #[inline]
    pub fn ingress_channel_error() {
        error!("Scheduler failed to reach ingress handler: ingress-side tx closed.")
    }

    #[inline]
    pub fn allocator_channel_error(id: usize) {
        error!("Scheduler failed to reach ground allocator {id}: allocator-side rx closed.")
    }

    #[inline]
    pub fn controller_channel_error() {
        error!("Scheduler failed to reach controller: controller-side tx closed.")
    }
}
