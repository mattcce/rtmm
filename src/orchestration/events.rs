//! Events for message passing across threads.
//!
//! Events are bucketed by the expected kinds of messages that need to be passed
//! from one specific entity type to the next. This bakes in messaging
//! invariants into the type system.

use crate::matchmaking::scheduling::{Assigned, Completed};

pub enum GroundAllocationEvent {
    Ready,
    Complete(Completed),
}

pub enum SchedulerEvent {
    Assignment(Assigned),
}

pub enum ControlEvent {
    Shutdown,
}
