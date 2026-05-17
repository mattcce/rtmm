use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use log::{error, info, warn};
use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn read_frame(stream: &mut UnixStream, buf: &mut Vec<u8>) -> io::Result<()> {
    // batch framing: first 4 bytes for length
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    buf.resize(len, 0);
    stream.read_exact(buf)?;
    Ok(())
}

pub fn write_frame(stream: &mut UnixStream, payload: &[u8]) -> io::Result<()> {
    // batch framing
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes())?;

    stream.write_all(payload)?;
    Ok(())
}

pub fn spawn_socket_listener<T: DeserializeOwned + Send + 'static>(
    path: &str,
) -> (std::thread::JoinHandle<()>, Receiver<T>) {
    if std::fs::metadata(path).is_ok() {
        std::fs::remove_file(path).unwrap();
    }
    let listener = UnixListener::bind(path).unwrap();

    let (tx, rx) = crossbeam_channel::unbounded();

    let path = path.to_owned();
    (
        std::thread::spawn(move || {
            info!("Listener thread spawned.");

            let _cleanup = scopeguard::guard(path, |path| {
                let _ = std::fs::remove_file(path);
            });

            'outer: loop {
                match listener.accept() {
                    Ok((mut stream, _addr)) => {
                        info!("Listener thread connected.");

                        let mut buf = Vec::new();

                        loop {
                            buf.clear();

                            if let Err(e) = read_frame(&mut stream, &mut buf) {
                                error!("Bad frame read: {e}.");
                                break; // reconnect on connection drop
                            }

                            let message = match postcard::from_bytes(&buf) {
                                Ok(message) => message,
                                Err(e) => {
                                    error!("Bad message: {e}.");
                                    continue; // skip bad message, try next frame
                                }
                            };

                            if let Err(e) = tx.send(message) {
                                error!("Receiver disconnected, failed to send message: {e}.");
                                break 'outer; // receiver dropped; stop all ingress
                            }
                        }
                    }
                    Err(e) => {
                        error!("Bad incoming connection: {e}.");
                        continue; // retry establishing connection
                    }
                }
            }

            info!("Listener thread closing.")
        }),
        rx,
    )
}

pub fn spawn_socket_sender<T: Serialize + Send + 'static>(
    path: &str,
) -> (std::thread::JoinHandle<()>, Sender<T>) {
    const RETRY_TIMEOUT_SECS: u64 = 5;

    let (tx, rx) = crossbeam_channel::unbounded();

    let path = path.to_owned();
    (
        std::thread::spawn(move || {
            info!("Sender thread spawned.");

            'outer: loop {
                let Ok(mut stream) = UnixStream::connect(&path) else {
                    warn!("Failed to connect to socket at {path}: retrying.");
                    std::thread::sleep(Duration::from_secs(RETRY_TIMEOUT_SECS));
                    continue;
                };
                info!("Sender connected to {path}.");

                loop {
                    let message = match rx.recv() {
                        Ok(message) => message,
                        Err(e) => {
                            error!("Sender disconnected, failed to send message: {e}.");
                            break 'outer; // sender dropped, stop all ingress
                        }
                    };

                    let buf = match postcard::to_extend(&message, Vec::new()) {
                        Ok(buf) => buf,
                        Err(e) => {
                            error!("Bad message: {e}.");
                            break 'outer;
                        }
                    };

                    if let Err(e) = write_frame(&mut stream, &buf) {
                        error!("Bad frame failed to send: {e}.");
                        break 'outer;
                    }
                }
            }

            info!("Sender thread closing.");
        }),
        tx,
    )
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixStream;

    use super::*;

    #[test]
    fn frame_roundtrip() {
        let (mut sender, mut receiver) = UnixStream::pair().unwrap();

        let payload = b"hello world";
        write_frame(&mut sender, payload).unwrap();

        let mut buf = Vec::new();
        read_frame(&mut receiver, &mut buf).unwrap();

        assert_eq!(buf, payload);
    }

    #[test]
    fn frame_size_is_four_plus_payload_length() {
        let (mut sender, mut receiver) = UnixStream::pair().unwrap();

        let payload = [0xABu8; 7];
        write_frame(&mut sender, &payload).unwrap();

        // must drop sender so receiver sees EOF; read_to_end blocks otherwise
        drop(sender);

        let mut all_bytes = Vec::new();
        receiver.read_to_end(&mut all_bytes).unwrap();

        assert_eq!(all_bytes.len(), 4 + 7);
        assert_eq!(&all_bytes[0..4], 7u32.to_be_bytes());
        assert_eq!(&all_bytes[4..], &payload[..]);
    }

    #[test]
    fn read_frame_errors_on_truncated_length_prefix() {
        let (mut sender, mut receiver) = UnixStream::pair().unwrap();

        // write only 3 bytes of the 4-byte length prefix
        sender.write_all(&[0x00, 0x00, 0x00]).unwrap();
        drop(sender);

        let mut buf = Vec::new();
        let result = read_frame(&mut receiver, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn read_frame_errors_when_payload_shorter_than_declared_length() {
        let (mut sender, mut receiver) = UnixStream::pair().unwrap();

        // declare length 10, but send only 3 bytes of payload
        sender.write_all(&10u32.to_be_bytes()).unwrap();
        sender.write_all(&[1, 2, 3]).unwrap();
        drop(sender);

        let mut buf = Vec::new();
        let result = read_frame(&mut receiver, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn listener_and_sender_with_real_socket() {
        let path = "/tmp/rtmm_test_listener_sender.sock";
        let _ = std::fs::remove_file(path);

        let (_, rx) = spawn_socket_listener::<Vec<u8>>(path);
        let (_, tx) = spawn_socket_sender::<Vec<u8>>(path);

        let payload = vec![1, 2, 3, 4, 5];
        tx.send(payload.clone()).unwrap();

        let received = rx.recv().unwrap();
        assert_eq!(received, payload);

        drop(tx);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn listener_accepts_second_connection_after_disconnect() {
        let path = "/tmp/rtmm_test_reconnect.sock";
        let _ = std::fs::remove_file(path);

        let (_, rx) = spawn_socket_listener::<Vec<u8>>(path);

        // first connection
        let (_, tx1) = spawn_socket_sender::<Vec<u8>>(path);
        tx1.send(vec![10, 20]).unwrap();
        assert_eq!(rx.recv().unwrap(), vec![10, 20]);
        drop(tx1);

        // second connection
        let (_, tx2) = spawn_socket_sender::<Vec<u8>>(path);
        tx2.send(vec![30, 40]).unwrap();
        assert_eq!(rx.recv().unwrap(), vec![30, 40]);

        drop(tx2);
        let _ = std::fs::remove_file(path);
    }
}
