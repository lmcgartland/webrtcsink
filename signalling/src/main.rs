use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Error};

use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use async_tungstenite::tungstenite::Message as WsMessage;
use clap::Parser;
use futures::channel::mpsc;
use futures::prelude::*;
use serde_derive::{Deserialize, Serialize};
use tracing::{debug, info, instrument, trace, warn};
use tracing_subscriber::prelude::*;

#[derive(Parser, Debug)]
#[clap(about, version, author)]
/// Program arguments
struct Args {}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "peer-type")]
#[serde(rename_all = "lowercase")]
enum RegisteredMessage {
    Producer,
    Consumer,
    Listener,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
enum OutgoingMessage {
    Registered(RegisteredMessage),
    ProducerAdded { peer_id: String },
    ProducerRemoved { peer_id: String },
    StartSession { peer_id: String },
    EndSession { peer_id: String },
    Peer(PeerMessage),
    List { producers: Vec<String> },
    Error { details: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "peer-type")]
#[serde(rename_all = "lowercase")]
enum RegisterMessage {
    Producer,
    Consumer,
    Listener,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct StartSessionMessage {
    peer_id: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
enum SdpMessage {
    Offer { sdp: String },
    Answer { sdp: String },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
enum PeerMessageInner {
    Ice {
        candidate: String,
        #[serde(rename = "sdpMLineIndex")]
        sdp_mline_index: u32,
    },
    Sdp(SdpMessage),
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
struct PeerMessage {
    peer_id: String,
    peer_message: PeerMessageInner,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct EndSessionMessage {
    peer_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
enum IncomingMessage {
    Register(RegisterMessage),
    StartSession(StartSessionMessage),
    EndSession(EndSessionMessage),
    Peer(PeerMessage),
    List,
}

struct State {
    message_handler: Box<dyn MessageHandler>,
    message_sender: Option<DefaultMessageSender>,
}

struct Peer {
    receive_task_handle: task::JoinHandle<()>,
    send_task_handle: task::JoinHandle<Result<(), Error>>,
    sender: mpsc::Sender<String>,
}

#[derive(Default)]
struct DefaultMessageSender {
    peers: HashMap<String, Peer>,
}

impl MessageSender for DefaultMessageSender {
    fn send_message(&mut self, message: String, peer_id: &str) {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            let mut sender = peer.sender.clone();
            let peer_id = peer_id.to_string();
            task::spawn(async move {
                if let Err(err) = sender.send(message).await {
                    warn!(peer_id = %peer_id, "Failed to send message: {}", err);
                }
            });
        } else {
            warn!(peer_id = %peer_id, "unknown peer");
        }
    }
}

fn remove_peer(state: Arc<Mutex<State>>, peer_id: &str) {
    {
        let mut state = state.lock().unwrap();
        let mut message_sender = state.message_sender.take().unwrap();

        state
            .message_handler
            .remove_peer(&mut message_sender, peer_id);

        state.message_sender = Some(message_sender);
    }

    if let Some(mut peer) = state
        .lock()
        .unwrap()
        .message_sender
        .as_mut()
        .unwrap()
        .peers
        .remove(peer_id)
    {
        let peer_id = peer_id.to_string();
        task::spawn(async move {
            peer.sender.close_channel();
            if let Err(err) = peer.send_task_handle.await {
                trace!(peer_id = %peer_id, "Error while joining send task: {}", err);
            }
            peer.receive_task_handle.await;
        });
    }
}

async fn accept_connection(state: Arc<Mutex<State>>, stream: TcpStream) {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    info!("Peer address: {}", addr);

    let ws = async_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");

    let this_id = uuid::Uuid::new_v4().to_string();
    info!(this_id = %this_id, "New WebSocket connection: {}", addr);

    // 1000 is completely arbitrary, we simply don't want infinite piling
    // up of messages as with unbounded
    let (websocket_sender, mut websocket_receiver) = mpsc::channel::<String>(1000);

    let this_id_clone = this_id.clone();
    let (mut ws_sink, mut ws_stream) = ws.split();
    let send_task_handle = task::spawn(async move {
        while let Some(msg) = websocket_receiver.next().await {
            trace!(this_id = %this_id_clone, "sending {}", msg);
            ws_sink.send(WsMessage::Text(msg)).await?;
        }

        ws_sink.send(WsMessage::Close(None)).await?;
        ws_sink.close().await?;

        Ok::<(), Error>(())
    });

    {}

    let state_clone = state.clone();
    let this_id_clone = this_id.clone();
    let receive_task_handle = task::spawn(async move {
        while let Some(msg) = async_std::stream::StreamExt::next(&mut ws_stream).await {
            let mut state = state_clone.lock().unwrap();

            match msg {
                Ok(WsMessage::Text(msg)) => {
                    let mut message_sender = state.message_sender.take().unwrap();

                    if let Err(err) = state.message_handler.handle_message(
                        &mut message_sender,
                        msg,
                        &this_id_clone,
                    ) {
                        warn!(this = %this_id_clone, "Error handling message: {:?}", err);
                        message_sender.send_message(
                            serde_json::to_string(&OutgoingMessage::Error {
                                details: err.to_string(),
                            })
                            .unwrap(),
                            &this_id_clone,
                        );
                    }

                    state.message_sender = Some(message_sender);
                }
                Ok(WsMessage::Close(reason)) => {
                    info!(this_id = %this_id_clone, "connection closed: {:?}", reason);
                    break;
                }
                Ok(_) => warn!(this_id = %this_id_clone, "Unsupported message type"),
                Err(err) => {
                    warn!(this_id = %this_id_clone, "recv error: {}", err);
                    break;
                }
            }
        }

        remove_peer(state_clone, &this_id_clone);
    });

    let mut state = state.lock().unwrap();

    state.message_sender.as_mut().unwrap().peers.insert(
        this_id,
        Peer {
            receive_task_handle,
            send_task_handle,
            sender: websocket_sender,
        },
    );
}

async fn run(args: Args) -> Result<(), Error> {
    tracing_log::LogTracer::init().expect("Failed to set logger");
    let env_filter = tracing_subscriber::EnvFilter::try_from_env("WEBRTCSINK_SIGNALLING_SERVER")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_target(true)
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        );
    let subscriber = tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(fmt_layer);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    let state = Arc::new(Mutex::new(State {
        message_handler: Box::new(DefaultMessageHandler::default()),
        message_sender: Some(DefaultMessageSender::default()),
    }));

    let addr = "127.0.0.1:8443".to_string();

    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    info!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        task::spawn(accept_connection(state.clone(), stream));
    }

    Ok(())
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    task::block_on(run(args))
}

trait MessageHandler: Sync + Send {
    fn handle_message(
        &mut self,
        sender: &mut dyn MessageSender,
        message: String,
        peer_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn remove_peer(&mut self, sender: &mut dyn MessageSender, peer_id: &str);
}

trait MessageSender {
    fn send_message(&mut self, message: String, peer_id: &str);
}

#[derive(Default)]
struct DefaultMessageHandler {
    producers: HashMap<String, HashSet<String>>,
    consumers: HashMap<String, Option<String>>,
    listeners: HashSet<String>,
}

impl MessageHandler for DefaultMessageHandler {
    #[instrument(level = "debug", skip(self, sender))]
    fn remove_peer(&mut self, sender: &mut dyn MessageSender, peer_id: &str) {
        info!(peer_id = %peer_id, "removing peer");

        self.listeners.remove(peer_id);

        if let Some(consumers) = self.producers.remove(peer_id) {
            for consumer_id in &consumers {
                info!(producer_id=%peer_id, consumer_id=%consumer_id, "ended session");
                self.consumers.insert(consumer_id.clone(), None);
                sender.send_message(
                    serde_json::to_string(&OutgoingMessage::EndSession {
                        peer_id: peer_id.to_string(),
                    })
                    .unwrap(),
                    &consumer_id,
                );
            }

            for listener in &self.listeners {
                sender.send_message(
                    serde_json::to_string(&OutgoingMessage::ProducerRemoved {
                        peer_id: peer_id.to_string(),
                    })
                    .unwrap(),
                    &listener,
                );
            }
        }

        if let Some(Some(producer_id)) = self.consumers.remove(peer_id) {
            info!(producer_id=%producer_id, consumer_id=%peer_id, "ended session");

            self.producers
                .get_mut(&producer_id)
                .unwrap()
                .remove(peer_id);

            sender.send_message(
                serde_json::to_string(&OutgoingMessage::EndSession {
                    peer_id: peer_id.to_string(),
                })
                .unwrap(),
                &producer_id,
            );
        }
    }

    #[instrument(level = "trace", skip(self, sender))]
    fn handle_message(
        &mut self,
        sender: &mut dyn MessageSender,
        message: String,
        peer_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("Handling {}", message);

        if let Ok(message) = serde_json::from_str::<IncomingMessage>(&message) {
            match message {
                IncomingMessage::Register(message) => match message {
                    RegisterMessage::Producer => self.register_producer(sender, peer_id),
                    RegisterMessage::Consumer => self.register_consumer(sender, peer_id),
                    RegisterMessage::Listener => self.register_listener(sender, peer_id),
                },
                IncomingMessage::StartSession(message) => {
                    self.start_session(sender, &message.peer_id, peer_id)
                }
                IncomingMessage::Peer(PeerMessage {
                    peer_id: other_peer_id,
                    peer_message,
                }) => match peer_message {
                    PeerMessageInner::Ice {
                        candidate,
                        sdp_mline_index,
                    } => self.handle_ice(
                        sender,
                        candidate,
                        sdp_mline_index,
                        &peer_id,
                        &other_peer_id,
                    ),
                    PeerMessageInner::Sdp(sdp_message) => match sdp_message {
                        SdpMessage::Offer { sdp } => {
                            self.handle_sdp_offer(sender, sdp, &peer_id, &other_peer_id)
                        }
                        SdpMessage::Answer { sdp } => {
                            self.handle_sdp_answer(sender, sdp, &peer_id, &other_peer_id)
                        }
                    },
                },
                IncomingMessage::List => self.list_producers(sender, peer_id),
                IncomingMessage::EndSession(EndSessionMessage {
                    peer_id: other_peer_id,
                }) => self.end_session(sender, &peer_id, &other_peer_id),
            }
            .map_err(|err| err.into())
        } else {
            Err(anyhow!("Unsupported message").into())
        }
    }
}

impl DefaultMessageHandler {
    #[instrument(level = "debug", skip(self, sender))]
    fn end_session(
        &mut self,
        sender: &mut dyn MessageSender,
        peer_id: &str,
        other_peer_id: &str,
    ) -> Result<(), Error> {
        info!(peer_id=%peer_id, other_peer_id=%other_peer_id, "endsession request");

        if let Some(ref mut consumers) = self.producers.get_mut(peer_id) {
            if consumers.remove(other_peer_id) {
                info!(producer_id=%peer_id, consumer_id=%other_peer_id, "ended session");

                sender.send_message(
                    serde_json::to_string(&OutgoingMessage::EndSession {
                        peer_id: peer_id.to_string(),
                    })
                    .unwrap(),
                    other_peer_id,
                );

                self.consumers.insert(other_peer_id.to_string(), None);

                Ok(())
            } else {
                Err(anyhow!(
                    "Producer {} has no consumer {}",
                    peer_id,
                    other_peer_id
                ))
            }
        } else if let Some(Some(producer_id)) = self.consumers.get(peer_id) {
            if producer_id == other_peer_id {
                info!(producer_id=%other_peer_id, consumer_id=%peer_id, "ended session");

                self.consumers.insert(peer_id.to_string(), None);
                self.producers
                    .get_mut(other_peer_id)
                    .unwrap()
                    .remove(peer_id);

                sender.send_message(
                    serde_json::to_string(&OutgoingMessage::EndSession {
                        peer_id: peer_id.to_string(),
                    })
                    .unwrap(),
                    other_peer_id,
                );

                Ok(())
            } else {
                Err(anyhow!(
                    "Consumer {} is not in a session with {}",
                    peer_id,
                    other_peer_id
                ))
            }
        } else {
            Err(anyhow!(
                "No session between {} and {}",
                peer_id,
                other_peer_id
            ))
        }
    }

    #[instrument(level = "debug", skip(self, sender))]
    fn list_producers(
        &mut self,
        sender: &mut dyn MessageSender,
        peer_id: &str,
    ) -> Result<(), Error> {
        sender.send_message(
            serde_json::to_string(&OutgoingMessage::List {
                producers: self.producers.keys().map(|s| s.clone()).collect(),
            })
            .unwrap(),
            peer_id,
        );

        Ok(())
    }

    #[instrument(level = "debug", skip(self, sender))]
    fn handle_ice(
        &mut self,
        sender: &mut dyn MessageSender,
        candidate: String,
        sdp_mline_index: u32,
        peer_id: &str,
        other_peer_id: &str,
    ) -> Result<(), Error> {
        if let Some(consumers) = self.producers.get(peer_id) {
            if consumers.contains(other_peer_id) {
                sender.send_message(
                    serde_json::to_string(&OutgoingMessage::Peer(PeerMessage {
                        peer_id: peer_id.to_string(),
                        peer_message: PeerMessageInner::Ice {
                            candidate,
                            sdp_mline_index,
                        },
                    }))
                    .unwrap(),
                    other_peer_id,
                );
                Ok(())
            } else {
                Err(anyhow!(
                    "cannot forward ICE from {} to {} as they are not in a session",
                    peer_id,
                    other_peer_id
                ))
            }
        } else if let Some(producer) = self.consumers.get(peer_id) {
            if &Some(other_peer_id.to_string()) == producer {
                sender.send_message(
                    serde_json::to_string(&OutgoingMessage::Peer(PeerMessage {
                        peer_id: peer_id.to_string(),
                        peer_message: PeerMessageInner::Ice {
                            candidate,
                            sdp_mline_index,
                        },
                    }))
                    .unwrap(),
                    other_peer_id,
                );

                Ok(())
            } else {
                Err(anyhow!(
                    "cannot forward ICE from {} to {} as they are not in a session",
                    peer_id,
                    other_peer_id
                ))
            }
        } else {
            Err(anyhow!(
                "cannot forward ICE from {} to {} as they are not in a session",
                peer_id,
                other_peer_id,
            ))
        }
    }

    #[instrument(level = "debug", skip(self, sender))]
    fn handle_sdp_offer(
        &mut self,
        sender: &mut dyn MessageSender,
        sdp: String,
        producer_id: &str,
        consumer_id: &str,
    ) -> Result<(), Error> {
        if let Some(consumers) = self.producers.get(producer_id) {
            if consumers.contains(consumer_id) {
                sender.send_message(
                    serde_json::to_string(&OutgoingMessage::Peer(PeerMessage {
                        peer_id: producer_id.to_string(),
                        peer_message: PeerMessageInner::Sdp(SdpMessage::Offer { sdp }),
                    }))
                    .unwrap(),
                    consumer_id,
                );
                Ok(())
            } else {
                Err(anyhow!(
                    "cannot forward offer from {} to {} as they are not in a session",
                    producer_id,
                    consumer_id
                ))
            }
        } else {
            Err(anyhow!(
                "cannot forward offer from {} to {} as they are not in a session or {} is not the producer",
                producer_id,
                consumer_id,
                producer_id,
            ))
        }
    }

    #[instrument(level = "debug", skip(self, sender))]
    fn handle_sdp_answer(
        &mut self,
        sender: &mut dyn MessageSender,
        sdp: String,
        consumer_id: &str,
        producer_id: &str,
    ) -> Result<(), Error> {
        if let Some(producer) = self.consumers.get(consumer_id) {
            if &Some(producer_id.to_string()) == producer {
                sender.send_message(
                    serde_json::to_string(&OutgoingMessage::Peer(PeerMessage {
                        peer_id: consumer_id.to_string(),
                        peer_message: PeerMessageInner::Sdp(SdpMessage::Answer { sdp }),
                    }))
                    .unwrap(),
                    producer_id,
                );
                Ok(())
            } else {
                Err(anyhow!(
                    "cannot forward answer from {} to {} as they are not in a session",
                    consumer_id,
                    producer_id
                ))
            }
        } else {
            Err(anyhow!(
                "cannot forward answer from {} to {} as they are not in a session",
                consumer_id,
                producer_id
            ))
        }
    }

    #[instrument(level = "debug", skip(self, sender))]
    fn register_producer(
        &mut self,
        sender: &mut dyn MessageSender,
        peer_id: &str,
    ) -> Result<(), Error> {
        if self.producers.contains_key(peer_id) {
            Err(anyhow!("{} is already registered as a producer", peer_id))
        } else {
            self.producers.insert(peer_id.to_string(), HashSet::new());

            for listener in &self.listeners {
                sender.send_message(
                    serde_json::to_string(&OutgoingMessage::ProducerAdded {
                        peer_id: peer_id.to_string(),
                    })
                    .unwrap(),
                    &listener,
                );
            }

            sender.send_message(
                serde_json::to_string(&OutgoingMessage::Registered(RegisteredMessage::Producer))
                    .unwrap(),
                peer_id,
            );

            info!(peer_id = %peer_id, "registered as a producer");

            Ok(())
        }
    }

    #[instrument(level = "debug", skip(self, sender))]
    fn register_consumer(
        &mut self,
        sender: &mut dyn MessageSender,
        peer_id: &str,
    ) -> Result<(), Error> {
        if self.consumers.contains_key(peer_id) {
            Err(anyhow!("{} is already registered as a consumer", peer_id))
        } else {
            self.consumers.insert(peer_id.to_string(), None);

            sender.send_message(
                serde_json::to_string(&OutgoingMessage::Registered(RegisteredMessage::Consumer))
                    .unwrap(),
                peer_id,
            );

            info!(peer_id = %peer_id, "registered as a consumer");

            Ok(())
        }
    }

    #[instrument(level = "debug", skip(self, sender))]
    fn register_listener(
        &mut self,
        sender: &mut dyn MessageSender,
        peer_id: &str,
    ) -> Result<(), Error> {
        if !self.listeners.insert(peer_id.to_string()) {
            Err(anyhow!("{} is already registered as a listener", peer_id))
        } else {
            sender.send_message(
                serde_json::to_string(&OutgoingMessage::Registered(RegisteredMessage::Listener))
                    .unwrap(),
                peer_id,
            );

            info!(peer_id = %peer_id, "registered as a listener");

            Ok(())
        }
    }

    #[instrument(level = "debug", skip(self, sender))]
    fn start_session(
        &mut self,
        sender: &mut dyn MessageSender,
        producer_id: &str,
        consumer_id: &str,
    ) -> Result<(), Error> {
        if !self.consumers.contains_key(consumer_id) {
            return Err(anyhow!(
                "Peer with id {} is not registered as a consumer",
                consumer_id
            ));
        }

        if let Some(producer_id) = self.consumers.get(consumer_id).unwrap() {
            return Err(anyhow!(
                "Consumer with id {} is already in a session with producer {}",
                consumer_id,
                producer_id,
            ));
        }

        if !self.producers.contains_key(producer_id) {
            return Err(anyhow!(
                "Peer with id {} is not registered as a producer",
                producer_id
            ));
        }

        self.consumers
            .insert(consumer_id.to_string(), Some(producer_id.to_string()));
        self.producers
            .get_mut(producer_id)
            .unwrap()
            .insert(consumer_id.to_string());

        sender.send_message(
            serde_json::to_string(&OutgoingMessage::StartSession {
                peer_id: consumer_id.to_string(),
            })
            .unwrap(),
            producer_id,
        );

        info!(producer_id = %producer_id, consumer_id = %consumer_id, "started a session");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use std::collections::VecDeque;

    #[derive(Debug)]
    struct MockSentMessage {
        message: String,
        peer_id: String,
    }

    #[derive(Default)]
    struct MockMessageSender {
        messages: VecDeque<MockSentMessage>,
    }

    impl MessageSender for MockMessageSender {
        fn send_message(&mut self, message: String, peer_id: &str) {
            self.messages.push_back(MockSentMessage {
                message,
                peer_id: peer_id.to_string(),
            });
        }
    }

    #[test]
    fn test_register_producer() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "producer");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::Registered(RegisteredMessage::Producer)
        );
    }

    #[test]
    fn test_list_producers() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::List;

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "listener",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "listener");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::List {
                producers: vec!["producer".to_string()]
            }
        );
    }

    #[test]
    fn test_register_consumer() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "consumer");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::Registered(RegisteredMessage::Consumer)
        );
    }

    #[test]
    #[should_panic(expected = "already registered as a producer")]
    fn test_register_producer_twice() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();
    }

    #[test]
    fn test_listener() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Listener);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "listener",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "listener");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::ProducerAdded {
                peer_id: "producer".to_string()
            }
        );
    }

    #[test]
    fn test_start_session() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "producer");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::StartSession {
                peer_id: "consumer".to_string()
            }
        );
    }

    #[test]
    fn test_remove_peer() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Listener);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "listener",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        handler.remove_peer(&mut sender, "producer");

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "consumer");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::EndSession {
                peer_id: "producer".to_string()
            }
        );

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "listener");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::ProducerRemoved {
                peer_id: "producer".to_string()
            }
        );
    }

    #[test]
    fn test_end_session_consumer() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::EndSession(EndSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "producer");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::EndSession {
                peer_id: "consumer".to_string()
            }
        );
    }

    #[test]
    fn test_end_session_producer() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::EndSession(EndSessionMessage {
            peer_id: "consumer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "consumer");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::EndSession {
                peer_id: "producer".to_string()
            }
        );
    }

    #[test]
    #[should_panic(expected = "producer has no consumer")]
    fn test_end_session_twice() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::EndSession(EndSessionMessage {
            peer_id: "consumer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::EndSession(EndSessionMessage {
            peer_id: "consumer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();
    }

    #[test]
    fn test_sdp_exchange() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Peer(PeerMessage {
            peer_id: "consumer".to_string(),
            peer_message: PeerMessageInner::Sdp(SdpMessage::Offer {
                sdp: "offer".to_string(),
            }),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "consumer");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::Peer(PeerMessage {
                peer_id: "producer".to_string(),
                peer_message: PeerMessageInner::Sdp(SdpMessage::Offer {
                    sdp: "offer".to_string()
                })
            })
        );
    }

    #[test]
    fn test_ice_exchange() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Peer(PeerMessage {
            peer_id: "consumer".to_string(),
            peer_message: PeerMessageInner::Ice {
                candidate: "candidate".to_string(),
                sdp_mline_index: 42,
            },
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "consumer");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::Peer(PeerMessage {
                peer_id: "producer".to_string(),
                peer_message: PeerMessageInner::Ice {
                    candidate: "candidate".to_string(),
                    sdp_mline_index: 42
                }
            })
        );

        let message = IncomingMessage::Peer(PeerMessage {
            peer_id: "producer".to_string(),
            peer_message: PeerMessageInner::Ice {
                candidate: "candidate".to_string(),
                sdp_mline_index: 42,
            },
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        let sent_message = sender.messages.pop_front().unwrap();

        assert_eq!(sent_message.peer_id, "producer");
        assert_eq!(
            serde_json::from_str::<OutgoingMessage>(&sent_message.message).unwrap(),
            OutgoingMessage::Peer(PeerMessage {
                peer_id: "consumer".to_string(),
                peer_message: PeerMessageInner::Ice {
                    candidate: "candidate".to_string(),
                    sdp_mline_index: 42
                }
            })
        );
    }

    #[test]
    #[should_panic(expected = "they are not in a session")]
    fn test_sdp_exchange_wrong_direction_offer() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Peer(PeerMessage {
            peer_id: "producer".to_string(),
            peer_message: PeerMessageInner::Sdp(SdpMessage::Offer {
                sdp: "offer".to_string(),
            }),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "not registered as a producer")]
    fn test_start_session_no_producer() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "not registered as a consumer")]
    fn test_start_session_no_consumer() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "already in a session with producer")]
    fn test_start_session_twice() {
        let mut sender = MockMessageSender::default();
        let mut handler = DefaultMessageHandler::default();

        let message = IncomingMessage::Register(RegisterMessage::Producer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "producer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::Register(RegisterMessage::Consumer);

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();

        sender.messages.pop_front().unwrap();

        let message = IncomingMessage::StartSession(StartSessionMessage {
            peer_id: "producer".to_string(),
        });

        handler
            .handle_message(
                &mut sender,
                serde_json::to_string(&message).unwrap(),
                "consumer",
            )
            .unwrap();
    }
}
