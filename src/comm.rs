use crate::runtime::message;
use inline_colorization::*;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use std::{collections::HashMap, error::Error};
use tokio::{
    self,
    io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
};

#[derive(Serialize, Deserialize, Debug)]
struct ClientInitMsg {
    user_id: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct Server2ClientMsg {
    env: String,
    err: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Client2ServerMsg {
    input: String,
    user_id: i32,
    timestamp: u128,
}

#[derive(Debug)]
struct ListenerCommMsg {
    stream: TcpStream,
    user_id: i32,
}

pub struct Communication {
    pub client_stream_map: HashMap<i32, TcpStream>,
}

impl Communication {
    pub async fn process_remote(&mut self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:1145").await?;
        let (sndr, mut rcvr): (Sender<ListenerCommMsg>, Receiver<ListenerCommMsg>) =
            mpsc::channel(message::BUFFER_SIZE);
        tokio::spawn(async move {
            while let Ok((mut stream, addr)) = listener.accept().await {
                println!(
                    "{color_blue}Connection with client {:?} established{color_reset}",
                    addr
                );
                let mut buffer = vec![0; 1024];
                let n = stream.read(&mut buffer).await.unwrap();
                let received_raw = String::from_utf8_lossy(&buffer[..n]);
                let received_json = serde_json::to_string(&received_raw).unwrap();
                let init_msg: ClientInitMsg = serde_json::from_str(&received_json).unwrap();
                let conn_msg = ListenerCommMsg {
                    stream: stream,
                    user_id: init_msg.user_id,
                };
                sndr.send(conn_msg).await.unwrap();
            }
        });
        loop {
            tokio::select! {
                Some(listener_msg) = rcvr.recv() => {
                    self.client_stream_map.insert(listener_msg.user_id, listener_msg.stream);
                }
                else => {
                    self.handle_existing_connections().await;
                }
            }
        }
    }

    async fn handle_existing_connections(&mut self) {
        let mut buffer = vec![0; 1024];
        let mut to_be_removed: Vec<i32> = vec![];
        for (id, stream) in self.client_stream_map.iter_mut() {
            match stream.try_read(&mut buffer) {
                Ok(0) => {
                    println!(
                        "{color_yellow}Connection with client {} break, OK(0){color_reset}",
                        id,
                    );
                    to_be_removed.push(id.clone());
                }
                Ok(n) => {
                    todo!("Requires: code update in manager finish, OK({n})");
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    println!(
                        "{color_yellow}Connection with client {} break, error {:?}{color_reset}",
                        id, e,
                    );
                    to_be_removed.push(id.clone());
                }
            }
        }
        for i in to_be_removed.into_iter() {
            self.client_stream_map.remove(&i);
        }
    }
}
