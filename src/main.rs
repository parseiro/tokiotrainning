use std::collections::HashMap;

use mini_redis::{Command, Connection, Frame};
use tokio::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use bytes::Bytes;

type Db = Arc<Mutex<HashMap<String, Bytes>>>;



#[tokio::main]
async fn main() {


    //bind the listener
    let mut listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Couldn't listen");

    println!("Listening...");

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        // the second item contains the IP and port of the new connection
        /*        let _a = listener.accept()
                    .await
                    .(|(socket, )| process(socket).await);
        */
        let db = db.clone();


        if let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                process(socket, db).await;
            });
        }
    }
}

async fn process(socket: TcpStream, db: Db) {
    // the Connection lets us read/write redis **frames** instead of
    // byte streams. The Connection type is defined by mini-redis.


    // println!("Socket: {:?}", socket);

    let mut connection = Connection::new(socket);

    // println!("Connection: {:?}", connection);


    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("Frame: {:?}", frame);

        let response = match Command::from_frame(frame).unwrap() {
            Command::Set(cmd) => {
                println!("I've received SET {}", cmd.key().to_string());

                let _ = db.lock()
                    .unwrap()
                    .insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Command::Get(cmd) => {
                println!("I've received GET {}", cmd.key().to_string());

                if let Some(value) = db.lock().unwrap().get(cmd.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        println!("Enviando \"{}\"", &response);
        connection.write_frame(&response).await.unwrap();
    }
}