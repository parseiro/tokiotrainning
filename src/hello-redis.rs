use mini_redis::{client, Result};
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;

#[derive(Debug)]
enum Comando {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    }
}

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[tokio::main]
pub async fn main() -> Result<()> {
    let (mut tx1, mut rx) = mpsc::channel(32);



    let manager = tokio::spawn(async move {
        // Open a connection to the mini-redis address.
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Comando::Get { key, resp } => {
                    println!("Recebi comando GET");
                    let res = client.get(&key).await;
                    let _ = resp.send(res);
                },
                Comando::Set { key, val, resp }=> {
                    println!("Recebi comando SET");
                    let res = client.set(&key, val).await;
                    let _ = resp.send(res);
                },
            }
        }
    });

    let mut tx2 = tx1.clone();

    let t1 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();

        let cmd = Comando::Get {
            key: "foo".to_string(),
            resp: resp_tx,
        };

        println!("I'm going to send message 1");
        tx1.send(cmd).await.unwrap();
        println!("I've just sent message 1");

        let res = resp_rx.await
            .map(|res| {
                res.map(|res2| {
                    res2.map(|_res3| {
                        // println!("Task 1 success: {}", _res3);
                    })
                })
            })
            .map_err(|err| println!("Task 1 error: {}", err))
            ;
        println!("Task 1 received \"{:?}\"", res);
    });
    
    let t2 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();

        let cmd = Comando::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            resp: resp_tx,
        };

        println!("I'm going to send message 2");
        tx2.send(cmd).await.unwrap();
        println!("I've just sent message 2");

        // Await the response
        let res = resp_rx.await
            .map_err(|err| println!("Task 2 error: {}", err))
            ;
        println!("Task 2 received \"{:?}\"", res);
    });

    // Set the key "hello" with value "world"
    // client.set("hello", "world".into()).await?;

    // println!("Task 1 will be awaited");
    t1.await.unwrap();
    // println!("Task 1 awaited");

    // println!("Task 2 will be awaited");
    t2.await.unwrap();
    // println!("Task 2 awaited");

    manager.await.unwrap();

    println!("Manager awaited");

    // Get key "hello"
    // let result = client.get("hello").await?;

    // println!("got value from the server; result={:?}", result);

    Ok(())
}