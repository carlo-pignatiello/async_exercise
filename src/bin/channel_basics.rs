use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(10);

    tokio::spawn(async move {
        tx.send("hello").await.unwrap();
    });

    let msg = rx.recv().await.unwrap();
    println!("got: {msg}");
}
