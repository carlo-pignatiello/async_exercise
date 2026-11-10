use rand::seq::IndexedRandom;
// Create an mpsc channel that carries String messages.
// Spawn 5 customer tasks. Each customer sends one order,
// like "customer 2 wants an espresso". Pick the drink however you like (a fixed list indexed by the customer’s id is fine).
// Spawn 1 barista task that receives orders in a loop and prints Preparing: customer 2 wants an espresso.
// When all customers have ordered, the barista prints Bar closed,
// made N coffees (with the real count) and the program ends by itself. It must not hang.
//
// Part 2: getting the coffee back (oneshot)
//
// Now each customer waits for their coffee.
//
// Replace the String message with a struct:
// rust
//    struct Order {
//        customer: usize,
//        drink: String,
//        reply: oneshot::Sender<String>,
//    }
// The barista “prepares” the drink by sleeping 200 ms (tokio::time::sleep),
// then replies with something like "espresso for customer 2".
// The customer awaits the reply and prints Customer 2 received: espresso for customer 2.
use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, sleep},
};

const NUM_CUSTOMERS: usize = 10;
const CHANNEL_CAPACITY: usize = 10;

#[derive(Debug, Clone, Copy)]
enum Beverage {
    Coffee,
    Cappuccino,
    Tea,
    Matcha,
    GreenTea,
}

impl Beverage {
    const ALL: [Beverage; 5] = [
        Beverage::Coffee,
        Beverage::Cappuccino,
        Beverage::Tea,
        Beverage::Matcha,
        Beverage::GreenTea,
    ];

    fn random() -> Beverage {
        let mut rng = rand::rng();
        *Self::ALL.choose(&mut rng).unwrap()
    }
}

struct Order {
    customer: usize,
    beverage: Beverage,
    reply: oneshot::Sender<String>,
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);

    let barista_handle = tokio::spawn(barista(rx));

    let mut customers = Vec::new();
    for id in 0..NUM_CUSTOMERS {
        let tx = tx.clone();
        let (reply_tx, reply_rx) = oneshot::channel();
        customers.push(tokio::spawn(async move {
            let order = Order {
                customer: id,
                beverage: Beverage::random(),
                reply: reply_tx,
            };
            tx.send(order).await.unwrap();
            let b = reply_rx.await.unwrap();
            println!("Customer {id} received: {b}");
        }));
    }
    drop(tx);

    for handle in customers {
        handle.await.unwrap();
    }
    barista_handle.await.unwrap();
    println!("Cafeteria is now closed");
}

async fn barista(mut rx: mpsc::Receiver<Order>) {
    while let Some(order) = rx.recv().await {
        println!(
            "Preparing {:?} for customer: {:?}",
            order.beverage, order.customer
        );
        sleep(Duration::from_millis(100)).await;
        let beverage = format!("{:?} for customer {}", order.beverage, order.customer);
        order.reply.send(beverage).unwrap();
    }
}
