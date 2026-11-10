// One bank task owns a single account balance, starting at 100.
// Nobody else can touch the balance directly.
// And 5 client tasks send requests to the bank.
// Deposit adds the amount and replies with the new balance.
// Withdraw subtracts the amount and replies Ok(new_balance),
// but if there isn’t enough money,
// it replies Err("insufficient funds".to_string()) and
// leaves the balance unchanged.
// Balance just replies with the current balance.
//
//
// Each client does, in order: deposit a random amount (1 to 50),
// withdraw a random amount (1 to 150), and ask for the balance.
// They print every result, for example:

use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, sleep},
};

const NUM_CUSTOMERS: usize = 5;
const CHANNEL_CAPACITY: usize = 10;
const STARTING_BALANCE: u32 = 100;

enum BankMsg {
    Deposit {
        amount: u32,
        reply: oneshot::Sender<u32>,
    },
    Withdraw {
        amount: u32,
        reply: oneshot::Sender<Result<u32, String>>,
    },
    Balance {
        reply: oneshot::Sender<u32>,
    },
}

struct Order {
    customer: usize,
    msg: BankMsg,
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel::<Order>(CHANNEL_CAPACITY);
    let bank_handle = tokio::spawn(bank(rx));

    let mut customers = Vec::new();
    for id in 0..NUM_CUSTOMERS {
        let tx = tx.clone();
        let (reply_deposit_tx, reply_deposit_rx) = oneshot::channel();
        let (reply_wd_tx, reply_wd_rx) = oneshot::channel();
        let (reply_balance_tx, reply_balance_rx) = oneshot::channel();
        customers.push(tokio::spawn(async move {
            sleep(Duration::from_millis(rand::random_range(0..=1000))).await;
            // DEPOSIT
            let deposit = BankMsg::Deposit {
                amount: rand::random_range(1..=50),
                reply: reply_deposit_tx,
            };
            let order = Order {
                customer: id,
                msg: deposit,
            };
            tx.send(order).await.unwrap();
            let res = reply_deposit_rx.await.unwrap();
            println!("Customer {:?} has deposited, now balance is {:?}", id, res);

            sleep(Duration::from_millis(rand::random_range(0..=500))).await;

            // WD
            let wd = BankMsg::Withdraw {
                amount: rand::random_range(1..=150),
                reply: reply_wd_tx,
            };
            let order = Order {
                customer: id,
                msg: wd,
            };
            tx.send(order).await.unwrap();
            let res = reply_wd_rx.await.unwrap();
            match res {
                Ok(b) => println!("Customer {:?} has withdrew, now balance is {:?}", id, b),
                Err(e) => println!("Customer {id} withdrew -> FAILED: {e}"),
            }

            sleep(Duration::from_millis(rand::random_range(0..=500))).await;

            // BALANCE
            let balance = BankMsg::Balance {
                reply: reply_balance_tx,
            };
            let order = Order {
                customer: id,
                msg: balance,
            };
            tx.send(order).await.unwrap();
            let res = reply_balance_rx.await.unwrap();
            println!("Customer {:?} has see the balance {:?}", id, res);
        }));
    }
    drop(tx);
    for handle in customers {
        handle.await.unwrap();
    }
    bank_handle.await.unwrap();
}

async fn bank(mut rx: mpsc::Receiver<Order>) {
    let mut balance = STARTING_BALANCE;
    while let Some(order) = rx.recv().await {
        let Order { customer, msg } = order;
        match msg {
            BankMsg::Deposit { amount, reply } => {
                println!("Customer {:?} wants to deposit {:?}", customer, amount);
                balance += amount;
                sleep(Duration::from_millis(200)).await;
                reply.send(balance).unwrap();
            }
            BankMsg::Withdraw { amount, reply } => {
                println!("Customer {:?} wants to withdraw {:?}", customer, amount);
                if amount > balance {
                    sleep(Duration::from_millis(200)).await;
                    reply.send(Err("insufficient funds".to_string())).unwrap();
                } else {
                    balance -= amount;
                    sleep(Duration::from_millis(300)).await;
                    reply.send(Ok(balance)).unwrap();
                }
            }
            BankMsg::Balance { reply } => {
                println!("Customer {:?} wants to see the balance", customer);
                sleep(Duration::from_millis(100)).await;
                reply.send(balance).unwrap();
            }
        };
    }
}
