// EXERCISE: the concert box office
//
// Run with:  cargo run --bin box_office
//
// Like bank.rs and cafeteria.rs, one task owns the state and everyone else
// talks to it through messages. New ideas in this exercise:
//   - the actor owns a COLLECTION (a HashMap), not a single number
//   - a "handle" struct hides the mpsc/oneshot plumbing behind async methods
//   - two actors talk to each other (box office -> payment processor)
//   - a typed error enum instead of String
//
// ─────────────────────────────────────────────────────────────────────────────
// Part 1: the box office actor
//
// The box office owns `HashMap<Show, u32>` (show -> seats left). It starts with:
//     Show::Rock => 10, Show::Jazz => 5, Show::Opera => 3
// Nobody else can touch the map.
//
// It handles these messages:
//   - Availability       -> replies with a copy of the whole map
//   - Reserve { show, seats }
//       * seats == 0 or seats > 4  -> Err(BookingError::InvalidQuantity)
//       * not enough seats left    -> Err(BookingError::SoldOut { show, left })
//       * otherwise subtract the seats, create a Ticket with a unique id
//         (a counter starting at 1), remember it, and reply Ok(ticket)
//   - Cancel { ticket_id }
//       * unknown id               -> Err(BookingError::UnknownTicket(id))
//       * otherwise put the seats back, forget the ticket, reply Ok(())
//
// Spawn 8 fan tasks. Each fan:
//   1. sleeps a random 0..300 ms
//   2. picks a random show and a random number of seats (1..=4)
//   3. tries to reserve and prints the result
//   4. if it got a ticket, with 30% probability cancels it and prints that
//
// When every fan is done, main asks for Availability and prints it.
// The program must end by itself (no hangs).
//
// Self-check: seats sold + seats left must equal the starting capacity for
// every show. Print that check at the end. (Hint: a spawned task can return a
// value: `JoinHandle<Option<Ticket>>` lets each fan hand back the ticket it kept.)
//
// ─────────────────────────────────────────────────────────────────────────────
// Part 2: the handle pattern
//
// In bank.rs every request needs: build a oneshot, build the message, send it,
// await the reply. Wrap that in a cloneable handle so fans just call methods:
//
//     #[derive(Clone)]
//     struct BoxOfficeHandle { tx: mpsc::Sender<BoxOfficeMsg> }
//
//     impl BoxOfficeHandle {
//         fn new() -> (Self, JoinHandle<()>)          // spawns the actor
//         async fn availability(&self) -> HashMap<Show, u32>
//         async fn reserve(&self, show: Show, seats: u32) -> Result<Ticket, BookingError>
//         async fn cancel(&self, ticket_id: u64) -> Result<(), BookingError>
//     }
//
// Fans should only see `BoxOfficeHandle`, never `mpsc` or `oneshot`.
// Question to think about: when does the actor loop end now that main
// doesn't hold a raw Sender to `drop`?
//
// ─────────────────────────────────────────────────────────────────────────────
// Part 3: actors talking to actors
//
// Add a second actor: the payment processor. It handles one message:
//     Charge { amount: u32, reply: oneshot::Sender<Result<(), BookingError>> }
// It "talks to the bank" by sleeping 100 ms, then fails with
// BookingError::PaymentDeclined 20% of the time.
//
// Price is 40 per seat. Change Reserve so the box office:
//   1. checks and HOLDS the seats (subtract them)
//   2. asks the payment processor (via a PaymentHandle) to charge
//   3. on success: issue the ticket
//      on failure: give the seats back and reply Err(PaymentDeclined)
//
// Then think about this: while the box office awaits the payment, it can't
// process any other message. Measure it (print timestamps). Bonus: fix it
// without breaking the invariant. Hint: the box office can hand the payment
// work to a spawned task and have that task send a follow-up message
// (e.g. `PaymentResult { .. }`) back to the box office.
//
// ─────────────────────────────────────────────────────────────────────────────
// Part 4 (bonus): timeouts and shutdown
//
//   - Fans give up if a reservation takes longer than 250 ms
//     (tokio::time::timeout). What happens to the seats if a fan gives up
//     after the box office already held them? Make sure they aren't lost.
//   - Add a Close message: after it, every Reserve gets
//     Err(BookingError::Closed), but Availability still works.
// ─────────────────────────────────────────────────────────────────────────────

#![allow(dead_code, unused)] // remove once you've filled everything in

use std::{boxed, collections::HashMap};
use rand::{prelude::IndexedRandom, random_ratio};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Duration, sleep},
};

const NUM_FANS: usize = 8;
const CHANNEL_CAPACITY: usize = 16;
const MAX_SEATS_PER_ORDER: u32 = 4;
const PRICE_PER_SEAT: u32 = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Show {
    Rock,
    Jazz,
    Opera,
}

impl Show {
    const ALL: [Show; 3] = [Show::Rock, Show::Jazz, Show::Opera];

    fn capacity(self) -> u32 {
        match self {
            Show::Rock => 10,
            Show::Jazz => 5,
            Show::Opera => 3,
        }
    }

    fn random() -> Show {
        let mut rng = rand::rng();
        *Self::ALL.choose(&mut rng).unwrap()
    }
}

#[derive(Debug, Clone)]
struct Ticket {
    id: u64,
    show: Show,
    seats: u32,
}

#[derive(Debug)]
enum BookingError {
    InvalidQuantity,
    SoldOut { show: Show, left: u32 },
    UnknownTicket(u64),
    PaymentDeclined, // Part 3
    Closed,          // Part 4
}

enum BoxOfficeMsg {
    Availability {
        reply: oneshot::Sender<HashMap<Show, u32>>,
    },
    Reserve {
        show: Show,
        seats: u32,
        reply: oneshot::Sender<Result<Ticket, BookingError>>,
    },
    Cancel {
        ticket_id: u64,
        reply: oneshot::Sender<Result<(), BookingError>>,
    },
}

#[tokio::main]
async fn main() {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
    let box_office_handle = tokio::spawn(box_office(rx));
    let mut spectators = Vec::with_capacity(NUM_FANS);
    for i in 0..NUM_FANS {
        let tx = tx.clone();
        let (reply_tx, reply_rx) = oneshot::channel();
        let (reply_tx_cn, reply_rx_cn) = oneshot::channel();
        spectators.push(tokio::spawn(async move { 
            sleep(Duration::from_millis(rand::random_range(0..300))).await;
            let show = Show::random();
            let seats = rand::random_range(1..5);
            let box_office_message = BoxOfficeMsg::Reserve { show, seats, reply: reply_tx};
            tx.send(box_office_message).await.unwrap();
            match reply_rx.await {
                Ok(Ok(ticket)) => {
                    println!("Fan {i} got ticket {ticket:?}");
                    let cancel = random_ratio(1, 3);
                    if cancel {
                        let cancel_msg = BoxOfficeMsg::Cancel { ticket_id: ticket.id, reply:  reply_tx_cn};
                        tx.send(cancel_msg).await;
                        match reply_rx_cn.await {
                            Ok(c) => println!("Fan {i} cancel the {ticket:?}"),
                            Err(e) => println!("Fan {i} unknown ticket id")
                        }
                    }
                },
                Ok(Err(BookingError::SoldOut { show, left })) => {
                    println!("Fan {i}: {show:?} sold out, only {left} left")
                },
                Ok(Err(BookingError::InvalidQuantity)) => {
                   println!("Fan {i}: invalid quantity") 
                },
                Ok(Err(e)) => println!("Fan {i}: booking refused: {e:?}"),
                Err(_) => println!("Fan {i}: box office closed without answering"),
            }
        }));
    }
    for handle in spectators {
        handle.await.unwrap();
    }
    let (reply_tx_ava, reply_rx_ava) = oneshot::channel(); 
    let box_office_message = BoxOfficeMsg::Availability { reply: reply_tx_ava};
    tx.send(box_office_message).await.unwrap();
    let avaibility = reply_rx_ava.await.unwrap();
    println!("Avaibility: {:?}", avaibility); 
    drop(tx);
    box_office_handle.await;
    println!("Box office closed");
}

async fn box_office(mut rx: mpsc::Receiver<BoxOfficeMsg>) {
    let mut seats_left: HashMap<Show, u32> =
        Show::ALL.iter().map(|&s| (s, s.capacity())).collect();
    let mut tickets: HashMap<u64, Ticket> = HashMap::new();
    let mut next_ticket_id: u64 = 1;
    while let Some(box_message) =  rx.recv().await { 
        match box_message {
            BoxOfficeMsg::Availability { reply} => {
                let _ = reply.send(seats_left.clone());
                continue;
            }
            BoxOfficeMsg::Reserve { show, seats, reply } => {
                if seats == 0 || seats > MAX_SEATS_PER_ORDER {
                    let _ = reply.send(Err(BookingError::InvalidQuantity));
                    continue;
                }
                let Some(left) = seats_left.get_mut(&show) else {
                    let _ = reply.send(Err(BookingError::SoldOut { show, left: 0 }));
                    continue;
                };
                if *left < seats {
                    let _ = reply.send(Err(BookingError::SoldOut { show, left: *left }));
                    continue;
                }
                *left -= seats;
                let ticket = Ticket { id: next_ticket_id, show, seats };
                tickets.insert(next_ticket_id, ticket.clone());
                next_ticket_id += 1;
                reply.send(Ok(ticket));
            }
            BoxOfficeMsg::Cancel { ticket_id, reply } => {
                let Some(ticket) = tickets.get(&ticket_id) else {
                    let _ = reply.send(Err(BookingError::UnknownTicket(ticket_id)));
                    continue;
                };
                *seats_left.entry(ticket.show).or_insert(0) += ticket.seats;
                reply.send(Ok(()));
            }
        } 
    }

}
