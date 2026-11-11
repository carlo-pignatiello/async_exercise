# Learning async Rust with Tokio
 
This repository is **just for learning**. It contains small exercises I wrote to understand how async programming works in Rust with [Tokio](https://tokio.rs), coming from a background in Go and goroutines.
 
The code is not meant for production: it's simple on purpose, with lots of `println!` to see what happens and when.
 
## Exercises
 
**Coffee bar**: one barista task and several customer tasks. Customers send orders through a channel, the barista prepares them one at a time and sends each coffee back.
 
**Bank**: one bank task owns an account balance, and several customer tasks deposit, withdraw and check the balance concurrently. Withdrawals can fail with "insufficient funds". The balance stays correct no matter the order of operations, because only the bank task touches it.
 
## Concepts practiced
 
- Spawning concurrent tasks with `tokio::spawn` (like `go` in Go)
- `mpsc` channels: many senders, one receiver
- `oneshot` channels: a single reply to a single request
- The "server task" (actor) pattern: one task owns the state, others send it messages, no `Mutex` needed
- Enums as messages, and `Result` for requests that can fail
- Clean shutdown: the channel closes when every `Sender` is dropped
## Run
 
You need [Rust](https://www.rust-lang.org/tools/install) installed. Then:
 
```bash
cargo run
```
 
The output changes on every run, because the tasks run concurrently and use random delays.
