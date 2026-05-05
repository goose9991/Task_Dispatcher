# A Comparison of Two Task Dispatch Policies
This report attempts to compare two task dispatch policies implemented in Rust over a shared worker pool with a bounded CPU budget. 

The first policy dispatches tasks in a First-In-First-Out (FIFO) manner. The second policy is optimized to be a budget-aware dispatcher with aging-based starvation prevention.

Each policy’s implementations are described, tested, and further stress tested against burst task delivery.

## Build instructions:

### For Windows:
Install Rust executable:
https://rustup.rs/

Choose option 1

Install Git(recommended from here on forward):
https://git-scm.com/

Open Git Bash terminal, then run:

>git clone https://github.com/goose9991/Task_Dispatcher.git

>cd Task_Dispatcher

### For Linux or Github Codespace: open terminal and run: 

>git clone https://github.com/goose9991/Task_Dispatcher.git

>cd Task_Dispatcher

>curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

Choose option 1 when prompted

Close and reopen terminal, cd into folder and run:
>source $HOME/.cargo/env

For each folder, fifo and optimized, cd into each folder and install rand:
>cargo add rand

Add the following to the bottom of fifo/Cargo.toml if not done so already:

```
[[bin]]
name = "fifo"
path = "src/main.rs"

[[bin]]
name = "stressed"
path = "src/stressed.rs"
```

Add a similar block to optimized/Cargo.toml:
```
[[bin]]
name = "optimized"
path = "src/main.rs"

[[bin]]
name = "stressed"
path = "src/stressed.rs"
```

#### To run the initial balanced test:

cd into either fifo or optimized and run:
>cargo run --release --bin fifo

#### or 

>cargo run --release --bin optimized

#### To run stress test for either:
cd into either fifo or optimized and run:
>cargo run --release --bin stressed

### To output txt files to root folder for comparison:

#### For fifo:

cd into fifo

for initial run:
>cargo run --release --bin fifo > ../fifo_balanced.txt

for fifo stress test:
>cargo run --release --bin stressed > ../fifo_stressed.txt

#### For optimized:

cd into optimized

for initial run:
>cargo run --release --bin optimized > ../optimized_balanced.txt

for optimized stress test:
>cargo run --release --bin stressed > ../optimized_stressed.txt

#### Tool Use Disclosure:
AI assistance (Anthropic's Claude) was used during this project for debugging concurrency issues, refining the scheduling policy through iterative testing, and clarifying Rust-specific syntax around Mutex, Condvar, and Arc. All implementation decisions, experimental design, and results analysis were directed by the author. All code was reviewed and tested by the author before inclusion.
