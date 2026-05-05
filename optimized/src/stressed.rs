use rand::prelude::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind { Io, Cpu }

#[derive(Clone, Debug)]
struct Task {
    id:           u32,
    kind:         Kind,
    cpu_cost:     f64,
    duration:     Duration,
    arrival_time: Instant,
    enqueue_time: Option<Instant>,
    start_time:   Option<Instant>,
    finish_time:  Option<Instant>,
    skip_count:   u32,
}

impl Task {
    fn new(id: u32, kind: Kind, cpu_cost: f64, duration: Duration) -> Self {
        Task {
            id, kind, cpu_cost, duration,
            arrival_time: Instant::now(),
            enqueue_time: None,
            start_time:   None,
            finish_time:  None,
            skip_count:   0,
        }
    }
    fn wait_ms(&self) -> f64 {
        match (self.enqueue_time, self.start_time) {
            (Some(e), Some(s)) => s.duration_since(e).as_millis() as f64,
            _ => 0.0,
        }
    }
    fn turnaround_ms(&self) -> f64 {
        match (self.enqueue_time, self.finish_time) {
            (Some(e), Some(f)) => f.duration_since(e).as_millis() as f64,
            _ => 0.0,
        }
    }
}

struct Config {
    num_tasks:           usize,
    arrival_interval_ms: u64,
    burst_size:          usize,
    num_workers:         usize,
    monitor_interval_ms: u64,
    random_seed:         u64,
    io_cpu_cost:         f64,
    io_duration_ms:      u64,
    cpu_cpu_cost:        f64,
    cpu_duration_ms:     u64,
    aging_threshold:     u32,
}

impl Config {
    fn new() -> Self {
        // ── Policy: budget-aware dispatch ────────────────────────────
        //
        // Root cause of previous IO-first underperformance:
        //
        //   IO-first filled workers with IO tasks (10% CPU each).
        //   With 8 workers all on IO: 8 × 10% = 80% budget used.
        //   The remaining 20% cannot fit another IO task AND there are
        //   no free workers — so it sits idle every dispatch cycle.
        //   FIFO avoids this by running CPU tasks (35%) which fill that
        //   gap. 6 IO + 1 CPU = 95% budget — far more efficient.
        //
        // New policy — two-pass dispatch:
        //
        //   Pass 1 (AGED): any task skipped >= aging_threshold times
        //     gets absolute priority. Starvation prevention only.
        //
        //   Pass 2 (BUDGET-AWARE):
        //     If remaining budget >= cpu_cost AND a CPU task exists:
        //       → dispatch CPU task.
        //       Rationale: a CPU task fills more of the remaining budget
        //       than an IO task would, increasing total throughput.
        //     Else:
        //       → dispatch IO task (IO still preferred when budget is tight
        //         or no CPU tasks are available, keeping IO wait low).
        //
        //   Pass 3 (FALLBACK): any task that fits the budget.
        //
        // This targets the 6% budget gap between FIFO (89.5%) and
        // IO-first (83.5%), aiming for ~90%+ utilization while keeping
        // IO avg wait well below FIFO by still running IO tasks in
        // parallel with the CPU tasks rather than after them.
        Config {
            num_tasks:           1000,
            // Burst pattern: 10 tasks every 200ms instead of 1 every 20ms.
            // Same average arrival rate (50 tasks/sec) but the queue spikes
            // by 10 tasks at a time, then sits idle 200ms — a stress test
            // for the dispatcher because it must make 10 decisions in a
            // tight window after each burst.
            arrival_interval_ms: 200,
            burst_size:          10,
            num_workers:         8,
            monitor_interval_ms: 10,
            random_seed:         42,
            io_cpu_cost:         0.10,
            io_duration_ms:      200,
            cpu_cpu_cost:        0.35,
            cpu_duration_ms:     200,
            aging_threshold:     20,
        }
    }
    fn task_for_kind(&self, kind: Kind) -> (f64, u64) {
        match kind {
            Kind::Io  => (self.io_cpu_cost,  self.io_duration_ms),
            Kind::Cpu => (self.cpu_cpu_cost, self.cpu_duration_ms),
        }
    }
}

struct Monitor { interval_ms: u64 }

#[derive(Clone, Debug)]
struct Snapshot {
    time_ms:        u64,
    cpu_percent:    f64,
    active_workers: usize,
}

impl Monitor {
    fn new(interval_ms: u64) -> Self { Monitor { interval_ms } }
    fn start(
        &self,
        state:     Arc<(Mutex<QueueState>, Condvar)>,
        snapshots: Arc<Mutex<Vec<Snapshot>>>,
        shutdown:  Arc<AtomicBool>,
        sim_start: Instant,
    ) {
        let interval = self.interval_ms;
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(interval));
                if shutdown.load(Ordering::Relaxed) { break; }
                let st = state.0.lock().unwrap();
                let snap = Snapshot {
                    time_ms:        sim_start.elapsed().as_millis() as u64,
                    cpu_percent:    st.active_cpu_load * 100.0,
                    active_workers: st.active_workers,
                };
                drop(st);
                snapshots.lock().unwrap().push(snap);
            }
        });
    }
}

struct QueueState {
    queue:           VecDeque<Task>,
    active_workers:  usize,
    active_cpu_load: f64,
    generation_done: bool,
}

impl QueueState {
    fn new() -> Self {
        QueueState {
            queue:           VecDeque::new(),
            active_workers:  0,
            active_cpu_load: 0.0,
            generation_done: false,
        }
    }
}

struct Worker { id: usize }

impl Worker {
    fn new(id: usize) -> Self { Worker { id } }

    fn start(
        &self,
        state:     Arc<(Mutex<QueueState>, Condvar)>,
        completed: Arc<Mutex<Vec<Task>>>,
        cfg:       Arc<Config>,
        sim_start: Instant,
    ) -> thread::JoinHandle<()> {
        let id = self.id;
        thread::spawn(move || {
            loop {
                let task = {
                    let (lock, cvar) = &*state;
                    let mut st = lock.lock().unwrap();

                    loop {
                        if st.generation_done
                            && st.queue.is_empty()
                            && st.active_workers == 0
                        {
                            break None;
                        }

                        if st.active_workers < cfg.num_workers && !st.queue.is_empty() {
                            let remaining = 1.0 - st.active_cpu_load;

                            // ── POLICY: budget-aware dispatch ─────────────
                            //
                            // Pass 1 — AGING: starvation guard.
                            //   Any task skipped >= aging_threshold is
                            //   promoted immediately regardless of kind.
                            //
                            // Pass 2 — BUDGET-AWARE selection:
                            //   If remaining budget >= cpu_cost AND a CPU
                            //   task exists in queue → dispatch CPU task.
                            //
                            //   Key insight: 8 IO tasks saturate workers
                            //   at 80% budget (8×10%), leaving 20% idle.
                            //   A CPU task (35%) fills that gap more
                            //   efficiently. 6IO+1CPU = 95% vs 8IO = 80%.
                            //   Running CPU tasks concurrently with IO
                            //   (not instead of IO) boosts throughput
                            //   without sacrificing IO wait time.
                            //
                            // Pass 3 — IO-FIRST fallback:
                            //   When budget is too tight for a CPU task,
                            //   or no CPU tasks exist, prefer IO tasks.
                            //   This keeps IO wait low for the 70% majority.
                            //
                            // Pass 4 — ANY FITTING TASK:
                            //   Last resort — take whatever fits budget.

                            // Pass 1 (HARD BLOCK): is there ANY aged task?
                            // If yes, no other task may dispatch from this worker
                            // until the aged task can run. This stops the
                            // pathological case where a CPU task hits skip_count
                            // 400+ because it kept being aged but never actually
                            // selected (budget too tight, worker fell through to IO).
                            let any_aged = st.queue.iter().any(|t| {
                                t.skip_count >= cfg.aging_threshold
                            });

                            // Aged task that ALSO fits the current budget
                            let aged_idx = st.queue.iter().position(|t| {
                                t.skip_count >= cfg.aging_threshold
                                    && t.cpu_cost <= remaining
                            });

                            // If an aged task exists but doesn't fit budget,
                            // wait — don't grab an IO task and skip it again.
                            // Only the worker holding the largest cpu_load can
                            // free space, so we just block here and let other
                            // running tasks finish.
                            let idx = if any_aged && aged_idx.is_none() {
                                None  // force wait
                            } else if let Some(i) = aged_idx {
                                Some(i)
                            } else {
                                // No aged tasks — normal budget-aware dispatch.
                                //
                                // Pass 2: CPU task if budget has room
                                let budget_idx = if remaining >= cfg.cpu_cpu_cost {
                                    st.queue.iter().position(|t| {
                                        t.kind == Kind::Cpu
                                            && t.cpu_cost <= remaining
                                    })
                                } else {
                                    None
                                };

                                // Pass 3: IO task
                                let io_idx = budget_idx.or_else(|| {
                                    st.queue.iter().position(|t| {
                                        t.kind == Kind::Io
                                            && t.cpu_cost <= remaining
                                    })
                                });

                                // Pass 4: any task that fits
                                io_idx.or_else(|| {
                                    st.queue.iter().position(|t| t.cpu_cost <= remaining)
                                })
                            };

                            match idx {
                                None => { st = cvar.wait(st).unwrap(); }
                                Some(chosen) => {
                                    for i in 0..chosen {
                                        st.queue[i].skip_count += 1;
                                    }
                                    let mut t = st.queue.remove(chosen).unwrap();
                                    t.start_time = Some(Instant::now());
                                    st.active_workers  += 1;
                                    st.active_cpu_load += t.cpu_cost;
                                    if !st.queue.is_empty() {
                                        cvar.notify_one();
                                    }
                                    break Some(t);
                                }
                            }
                        } else {
                            st = cvar.wait(st).unwrap();
                        }
                    }
                };

                match task {
                    None => break,
                    Some(mut task) => {
                        let arrived_at = task.arrival_time
                            .duration_since(sim_start)
                            .as_millis();

                        println!(
                            "[Worker {:>2}] START  task {:>4} | kind: {:?} | skips: {:>2} | arrived: {}ms",
                            id, task.id, task.kind, task.skip_count, arrived_at
                        );

                        thread::sleep(task.duration);
                        task.finish_time = Some(Instant::now());

                        println!(
                            "[Worker {:>2}] FINISH task {:>4} | kind: {:?} | arrived: {}ms | wait: {:.0}ms | turnaround: {:.0}ms",
                            id, task.id, task.kind, arrived_at,
                            task.wait_ms(), task.turnaround_ms()
                        );

                        {
                            let (lock, cvar) = &*state;
                            let mut st = lock.lock().unwrap();
                            st.active_workers  -= 1;
                            st.active_cpu_load -= task.cpu_cost;
                            if st.generation_done && st.queue.is_empty() {
                                cvar.notify_all();
                            } else {
                                cvar.notify_one();
                            }
                        }

                        completed.lock().unwrap().push(task);
                    }
                }
            }
        })
    }
}

fn start_queue_thread(
    rx:    mpsc::Receiver<Task>,
    state: Arc<(Mutex<QueueState>, Condvar)>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for task in rx {
            let (lock, cvar) = &*state;
            let mut st = lock.lock().unwrap();
            st.queue.push_back(task);
            cvar.notify_all();
        }
        let (lock, cvar) = &*state;
        let mut st = lock.lock().unwrap();
        st.generation_done = true;
        cvar.notify_all();
    })
}

fn avg(vals: &[f64]) -> f64 {
    if vals.is_empty() { 0.0 } else { vals.iter().sum::<f64>() / vals.len() as f64 }
}

fn main() {
    let sim_start = Instant::now();
    let cfg = Arc::new(Config::new());

    let (tx, rx) = mpsc::channel::<Task>();
    let state: Arc<(Mutex<QueueState>, Condvar)> =
        Arc::new((Mutex::new(QueueState::new()), Condvar::new()));
    let completed: Arc<Mutex<Vec<Task>>>     = Arc::new(Mutex::new(Vec::new()));
    let snapshots: Arc<Mutex<Vec<Snapshot>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = Arc::new(AtomicBool::new(false));

    println!("=======================================================");
    println!("  Optimized – Budget-Aware + Aging  |  Experiment B: Stressed (Burst)");
    println!("=======================================================");
    println!("  Tasks:           {}", cfg.num_tasks);
    println!("  Workers:         {} (shared pool)", cfg.num_workers);
    println!("  Arrival rate:    {} tasks every {} ms (BURST)", cfg.burst_size, cfg.arrival_interval_ms);
    println!("  IO task:         {:.0}% CPU, {} ms", cfg.io_cpu_cost  * 100.0, cfg.io_duration_ms);
    println!("  CPU task:        {:.0}% CPU, {} ms", cfg.cpu_cpu_cost * 100.0, cfg.cpu_duration_ms);
    println!("  IO/CPU ratio:    70% / 30%  |  Seed: {}", cfg.random_seed);
    println!("  Aging threshold: {} skips", cfg.aging_threshold);
    println!();
    println!("  Policy: budget-aware dispatch with aging");
    println!("    Problem with IO-first: 8 IO workers = 80% budget max.");
    println!("    20% budget always idle when all workers are on IO tasks.");
    println!("    Fix: when remaining budget >= 35%, prefer CPU tasks —");
    println!("    they fill that dead space (6IO+1CPU = 95% vs 8IO = 80%).");
    println!("    IO tasks still run concurrently, keeping IO wait low.");
    println!("    Aging (threshold={}) prevents CPU task starvation.", cfg.aging_threshold);
    println!("
[*] Running...");

    let monitor = Monitor::new(cfg.monitor_interval_ms);
    monitor.start(Arc::clone(&state), Arc::clone(&snapshots), Arc::clone(&shutdown), sim_start);

    let queue_handle = start_queue_thread(rx, Arc::clone(&state));

    let mut handles = Vec::new();
    for i in 0..cfg.num_workers {
        let h = Worker::new(i).start(
            Arc::clone(&state),
            Arc::clone(&completed),
            Arc::clone(&cfg),
            sim_start,
        );
        handles.push(h);
    }

    {
        let mut rng = StdRng::seed_from_u64(cfg.random_seed);
        let mut sent = 0usize;
        while sent < cfg.num_tasks {
            thread::sleep(Duration::from_millis(cfg.arrival_interval_ms));
            // Send a burst of tasks all at once
            let burst = cfg.burst_size.min(cfg.num_tasks - sent);
            for _ in 0..burst {
                let kind = if rng.random_bool(0.70) { Kind::Io } else { Kind::Cpu };
                let (cpu_cost, duration_ms) = cfg.task_for_kind(kind);
                let mut task = Task::new(
                    (sent + 1) as u32, kind, cpu_cost,
                    Duration::from_millis(duration_ms),
                );
                task.enqueue_time = Some(Instant::now());
                tx.send(task).unwrap();
                sent += 1;
            }
        }
    }
    drop(tx);

    queue_handle.join().unwrap();
    for h in handles { h.join().unwrap(); }

    shutdown.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(cfg.monitor_interval_ms + 5));

    let completed_tasks = completed.lock().unwrap().clone();
    let snapshots_data  = snapshots.lock().unwrap().clone();

    let io_done  = completed_tasks.iter().filter(|t| t.kind == Kind::Io ).count();
    let cpu_done = completed_tasks.iter().filter(|t| t.kind == Kind::Cpu).count();

    let waits:    Vec<f64> = completed_tasks.iter().map(|t| t.wait_ms()).collect();
    let turns:    Vec<f64> = completed_tasks.iter().map(|t| t.turnaround_ms()).collect();
    let io_waits: Vec<f64> = completed_tasks.iter()
        .filter(|t| t.kind == Kind::Io).map(|t| t.wait_ms()).collect();
    let cp_waits: Vec<f64> = completed_tasks.iter()
        .filter(|t| t.kind == Kind::Cpu).map(|t| t.wait_ms()).collect();
    let max_wait = waits.iter().cloned().fold(0.0_f64, f64::max);
    let aged = completed_tasks.iter().filter(|t| t.skip_count >= cfg.aging_threshold).count();

    let cpu_snaps: Vec<f64> = snapshots_data.iter().map(|s| s.cpu_percent).collect();
    let wkr_snaps: Vec<f64> = snapshots_data.iter().map(|s| s.active_workers as f64).collect();
    let avg_wkr = avg(&wkr_snaps);
    let total_work_ms: f64 = completed_tasks.iter()
        .map(|t| t.duration.as_millis() as f64).sum();

    println!("\n╔══════════════════════════════════════════════════════╗");
    println!("║  Optimized Results — Experiment B (Stressed/Burst)       ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!("\n  ── Required Metrics ──────────────────────────────────");
    println!("  Total tasks completed  : {}", completed_tasks.len());
    println!("  Makespan               : {} ms", sim_start.elapsed().as_millis());
    println!("  Total monitor time     : {} ms", snapshots_data.last().map_or(0, |s| s.time_ms));
    println!("  Total work time        : {:.0} ms", total_work_ms);
    println!("  Avg wait time          : {:.1} ms", avg(&waits));
    println!("  Avg turnaround time    : {:.1} ms", avg(&turns));
    println!("\n  ── Additional Metrics ────────────────────────────────");
    println!("  IO tasks completed     : {}", io_done);
    println!("  CPU tasks completed    : {}", cpu_done);
    println!("  Max wait time          : {:.0} ms", max_wait);
    println!("  Avg CPU consumption    : {:.1}%", avg(&cpu_snaps));
    println!("  Avg active workers     : {:.2} / {}", avg_wkr, cfg.num_workers);
    println!("  Worker utilization     : {:.1}%", avg_wkr / cfg.num_workers as f64 * 100.0);
    println!("  Avg wait (IO tasks)    : {:.1} ms", avg(&io_waits));
    println!("  Avg wait (CPU tasks)   : {:.1} ms", avg(&cp_waits));
    println!("  Fairness gap |IO-CPU|  : {:.1} ms", (avg(&io_waits) - avg(&cp_waits)).abs());
    println!("  Tasks aged/promoted    : {}", aged);
    println!("  Monitor snapshots      : {}", snapshots_data.len());
    println!("\n=======================================================");
    println!("  Experiment B (Stressed/Burst) — complete.");
    println!("=======================================================");
}