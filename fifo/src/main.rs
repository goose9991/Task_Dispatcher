use rand::prelude::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

// ── Task kind ────────────────────────────────────────────────
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind { Io, Cpu }

// ── Task ─────────────────────────────────────────────────────
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
}

impl Task {
    fn new(id: u32, kind: Kind, cpu_cost: f64, duration: Duration) -> Self {
        Task {
            id,
            kind,
            cpu_cost,
            duration,
            arrival_time: Instant::now(),
            enqueue_time: None,
            start_time:   None,
            finish_time:  None,
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

// ── Config ────────────────────────────────────────────────────
struct Config {
    num_tasks:           usize,
    arrival_interval_ms: u64,
    num_workers:         usize,
    monitor_interval_ms: u64,
    random_seed:         u64,
    io_cpu_cost:         f64,
    io_duration_ms:      u64,
    cpu_cpu_cost:        f64,
    cpu_duration_ms:     u64,
}

impl Config {
    fn new() -> Self {
        Config {
            num_tasks:           1000,
            arrival_interval_ms: 20,
            num_workers:         8,
            monitor_interval_ms: 10,
            random_seed:         42,
            io_cpu_cost:         0.10,
            io_duration_ms:      200,
            cpu_cpu_cost:        0.35,
            cpu_duration_ms:     200,
        }
    }
    fn task_for_kind(&self, kind: Kind) -> (f64, u64) {
        match kind {
            Kind::Io  => (self.io_cpu_cost,  self.io_duration_ms),
            Kind::Cpu => (self.cpu_cpu_cost, self.cpu_duration_ms),
        }
    }
}

// ── Monitor ───────────────────────────────────────────────────
struct Monitor {
    interval_ms: u64,
}

#[derive(Clone, Debug)]
struct Snapshot {
    time_ms:        u64,
    cpu_percent:    f64,
    active_workers: usize,
}

impl Monitor {
    fn new(interval_ms: u64) -> Self {
        Monitor { interval_ms }
    }
    fn start(
        &self,
        state: Arc<(Mutex<QueueState>, Condvar)>,
        snapshots: Arc<Mutex<Vec<Snapshot>>>,
        shutdown: Arc<AtomicBool>,
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

// ── Shared queue state ────────────────────────────────────────
struct QueueState {
    queue:           VecDeque<Task>,
    active_workers:  usize,
    active_cpu_load: f64,
    generation_done: bool,
}

impl QueueState {
    fn new() -> Self {
        QueueState {
            queue:  VecDeque::new(),
            active_workers:  0,
            active_cpu_load: 0.0,
            generation_done: false,
        }
    }
}

// ── Queue thread ──────────────────────────────────────────────
fn start_queue_thread(
    rx: mpsc::Receiver<Task>,
    state: Arc<(Mutex<QueueState>, Condvar)>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for task in rx {
            let (lock, cvar) = &*state;
            let mut st = lock.lock().unwrap();
            st.queue.push_back(task);
            cvar.notify_all();
        }
        // Channel closed — all tasks sent
        let (lock, cvar) = &*state;
        let mut st = lock.lock().unwrap();
        st.generation_done = true;
        cvar.notify_all();
    })
}

// ── Worker ────────────────────────────────────────────────────
struct Worker {
    id: usize,
}

impl Worker {
    fn new(id: usize) -> Self {
        Worker { id }
    }
    fn start(
        &self,
        state: Arc<(Mutex<QueueState>, Condvar)>,
        completed: Arc<Mutex<Vec<Task>>>,
        num_workers: usize,
        sim_start: Instant,
    ) -> thread::JoinHandle<()> {
        let id = self.id;
        thread::spawn(move || {
            loop {
                let task = {
                    let (lock, cvar) = &*state;
                    let mut st = lock.lock().unwrap();
                    loop {
                        // Exit: generation done and queue empty
                        if st.generation_done && st.queue.is_empty() {
                            break None;
                        }

                        // FIFO: dispatch front of queue if worker slot and CPU headroom available
                        let can_take = st.queue.front().map_or(false, |front| {
                            st.active_workers < num_workers
                                && st.active_cpu_load + front.cpu_cost <= 1.0
                        });

                        if can_take {
                            let mut t = st.queue.pop_front().unwrap();
                            t.start_time = Some(Instant::now());
                            st.active_workers  += 1;
                            st.active_cpu_load += t.cpu_cost;
                            break Some(t);
                        }

                        // Wait — a worker finishing will free CPU headroom and wake us
                        let (new_st, _) = cvar
                            .wait_timeout(st, Duration::from_millis(5))
                            .unwrap();
                        st = new_st;
                    }
                };

                match task {
                    None => break,
                    Some(mut task) => {
                        let arrived_at = task.arrival_time
                            .duration_since(sim_start)
                            .as_millis();

                        println!(
                            "[Worker {:>2}] START  task {:>4} | kind: {:?} | arrived: {}ms",
                            id, task.id, task.kind, arrived_at
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
                            cvar.notify_all();
                        }

                        completed.lock().unwrap().push(task);
                    }
                }
            }
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────
fn avg(vals: &[f64]) -> f64 {
    if vals.is_empty() { 0.0 } else { vals.iter().sum::<f64>() / vals.len() as f64 }
}

// ── Main ──────────────────────────────────────────────────────
fn main() {
    let sim_start = Instant::now();
    let cfg = Config::new();

    let (tx, rx) = mpsc::channel::<Task>();

    let state: Arc<(Mutex<QueueState>, Condvar)> =
        Arc::new((Mutex::new(QueueState::new()), Condvar::new()));
    let completed: Arc<Mutex<Vec<Task>>>     = Arc::new(Mutex::new(Vec::new()));
    let snapshots: Arc<Mutex<Vec<Snapshot>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = Arc::new(AtomicBool::new(false));

    println!("=======================================================");
    println!(" FIFO Task Dispatcher");
    println!("=======================================================");
    println!("  Tasks:        {}", cfg.num_tasks);
    println!("  Workers:      {}", cfg.num_workers);
    println!("  Arrival rate: 1 task every {} ms", cfg.arrival_interval_ms);
    println!("  IO task:      {:.0}% CPU, {} ms", cfg.io_cpu_cost * 100.0, cfg.io_duration_ms);
    println!("  CPU task:     {:.0}% CPU, {} ms", cfg.cpu_cpu_cost * 100.0, cfg.cpu_duration_ms);
    println!("  IO/CPU ratio: 70% / 30%  |  Seed: {}", cfg.random_seed);
    println!("\n[*] Running...");

    // Start monitor
    let monitor = Monitor::new(cfg.monitor_interval_ms);
    monitor.start(Arc::clone(&state), Arc::clone(&snapshots), Arc::clone(&shutdown), sim_start);

    // Start queue thread
    let queue_handle = start_queue_thread(rx, Arc::clone(&state));

    // Start workers
    let mut handles = Vec::new();
    for i in 0..cfg.num_workers {
        let h = Worker::new(i).start(
            Arc::clone(&state),
            Arc::clone(&completed),
            cfg.num_workers,
            sim_start,
        );
        handles.push(h);
    }

    // Generator — send 1 task every 20ms
    {
        let mut rng = StdRng::seed_from_u64(cfg.random_seed);
        for i in 0..cfg.num_tasks {
            thread::sleep(Duration::from_millis(cfg.arrival_interval_ms));
            let kind = if rng.random_bool(0.70) { Kind::Io } else { Kind::Cpu };
            let (cpu_cost, duration_ms) = cfg.task_for_kind(kind);
            let mut task = Task::new((i + 1) as u32, kind, cpu_cost,
                                     Duration::from_millis(duration_ms));
            task.enqueue_time = Some(Instant::now());
            tx.send(task).unwrap();
        }
    }
    drop(tx);

    queue_handle.join().unwrap();

    for h in handles {
        h.join().unwrap();
    }

    // Stop monitor
    shutdown.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(cfg.monitor_interval_ms + 5));

    // ── Print results ─────────────────────────────────────────
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

    let cpu_snaps: Vec<f64> = snapshots_data.iter().map(|s| s.cpu_percent).collect();
    let wkr_snaps: Vec<f64> = snapshots_data.iter().map(|s| s.active_workers as f64).collect();
    let avg_wkr = avg(&wkr_snaps);

    let total_work_ms: f64 = completed_tasks.iter()
        .map(|t| t.duration.as_millis() as f64).sum();

    println!("\n╔══════════════════════════════════════════════════════╗");
    println!("║  FIFO Results                                        ║");
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
    println!("  Tasks aged/promoted    : {}", 0);
    println!("  Monitor snapshots      : {}", snapshots_data.len());

    println!("\n=======================================================");
    println!("  Simulation complete. Clean shutdown confirmed.");
    println!("=======================================================");
}