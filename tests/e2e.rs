use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc, Mutex,
};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{unbounded, Sender};
use lfdb::{Engine, EngineBuilder, Error, LogLevel, Logger};
use rand::{seq::IteratorRandom, thread_rng};
use tempfile::{tempdir_in, TempDir};

struct TestLogger;
impl Logger for TestLogger {
  fn log(&self, level: LogLevel, msg: &[u8]) {
    println!("[{}] {}", level.to_str(), String::from_utf8_lossy(msg))
  }
}

fn build_engine(dir: &TempDir) -> Engine {
  EngineBuilder::new(dir.path())
    .wal_file_size(8 << 20)
    .gc_thread_count(3)
    .buffer_pool_memory_capacity(32 << 20)
    .buffer_pool_shard_count(1 << 2)
    .group_commit_count(10)
    .gc_trigger_interval(Duration::from_secs(5))
    .logger(TestLogger)
    .log_level(LogLevel::Trace)
    .build()
    .expect("engine bootstrap failed")
}

const TEST_TABLE: &str = "test";

fn create_table(engine: &Engine, name: &str) {
  let mut tx = engine.new_tx().unwrap();
  tx.open_table(name).unwrap();
  tx.commit().unwrap();
}

// ============================================================
// 1. Basic CRUD
// ============================================================
#[test]
fn test_basic_crud() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx = engine.new_tx().unwrap();
  let table = tx.open_table(TEST_TABLE).unwrap();

  table.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();

  // read within same tx
  let val = table.get(&b"key1".to_vec()).unwrap();
  assert_eq!(val, Some(b"value1".to_vec()));

  // remove
  table.remove(&b"key1".to_vec()).unwrap();
  let val = table.get(&b"key1".to_vec()).unwrap();
  assert_eq!(val, None);

  tx.commit().unwrap();
}

// ============================================================
// 2. Commit Visibility
// ============================================================
#[test]
fn test_commit_visibility() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx1 = engine.new_tx().unwrap();
  tx1
    .open_table(TEST_TABLE)
    .unwrap()
    .insert(b"hello".to_vec(), b"world".to_vec())
    .unwrap();
  tx1.commit().unwrap();

  let tx2 = engine.new_tx().unwrap();
  let val = tx2
    .table(TEST_TABLE)
    .unwrap()
    .get(&b"hello".to_vec())
    .unwrap();
  assert_eq!(val, Some(b"world".to_vec()));
}

// ============================================================
// 3. Abort Invisibility
// ============================================================
#[test]
fn test_abort_invisibility() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  let mut tx1 = engine.new_tx().unwrap();
  let t1 = tx1.table(TEST_TABLE).unwrap();
  t1.insert(b"ghost".to_vec(), b"data".to_vec()).unwrap();
  tx1.abort().unwrap();

  let tx2 = engine.new_tx().unwrap();
  let t2 = tx2.table(TEST_TABLE).unwrap();
  let val = t2.get(&b"ghost".to_vec()).unwrap();
  assert_eq!(val, None);
}

// ============================================================
// 4. Drop Abort (implicit abort on drop)
// ============================================================
#[test]
fn test_drop_abort() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  {
    let tx1 = engine.new_tx().unwrap();
    let t1 = tx1.table(TEST_TABLE).unwrap();
    t1.insert(b"vanish".to_vec(), b"poof".to_vec()).unwrap();
    // no commit, no abort — just drop
  }

  let tx2 = engine.new_tx().unwrap();
  let t2 = tx2.table(TEST_TABLE).unwrap();
  let val = t2.get(&b"vanish".to_vec()).unwrap();
  assert_eq!(val, None);
}

// ============================================================
// 5. Write Conflict
// ============================================================
#[test]
fn test_write_conflict() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);
  {
    let mut tx1 = engine.new_tx().unwrap();
    let t1 = tx1.table(TEST_TABLE).unwrap();
    t1.insert(b"contested".to_vec(), b"v1".to_vec()).unwrap();
    // tx1 NOT committed — still active

    let tx2 = engine.new_tx().unwrap();
    let t2 = tx2.table(TEST_TABLE).unwrap();
    let result = t2.insert(b"contested".to_vec(), b"v2".to_vec());
    assert!(result.is_err());
    if let Err(Error::WriteConflict) = result {
      // expected
    } else {
      panic!("expected WriteConflict");
    }

    tx1.commit().unwrap();
  }

  {
    let key = b"key";
    let mut tx1 = engine.new_tx().unwrap();
    let t1 = tx1.table(TEST_TABLE).unwrap();
    t1.insert(key.to_vec(), b"value1".to_vec()).unwrap();

    let tx2 = engine.new_tx().unwrap();
    let t2 = tx2.table(TEST_TABLE).unwrap();
    assert_eq!(t2.get(key).unwrap(), None);

    tx1.commit().unwrap();

    // it should throw WriteConflict after commit.
    match t2.insert(key.to_vec(), b"value2".to_vec()) {
      Err(Error::WriteConflict) => {}
      _ => panic!("must be WriteConflict"),
    }
  }
}

// ============================================================
// 6. TransactionClosed
// ============================================================
#[test]
fn test_transaction_closed_after_commit() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  let mut tx = engine.new_tx().unwrap();
  {
    let table = tx.table(TEST_TABLE).unwrap();
    table.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
  }
  tx.commit().unwrap();

  assert!(matches!(
    tx.table(TEST_TABLE),
    Err(Error::TransactionClosed)
  ));
  assert!(matches!(tx.commit(), Err(Error::TransactionClosed)));
  assert!(matches!(tx.abort(), Err(Error::TransactionClosed)));
}

#[test]
fn test_transaction_closed_after_abort() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  let mut tx = engine.new_tx().unwrap();
  {
    let table = tx.table(TEST_TABLE).unwrap();
    table.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
  }
  tx.abort().unwrap();

  assert!(matches!(
    tx.table(TEST_TABLE),
    Err(Error::TransactionClosed)
  ));
}

// ============================================================
// 7. Scan
// ============================================================
#[test]
fn test_scan_range() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  let mut tx = engine.new_tx().unwrap();
  let table = tx.table(TEST_TABLE).unwrap();
  for i in 0u8..10 {
    table.insert(vec![i], vec![i * 10]).unwrap();
  }
  tx.commit().unwrap();

  let tx2 = engine.new_tx().unwrap();
  let t2 = tx2.table(TEST_TABLE).unwrap();

  // scan [3, 7)
  let mut iter = t2.scan(&vec![3], &vec![7]).unwrap();
  let mut results = vec![];
  while let Some((k, v)) = iter.try_next().unwrap() {
    results.push((k, v));
  }
  assert_eq!(results.len(), 4); // keys 3,4,5,6
  assert_eq!(results[0], (vec![3], vec![30]));
  assert_eq!(results[3], (vec![6], vec![60]));
}

#[test]
fn test_scan_all() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  let mut tx = engine.new_tx().unwrap();
  let table = tx.table(TEST_TABLE).unwrap();
  for i in 0u8..5 {
    table.insert(vec![i], vec![i]).unwrap();
  }
  tx.commit().unwrap();

  let tx2 = engine.new_tx().unwrap();
  let t2 = tx2.table(TEST_TABLE).unwrap();
  let mut iter = t2.scan_all().unwrap();
  let mut results = vec![];
  while let Some((k, v)) = iter.try_next().unwrap() {
    results.push((k, v));
  }
  assert_eq!(results.len(), 5);
  for i in 0u8..5 {
    assert_eq!(results[i as usize], (vec![i], vec![i]));
  }
}

// ============================================================
// 8. Overwrite
// ============================================================
#[test]
fn test_overwrite() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  let mut tx1 = engine.new_tx().unwrap();
  let t1 = tx1.table(TEST_TABLE).unwrap();
  t1.insert(b"key".to_vec(), b"v1".to_vec()).unwrap();
  tx1.commit().unwrap();

  let mut tx2 = engine.new_tx().unwrap();
  let t2 = tx2.table(TEST_TABLE).unwrap();
  t2.insert(b"key".to_vec(), b"v2".to_vec()).unwrap();
  tx2.commit().unwrap();

  let tx3 = engine.new_tx().unwrap();
  let t3 = tx3.table(TEST_TABLE).unwrap();
  let val = t3.get(&b"key".to_vec()).unwrap();
  assert_eq!(val, Some(b"v2".to_vec()));
}

// ============================================================
// 9. Crash Recovery
// ============================================================
#[test]
fn test_crash_recovery() {
  let dir = tempdir_in(".").unwrap();
  let committed_keys: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(vec![]));
  let stop = Arc::new(AtomicBool::new(false));

  // Phase 1: concurrent writes, then drop engine mid-flight
  {
    let engine = Arc::new(build_engine(&dir));
    create_table(&engine, TEST_TABLE);
    let mut handles = vec![];

    for t in 0..4u8 {
      let engine = engine.clone();
      let committed = committed_keys.clone();
      let stop = stop.clone();

      handles.push(thread::spawn(move || {
        for i in 0..100u8 {
          if stop.load(Ordering::Acquire) {
            break;
          }
          let key = vec![t, i];
          let value = vec![t, i, 0xFF];
          let mut tx = match engine.new_tx() {
            Ok(tx) => tx,
            Err(_) => break,
          };
          let table = match tx.table(TEST_TABLE) {
            Ok(t) => t,
            Err(_) => break,
          };
          if table.insert(key.clone(), value).is_err() {
            continue;
          }
          if tx.commit().is_ok() {
            committed.lock().unwrap().push(key);
          }
        }
      }));
    }

    // let workers run briefly then kill engine
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Release);
    for h in handles {
      h.join().expect("join error");
    }
    // engine dropped here
  }

  // Phase 2: restart and verify
  let engine = build_engine(&dir);
  let tx = engine.new_tx().unwrap();
  let table = tx.table(TEST_TABLE).unwrap();
  let keys = committed_keys.lock().unwrap();

  assert!(!keys.is_empty(), "should have committed at least some keys");

  for key in keys.iter() {
    let val = table.get(key).unwrap();
    assert!(
      val.is_some(),
      "committed key {:?} missing after recovery",
      key
    );
    let v = val.unwrap();
    assert_eq!(v[0], key[0]);
    assert_eq!(v[1], key[1]);
    assert_eq!(v[2], 0xFF);
  }
}

// ============================================================
// 10. Snapshot Isolation
// ============================================================
#[test]
fn test_snapshot_isolation() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  // tx1 and tx2 both start
  let mut tx1 = engine.new_tx().unwrap();
  let tx2 = engine.new_tx().unwrap();

  // tx1 inserts and commits AFTER tx2 started
  {
    let t1 = tx1.table(TEST_TABLE).unwrap();
    t1.insert(b"after".to_vec(), b"should-not-see".to_vec())
      .unwrap();
  }
  tx1.commit().unwrap();

  // tx2 should NOT see tx1's write (snapshot isolation)
  {
    let t2 = tx2.table(TEST_TABLE).unwrap();
    let val = t2.get(&b"after".to_vec()).unwrap();
    assert_eq!(val, None, "tx2 should not see tx1's post-start commit");
  }

  drop(tx2);

  // tx3 starts AFTER tx1 committed — should see it
  let tx3 = engine.new_tx().unwrap();
  let t3 = tx3.table(TEST_TABLE).unwrap();
  let val = t3.get(&b"after".to_vec()).unwrap();
  assert_eq!(val, Some(b"should-not-see".to_vec()));
}

// ============================================================
// 11. Entry Split (many versions on single key)
// ============================================================
#[test]
fn test_entry_split() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  let key = b"hot-key".to_vec();
  // 100-byte value * 50 txs → well over 4KB page, forces split
  let iterations = 50;

  for i in 0..iterations {
    let mut tx = engine.new_tx().unwrap();
    let table = tx.table(TEST_TABLE).unwrap();
    let value = vec![i as u8; 100];
    table.insert(key.clone(), value).unwrap();
    tx.commit().unwrap();
  }

  // verify latest value is readable
  let tx = engine.new_tx().unwrap();
  let table = tx.table(TEST_TABLE).unwrap();
  let val = table.get(&key).unwrap();
  assert!(
    val.is_some(),
    "hot key should be readable after many overwrites"
  );
  let v = val.unwrap();
  assert_eq!(v.len(), 100);
  assert_eq!(v[0], (iterations - 1) as u8);
}

// ============================================================
// 12. Large-scale Insert + Scan + Recovery (B-Tree node splits)
// ============================================================
#[test]
fn test_btree_node_split_and_recovery() {
  let dir = tempdir_in(".").unwrap();
  let key_count: usize = 100_000;

  // Phase 1: worker pool (100 threads) concurrently insert → forces leaf + internal node splits
  {
    let engine = Arc::new(build_engine(&dir));
    create_table(&engine, TEST_TABLE);
    let thread_count = 1000;
    let (task_tx, task_rx) =
      crossbeam::channel::unbounded::<(usize, crossbeam::channel::Sender<()>)>();

    let mut workers = Vec::new();
    for _ in 0..thread_count {
      let rx = task_rx.clone();
      let e = engine.clone();
      workers.push(thread::spawn(move || {
        while let Ok((i, done)) = rx.recv() {
          let mut cursor = e.new_tx().unwrap();
          let table = cursor.table(TEST_TABLE).unwrap();
          let key = format!("key-{:06}", i).into_bytes();
          let value = format!("val-{:06}", i).into_bytes();
          table.insert(key, value).unwrap();
          cursor.commit().unwrap();
          done.send(()).unwrap();
        }
      }));
    }

    let rng = &mut thread_rng();
    let mut completions = Vec::with_capacity(key_count);
    for i in (0..key_count).choose_multiple(rng, key_count) {
      let (done_tx, done_rx) = crossbeam::channel::unbounded();
      task_tx.send((i, done_tx)).unwrap();
      completions.push(done_rx);
    }
    completions.into_iter().for_each(|r| r.recv().unwrap());
    drop(task_tx);
    for w in workers {
      w.join().expect("worker panicked");
    }

    // scan all and verify order + completeness
    let tx = engine.new_tx().unwrap();
    let table = tx.table(TEST_TABLE).unwrap();
    let mut iter = table.scan_all().unwrap();
    let mut count = 0;
    let mut prev_key: Option<Vec<u8>> = None;
    while let Some((k, v)) = iter.try_next().unwrap() {
      // keys should be in sorted order
      if let Some(ref pk) = prev_key {
        assert!(k > *pk, "keys not sorted: {:?} >= {:?}", pk, k);
      }
      prev_key = Some(k.clone());

      // value matches key
      let expected_val = String::from_utf8_lossy(&k)
        .replacen("key", "val", 1)
        .into_bytes();
      assert_eq!(
        v,
        expected_val,
        "value mismatch for key {:?}",
        String::from_utf8_lossy(&k)
      );
      count += 1;
    }
    assert_eq!(
      count, key_count,
      "scan should return all {} keys",
      key_count
    );

    // point-read spot checks
    for i in [0, 1, 500, 2500, 4999] {
      let key = format!("key-{:06}", i).into_bytes();
      let val = table.get(&key).unwrap();
      assert!(
        val.is_some(),
        "key {:?} missing",
        String::from_utf8_lossy(&key)
      );
    }
    // engine dropped here
  }

  // Phase 2: restart engine and verify persistence
  {
    let engine = build_engine(&dir);
    let tx = engine.new_tx().unwrap();
    let table = tx.table(TEST_TABLE).unwrap();

    let mut iter = table.scan_all().unwrap();
    let mut count = 0;
    let mut prev_key: Option<Vec<u8>> = None;
    while let Some((k, v)) = iter.try_next().unwrap() {
      if let Some(ref pk) = prev_key {
        assert!(k > *pk, "keys not sorted after recovery");
      }
      prev_key = Some(k.clone());
      let expected_val = String::from_utf8_lossy(&k)
        .replacen("key", "val", 1)
        .into_bytes();
      assert_eq!(
        v,
        expected_val,
        "post-recovery value mismatch for {:?}",
        String::from_utf8_lossy(&k)
      );
      count += 1;
    }
    assert_eq!(
      count, key_count,
      "all {} keys should survive recovery",
      key_count
    );
  }
}

// ============================================================
// 13. Crash Recovery via process::exit
// ============================================================

// Child process: concurrent writes to 10 tables then process::exit (no drop, no flush)
#[test]
#[ignore]
fn crash_writer() {
  let dir = std::env::var("CRASH_DIR").expect("CRASH_DIR not set");
  let engine = EngineBuilder::new(std::path::Path::new(&dir))
    .group_commit_count(10)
    .build()
    .expect("engine bootstrap failed");
  let rng = &mut thread_rng();

  let table_count: usize = 10;
  let table_names: Vec<String> = (0..table_count)
    .map(|i| format!("table_{:02}", i))
    .collect();
  for name in &table_names {
    create_table(&engine, name);
  }

  let key_count: usize = 30_000;
  let thread_count = 1000;
  let (task_tx, task_rx) =
    crossbeam::channel::unbounded::<(usize, crossbeam::channel::Sender<()>)>();
  let engine = Arc::new(engine);
  let table_names = Arc::new(table_names);

  for _ in 0..thread_count {
    let rx = task_rx.clone();
    let e = engine.clone();
    let names = table_names.clone();
    thread::spawn(move || {
      while let Ok((i, done)) = rx.recv() {
        let table_name = &names[i % names.len()];
        let mut cursor = e.new_tx().unwrap();
        let table = cursor.table(table_name).unwrap();
        let key = format!("key-{:06}", i).into_bytes();
        let value = format!("val-{:06}", i).into_bytes();
        table.insert(key, value).unwrap();
        cursor.commit().unwrap();
        // output format: table_index:key_index
        let _ = std::io::Write::write_all(
          &mut std::io::stdout().lock(),
          format!("{:02}:{:06}\n", i % names.len(), i).as_bytes(),
        );
        let _ = done.send(());
      }
    });
  }

  let mut completions = Vec::new();
  for i in (0..key_count).choose_multiple(rng, key_count) {
    let (done_tx, done_rx) = crossbeam::channel::unbounded();
    task_tx.send((i, done_tx)).unwrap();
    completions.push(done_rx);
  }

  let mut c = 0;
  for done in completions {
    let _ = done.recv();
    c += 1;
    if c == key_count / 2 {
      std::process::exit(0);
    }
  }
}

// Parent process: spawn crash_writer, collect committed keys, verify recovery across 10 tables
#[test]
fn test_process_crash_recovery() {
  use std::io::BufRead;
  use std::process::{Command, Stdio};

  let dir = tempdir_in(".").unwrap();

  let table_count: usize = 10;
  let table_names: Vec<String> = (0..table_count)
    .map(|i| format!("table_{:02}", i))
    .collect();

  // Phase 1: spawn child that writes concurrently to 10 tables then crashes
  let mut child = Command::new(std::env::current_exe().unwrap())
    .arg("--ignored")
    .arg("--exact")
    .arg("crash_writer")
    .arg("--nocapture")
    .env("CRASH_DIR", dir.path())
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()
    .expect("failed to spawn crash_writer");

  let stdout = child.stdout.take().unwrap();
  let reader = std::io::BufReader::new(stdout);

  // committed per table: table_index -> set of key indices
  let mut committed: std::collections::HashMap<usize, std::collections::HashSet<usize>> =
    std::collections::HashMap::new();
  for s in reader.lines().map_while(|l| l.ok()) {
    // format: "TT:KKKKKK"
    if s.len() < 9 {
      continue;
    }
    if let Some((t, k)) = s.split_once(':') {
      if let (Ok(table_idx), Ok(key_idx)) =
        (t.trim().parse::<usize>(), k.trim().parse::<usize>())
      {
        committed.entry(table_idx).or_default().insert(key_idx);
      }
    }
  }

  let _ = child.wait();

  let total_committed: usize = committed.values().map(|s| s.len()).sum();
  assert!(
    total_committed > 0,
    "child should have committed at least some keys"
  );

  // Phase 2: reopen engine and verify all committed keys survived across all tables
  {
    let engine = build_engine(&dir);

    for (table_idx, keys) in &committed {
      let table_name = &table_names[*table_idx];
      let tx = engine.new_tx().unwrap();
      let table = tx.table(table_name).unwrap();

      for key_idx in keys {
        let key = format!("key-{:06}", key_idx).into_bytes();
        let expected = format!("val-{:06}", key_idx).into_bytes();
        let val = table.get(&key).unwrap();
        assert_eq!(
          val,
          Some(expected),
          "committed key {} in {} missing after crash recovery",
          key_idx,
          table_name
        );
      }
    }
  }

  eprintln!(
    "crash recovery: {} keys committed across {} tables, all verified",
    total_committed,
    committed.len()
  );
}

// ============================================================
// 14. Hard Workload (concurrent insert + scan)
// ============================================================
#[test]
fn test_hard_workload() {
  let dir = tempdir_in(".").unwrap();
  let engine = Arc::new(build_engine(&dir));
  let rng = &mut thread_rng();

  let table_count: usize = 10;
  let key_count: usize = 100_000;
  let thread_count: usize = 1000;

  let table_names: Vec<String> = (0..table_count)
    .map(|i| format!("table_{:02}", i))
    .collect();

  for name in &table_names {
    create_table(&engine, name);
  }

  // each key is assigned to a table by index % table_count
  let keys: Vec<(String, Vec<u8>)> = (0..key_count)
    .map(|i| {
      let table = table_names[i % table_count].clone();
      let key = format!("{:06}", i).as_bytes().to_vec();
      (table, key)
    })
    .choose_multiple(rng, key_count);

  let mut waiting = Vec::with_capacity(key_count);
  let mut threads = Vec::with_capacity(thread_count);
  let (tx, rx) = unbounded::<(String, Vec<u8>, Sender<()>)>();
  for _ in 0..thread_count {
    let e = engine.clone();
    let rx = rx.clone();
    let th = std::thread::spawn(move || {
      while let Ok((table_name, vec, t)) = rx.recv() {
        let mut r = e.new_tx().expect("start error");
        let table = r.table(&table_name).expect("table error");
        table.insert(vec.clone(), vec).expect("insert error");
        r.commit().expect("commit error");
        t.send(()).unwrap();
      }
    });
    threads.push(th);
  }

  for (table_name, key) in &keys {
    let (t, r) = unbounded();
    tx.send((table_name.clone(), key.clone(), t)).unwrap();
    waiting.push(r);
  }

  waiting.into_iter().for_each(|r| r.recv().unwrap());
  drop(tx);
  threads.into_iter().for_each(|h| h.join().unwrap());

  // verify each key
  let tx = engine.new_tx().unwrap();
  for (table, key) in &keys {
    let table = tx.table(table).unwrap();
    assert_eq!(table.get(key).unwrap(), Some(key.to_vec()))
  }

  // verify each table
  let keys_per_table: usize = key_count / table_count;
  for name in &table_names {
    let t = engine.new_tx().expect("tx start error");
    let table = t.table(name).expect("table error");
    let mut iter = table.scan_all().expect("scan start error");
    let mut count = 0;
    while let Ok(Some(_)) = iter.try_next() {
      count += 1;
    }
    assert_eq!(
      keys_per_table, count,
      "table {} expected {} keys, got {}",
      name, keys_per_table, count
    );
  }
}

// ============================================================
// 15. Heavy GC with concurrent reads (temp page coverage)
// ============================================================
#[test]
fn test_heavy_gc_single_key() {
  let dir = tempdir_in(".").unwrap();
  let engine = Arc::new(
    EngineBuilder::new(dir.path())
      .buffer_pool_memory_capacity(32 << 20)
      .buffer_pool_shard_count(1 << 2)
      .group_commit_count(10)
      .gc_trigger_interval(Duration::from_millis(50))
      .logger(TestLogger)
      .log_level(LogLevel::Trace)
      .build()
      .expect("engine bootstrap failed"),
  );

  create_table(&engine, TEST_TABLE);

  let key = b"hot-key".to_vec();
  let iterations = 500;
  let stop = Arc::new(AtomicBool::new(false));

  // writer: single key, sequential values
  let writer_engine = engine.clone();
  let writer_key = key.clone();
  let writer_stop = stop.clone();
  let last_value = Arc::new(Mutex::new(0u32));
  let last_val_writer = last_value.clone();

  let writer = thread::spawn(move || {
    for i in 0..iterations {
      if writer_stop.load(Ordering::Acquire) {
        break;
      }
      let mut tx = writer_engine.new_tx().unwrap();
      let table = tx.table(TEST_TABLE).unwrap();
      let value = (i as u32).to_le_bytes().to_vec();
      table.insert(writer_key.clone(), value).unwrap();
      tx.commit().unwrap();
      *last_val_writer.lock().unwrap() = i as u32;
    }
  });

  // reader: continuously read the same key
  let reader_engine = engine.clone();
  let reader_key = key.clone();
  let reader_stop = stop.clone();
  let reader = thread::spawn(move || {
    let mut read_count = 0u64;
    while !reader_stop.load(Ordering::Acquire) {
      let tx = reader_engine.new_tx().unwrap();
      let table = tx.table(TEST_TABLE).unwrap();
      let _ = table.get(&reader_key);
      read_count += 1;
    }
    read_count
  });

  writer.join().unwrap();
  stop.store(true, Ordering::Release);
  let read_count = reader.join().unwrap();

  // phase 1: verify final value
  let final_val = *last_value.lock().unwrap();
  {
    let tx = engine.new_tx().unwrap();
    {
      let table = tx.table(TEST_TABLE).unwrap();
      let val = table.get(&key).unwrap();
      assert!(val.is_some(), "key should exist after heavy writes");
      let bytes = val.unwrap();
      let stored = u32::from_le_bytes(bytes.try_into().unwrap());
      assert_eq!(stored, final_val, "final value mismatch");
    }
  }

  eprintln!(
    "heavy gc: {} writes, {} reads, final value = {}",
    iterations, read_count, final_val
  );

  // phase 2: restart and verify persistence
  drop(engine);
  let engine = build_engine(&dir);
  let tx = engine.new_tx().unwrap();
  let table = tx.table(TEST_TABLE).unwrap();
  let val = table.get(&key).unwrap();
  assert!(val.is_some(), "key should survive restart");
  let bytes = val.unwrap();
  let stored = u32::from_le_bytes(bytes.try_into().unwrap());
  assert_eq!(stored, final_val, "value mismatch after restart");
}

// ============================================================
// 16. Insert, Remove, and GC
// ============================================================
#[test]
fn insert_and_remove_and_gc() {
  let dir = tempdir_in(".").unwrap();
  let engine = Arc::new(build_engine(&dir));
  create_table(&engine, TEST_TABLE);

  let count: usize = 1000;
  for i in 0..count {
    let mut t = engine.new_tx().expect("tx start error");
    let table = t.table(TEST_TABLE).expect("table error");
    let bytes: Vec<u8> = i.to_le_bytes().into();
    table.insert(bytes.clone(), bytes).expect("insert error");
    t.commit().expect("commit error")
  }

  for i in 0..count {
    let mut t = engine.new_tx().expect("tx start error");
    let table = t.table(TEST_TABLE).expect("table error");
    let bytes: Vec<u8> = i.to_le_bytes().into();
    table.remove(&bytes).expect("remove error");
    t.commit().expect("commit error")
  }
  let e = engine.clone();
  let th = std::thread::spawn(move || {
    for _ in 0..count {
      let tx = e.new_tx().expect("tx start error");
      let table = tx.table(TEST_TABLE).expect("table error");
      let mut iter = table.scan_all().expect("scan error");
      while let Ok(Some(_)) = iter.try_next() {}
    }
  });
  std::thread::sleep(Duration::from_secs(60));

  for i in count..count << 1 {
    let mut t = engine.new_tx().expect("tx start error");
    let table = t.table(TEST_TABLE).expect("table error");
    let bytes: Vec<u8> = i.to_le_bytes().into();
    table.insert(bytes.clone(), bytes).expect("insert error");
    t.commit().expect("commit error")
  }
  th.join().unwrap();

  let engine = build_engine(&dir);

  let mut t = engine.new_tx().expect("tx start error");
  let table = t.table(TEST_TABLE).expect("table error");
  let mut iter = table.scan_all().expect("scan start error");

  let mut c = 0;
  while let Ok(Some(_)) = iter.try_next() {
    c += 1;
  }
  assert_eq!(c, count);

  for i in 0..count {
    let bytes: Vec<u8> = i.to_le_bytes().into();
    table.insert(bytes.clone(), bytes).expect("insert error");
  }

  t.commit().expect("commit error");

  let mut t = engine.new_tx().expect("tx start error");
  let table = t.table(TEST_TABLE).expect("table error");
  let mut iter = table.scan_all().expect("scan start error");

  let mut c = 0;
  while let Ok(Some(_)) = iter.try_next() {
    c += 1;
  }
  assert_eq!(c, count << 1);
  t.commit().expect("commit error");
}

// ============================================================
// 17. Start but not commit
// ============================================================
#[test]
#[ignore]
fn write_not_commit() {
  let dir = std::env::var("CRASH_DIR").expect("CRASH_DIR not set");
  let key = std::env::var("CRASH_KEY").expect("CRASH_KEY not set");
  let engine = EngineBuilder::new(std::path::Path::new(&dir))
    .group_commit_count(10)
    .build()
    .expect("engine bootstrap failed");
  create_table(&engine, TEST_TABLE);

  let t = engine.new_tx().expect("start failed.");
  let table = t.table(TEST_TABLE).expect("table failed");
  table
    .insert(key.as_bytes().to_vec(), key.as_bytes().to_vec())
    .expect("insert failed");

  let mut t2 = engine.new_tx().expect("start failed.");
  let table2 = t2.table(TEST_TABLE).expect("table failed");
  table2
    .insert(vec![1, 2, 3], vec![1, 2, 3])
    .expect("insert failed");
  t2.commit().expect("commit failed");

  std::process::exit(0);
}

#[test]
fn test_start_not_commit() {
  use std::io::stdout;
  use std::process::Command;
  let dir = tempdir_in(".").unwrap();

  let key = format!("key");
  // Phase 1: spawn child that writes concurrently then crashes
  if !Command::new(std::env::current_exe().unwrap())
    .arg("--ignored")
    .arg("--exact")
    .arg("write_not_commit")
    .arg("--")
    .arg("--nocapture")
    .env("CRASH_DIR", dir.path())
    .env("CRASH_KEY", key.clone())
    .stdout(stdout())
    .stderr(stdout())
    .status()
    .expect("failed to spawn write_not_commit")
    .success()
  {
    panic!("fail to run child")
  }

  let engine = build_engine(&dir);

  let mut t = engine.new_tx().expect("tx start error");
  let table = t.table(TEST_TABLE).expect("table error");
  assert_eq!(table.get(&key.into_bytes()).expect("find error"), None);
  t.commit().expect("commit error")
}

// ============================================================
// 18. Timeout
// ============================================================
#[test]
fn test_timeout() {
  let dir = tempdir_in(".").unwrap();
  let engine = EngineBuilder::new(dir.path())
    .transaction_timeout(Duration::from_millis(500))
    .logger(TestLogger)
    .log_level(LogLevel::Trace)
    .build()
    .unwrap();
  {
    let mut tx = engine.new_tx_timeout(Duration::from_secs(1)).unwrap();
    let table = tx.open_table(TEST_TABLE).unwrap();

    std::thread::sleep(Duration::from_secs(5));

    match table.get(b"123") {
      Err(lfdb::Error::TransactionClosed) => {}
      _ => panic!("must timeout"),
    }
  }

  {
    let mut tx = engine.new_tx_timeout(Duration::from_secs(1)).unwrap();
    let table = tx.open_table(TEST_TABLE).unwrap();
    let _ = table.get(b"123123");
  }
}

// ============================================================
// 19. Large key/value
// ============================================================
#[test]
fn test_large_key_value() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  let tx = engine.new_tx().expect("start error");
  let table = tx.table(TEST_TABLE).expect("table error");
  let large_key = vec![0; 257];
  let large_value = vec![0; (1 << 16) + 1];

  assert_eq!(table.insert(large_key, vec![]).is_err(), true);
  assert_eq!(table.insert(vec![], large_value).is_err(), true);
}

// ============================================================
// 20. Large key/value + GC
// ============================================================
#[test]
fn test_large_key_value_gc() {
  let dir = tempdir_in(".").unwrap();

  let count = 100;

  let mut keys = Vec::with_capacity(count);
  let mut values = Vec::with_capacity(count);

  for i in 0..count {
    keys.push(format!("{:06}", i).into_bytes());
    let mut v = vec![0; 1 << 15];
    v[..6].copy_from_slice(format!("{:06}", i).as_bytes());
    values.push(v);
  }

  {
    let engine = build_engine(&dir);
    create_table(&engine, TEST_TABLE);

    let mut tx = engine.new_tx().unwrap();
    let table = tx.table(TEST_TABLE).unwrap();
    for i in 0..(count / 2) {
      table.insert(keys[i].clone(), values[i].clone()).unwrap();
    }
    tx.commit().unwrap();

    let mut tx = engine.new_tx().unwrap();
    let table = tx.table(TEST_TABLE).unwrap();
    for i in 0..(count / 2) {
      table.remove(&keys[i]).unwrap();
    }
    tx.commit().unwrap();

    let mut tx = engine.new_tx().unwrap();
    let table = tx.table(TEST_TABLE).unwrap();
    for i in (count / 2)..count {
      table.insert(keys[i].clone(), values[i].clone()).unwrap();
    }
    tx.commit().unwrap();
  }

  {
    let engine = build_engine(&dir);

    let tx = engine.new_tx().unwrap();
    let table = tx.table(TEST_TABLE).unwrap();
    let mut iter = table.scan_all().unwrap();

    let mut found = 0;
    while let Ok(Some(_)) = iter.try_next() {
      found += 1;
    }
    assert_eq!(found, count / 2);

    let mut tx = engine.new_tx().unwrap();
    let table = tx.table(TEST_TABLE).unwrap();
    for i in (count / 2)..count {
      assert_eq!(table.get(&keys[i]).unwrap(), Some(values[i].clone()));
    }
    tx.commit().unwrap();
  }
}

// ============================================================
// 21. Snapshot Isolation 2
// ============================================================
#[test]
fn test_isolation() {
  let dir = tempdir_in(".").unwrap();
  let e = build_engine(&dir);
  create_table(&e, TEST_TABLE);

  let mut t1 = e.new_tx().unwrap();
  let tab1 = t1.table(TEST_TABLE).unwrap();
  tab1.insert(b"111".to_vec(), b"111".to_vec()).unwrap();

  let t2 = e.new_tx().unwrap();
  let tab2 = t2.table(TEST_TABLE).unwrap();
  assert_eq!(tab2.get(b"111").unwrap(), None);

  t1.commit().unwrap();

  assert_eq!(tab2.get(b"111").unwrap(), None);
}

// ============================================================
// 22. Abort open table
// ============================================================
#[test]
fn test_abort_open_table() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  {
    let mut tx = engine.new_tx().unwrap();
    let table = tx.open_table(TEST_TABLE).unwrap();
    for i in 0..100 {
      let key = format!("123{:06}", i).as_bytes().to_vec();
      table.insert(key.clone(), key).unwrap();
    }

    tx.abort().unwrap();
  }

  let tx = engine.new_tx().unwrap();
  match tx.table(TEST_TABLE) {
    Err(_) => {}
    Ok(_) => panic!("must error."),
  };
}

// ============================================================
// 23. Drop table commit makes it invisible
// ============================================================
#[test]
fn test_drop_then_commit_makes_invisible() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  // populate
  {
    let mut tx = engine.new_tx().unwrap();
    let t = tx.open_table(TEST_TABLE).unwrap();
    t.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
    tx.commit().unwrap();
  }

  // drop
  {
    let mut tx = engine.new_tx().unwrap();
    tx.drop_table(TEST_TABLE).unwrap();
    tx.commit().unwrap();
  }

  // dropped table must not be visible to subsequent transactions
  let tx = engine.new_tx().unwrap();
  match tx.table(TEST_TABLE) {
    Err(_) => {}
    Ok(_) => panic!("dropped table must not be visible"),
  }
}

// ============================================================
// 24. Drop table aborted keeps the table alive
// ============================================================
#[test]
fn test_drop_then_abort_keeps_table() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  // populate
  {
    let mut tx = engine.new_tx().unwrap();
    let t = tx.open_table(TEST_TABLE).unwrap();
    t.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
    tx.commit().unwrap();
  }

  // drop then abort
  {
    let mut tx = engine.new_tx().unwrap();
    tx.drop_table(TEST_TABLE).unwrap();
    tx.abort().unwrap();
  }

  // data must remain intact
  let tx = engine.new_tx().unwrap();
  let t = tx.table(TEST_TABLE).unwrap();
  assert_eq!(t.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}

// ============================================================
// 25. Create and drop within the same transaction
// ============================================================
#[test]
fn test_create_then_drop_in_same_tx_commit() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  {
    let mut tx = engine.new_tx().unwrap();
    tx.open_table(TEST_TABLE).unwrap();
    tx.drop_table(TEST_TABLE).unwrap();
    tx.commit().unwrap();
  }

  let tx = engine.new_tx().unwrap();
  match tx.table(TEST_TABLE) {
    Err(_) => {}
    Ok(_) => panic!("table created and dropped in same tx must not exist"),
  }
}

// ============================================================
// 26. Reopen after drop starts fresh
// ============================================================
#[test]
fn test_reopen_after_drop_starts_fresh() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  // first generation: insert data
  {
    let mut tx = engine.new_tx().unwrap();
    let t = tx.open_table(TEST_TABLE).unwrap();
    t.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
    tx.commit().unwrap();
  }

  // drop the table
  {
    let mut tx = engine.new_tx().unwrap();
    tx.drop_table(TEST_TABLE).unwrap();
    tx.commit().unwrap();
  }

  // reopen with the same name and write fresh data
  {
    let mut tx = engine.new_tx().unwrap();
    let t = tx.open_table(TEST_TABLE).unwrap();
    // old data must be gone
    assert_eq!(t.get(&b"k".to_vec()).unwrap(), None);
    t.insert(b"k2".to_vec(), b"v2".to_vec()).unwrap();
    tx.commit().unwrap();
  }

  let tx = engine.new_tx().unwrap();
  let t = tx.table(TEST_TABLE).unwrap();
  assert_eq!(t.get(&b"k".to_vec()).unwrap(), None);
  assert_eq!(t.get(&b"k2".to_vec()).unwrap(), Some(b"v2".to_vec()));
}

// ============================================================
// 27. Concurrent drop returns WriteConflict
// ============================================================
#[test]
fn test_concurrent_drop_returns_conflict() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  create_table(&engine, TEST_TABLE);

  let mut tx1 = engine.new_tx().unwrap();
  let mut tx2 = engine.new_tx().unwrap();

  tx1.drop_table(TEST_TABLE).unwrap();
  match tx2.drop_table(TEST_TABLE) {
    Err(Error::WriteConflict) => {}
    Err(e) => panic!("expected WriteConflict, got {:?}", e),
    Ok(_) => panic!("expected WriteConflict, got Ok"),
  }
}

// ============================================================
// 28. Drop and recreate many tables
// ============================================================
#[test]
fn test_drop_and_recreate_many_tables() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let table_names: Vec<String> = (0..10).map(|i| format!("table_{}", i)).collect();

  // first generation: create 10 tables and write 100 keys each
  {
    let mut tx = engine.new_tx().unwrap();
    for name in &table_names {
      let t = tx.open_table(name).unwrap();
      for i in 0..100 {
        let key = format!("k{:04}", i).into_bytes();
        let val = format!("v1-{}-{}", name, i).into_bytes();
        t.insert(key, val).unwrap();
      }
    }
    tx.commit().unwrap();
  }

  // drop all 10
  {
    let mut tx = engine.new_tx().unwrap();
    for name in &table_names {
      tx.drop_table(name).unwrap();
    }
    tx.commit().unwrap();
  }

  // dropped tables must not be visible
  {
    let tx = engine.new_tx().unwrap();
    for name in &table_names {
      assert!(tx.table(name).is_err(), "{} must not be visible", name);
    }
  }

  let kvs = (0..100)
    .map(|i| {
      (
        format!("k{:04}", i).into_bytes(),
        format!("v2-{}", i).into_bytes(),
      )
    })
    .collect::<Vec<_>>();
  // second generation: recreate the same names with fresh data
  {
    let mut tx = engine.new_tx().unwrap();
    for name in &table_names {
      let t = tx.open_table(name).unwrap();
      for (k, v) in kvs.iter() {
        t.insert(k.clone(), v.clone()).unwrap();
      }
    }
    tx.commit().unwrap();
  }

  // verify only the new generation is visible
  let tx = engine.new_tx().unwrap();
  for name in &table_names {
    let t = tx.table(name).unwrap();
    for (k, v) in kvs.iter() {
      assert_eq!(
        t.get(k).unwrap(),
        Some(v.clone()),
        "table {} key {k:?} should hold the second-generation value",
        name,
      );
    }
  }
}
