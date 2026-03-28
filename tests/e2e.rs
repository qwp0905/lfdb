use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc, Mutex,
};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{unbounded, Sender};
use lfkv_db::{Engine, EngineBuilder, Error, LogLevel, Logger};
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
    .buffer_pool_memory_capacity(32 << 20)
    .buffer_pool_shard_count(1 << 2)
    .group_commit_count(10)
    .gc_trigger_interval(Duration::from_secs(5))
    .logger(TestLogger)
    .log_level(LogLevel::Trace)
    .build()
    .expect("engine bootstrap failed")
}

// ============================================================
// 1. Basic CRUD
// ============================================================
#[test]
fn test_basic_crud() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx = engine.new_tx().unwrap();
  tx.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();

  // read within same tx
  let val = tx.get(&b"key1".to_vec()).unwrap();
  assert_eq!(val, Some(b"value1".to_vec()));

  // remove
  tx.remove(&b"key1".to_vec()).unwrap();
  let val = tx.get(&b"key1".to_vec()).unwrap();
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
  tx1.insert(b"hello".to_vec(), b"world".to_vec()).unwrap();
  tx1.commit().unwrap();

  let tx2 = engine.new_tx().unwrap();
  let val = tx2.get(&b"hello".to_vec()).unwrap();
  assert_eq!(val, Some(b"world".to_vec()));
}

// ============================================================
// 3. Abort Invisibility
// ============================================================
#[test]
fn test_abort_invisibility() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx1 = engine.new_tx().unwrap();
  tx1.insert(b"ghost".to_vec(), b"data".to_vec()).unwrap();
  tx1.abort().unwrap();

  let tx2 = engine.new_tx().unwrap();
  let val = tx2.get(&b"ghost".to_vec()).unwrap();
  assert_eq!(val, None);
}

// ============================================================
// 4. Drop Abort (implicit abort on drop)
// ============================================================
#[test]
fn test_drop_abort() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  {
    let tx1 = engine.new_tx().unwrap();
    tx1.insert(b"vanish".to_vec(), b"poof".to_vec()).unwrap();
    // no commit, no abort — just drop
  }

  let tx2 = engine.new_tx().unwrap();
  let val = tx2.get(&b"vanish".to_vec()).unwrap();
  assert_eq!(val, None);
}

// ============================================================
// 5. Write Conflict
// ============================================================
#[test]
fn test_write_conflict() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);
  {
    let mut tx1 = engine.new_tx().unwrap();
    tx1.insert(b"contested".to_vec(), b"v1".to_vec()).unwrap();
    // tx1 NOT committed — still active

    let tx2 = engine.new_tx().unwrap();
    let result = tx2.insert(b"contested".to_vec(), b"v2".to_vec());
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
    tx1.insert(key.to_vec(), b"value1".to_vec()).unwrap();

    let tx2 = engine.new_tx().unwrap();
    assert_eq!(tx2.get(key).unwrap(), None);

    tx1.commit().unwrap();

    // it should throw WriteConflict after commit.
    match tx2.insert(key.to_vec(), b"value2".to_vec()) {
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

  let mut tx = engine.new_tx().unwrap();
  tx.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
  tx.commit().unwrap();

  assert!(matches!(
    tx.insert(b"k2".to_vec(), b"v2".to_vec()),
    Err(Error::TransactionClosed)
  ));
  assert!(matches!(
    tx.get(&b"k".to_vec()),
    Err(Error::TransactionClosed)
  ));
  assert!(matches!(
    tx.remove(&b"k".to_vec()),
    Err(Error::TransactionClosed)
  ));
  assert!(matches!(tx.scan_all(), Err(Error::TransactionClosed)));
  assert!(matches!(tx.commit(), Err(Error::TransactionClosed)));
  assert!(matches!(tx.abort(), Err(Error::TransactionClosed)));
}

#[test]
fn test_transaction_closed_after_abort() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let mut tx = engine.new_tx().unwrap();
  tx.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
  tx.abort().unwrap();

  assert!(matches!(
    tx.insert(b"k2".to_vec(), b"v2".to_vec()),
    Err(Error::TransactionClosed)
  ));
  assert!(matches!(
    tx.get(&b"k".to_vec()),
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

  let mut tx = engine.new_tx().unwrap();
  for i in 0u8..10 {
    tx.insert(vec![i], vec![i * 10]).unwrap();
  }
  tx.commit().unwrap();

  let tx2 = engine.new_tx().unwrap();

  // scan [3, 7)
  let mut iter = tx2.scan(&vec![3], &vec![7]).unwrap();
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

  let mut tx = engine.new_tx().unwrap();
  for i in 0u8..5 {
    tx.insert(vec![i], vec![i]).unwrap();
  }
  tx.commit().unwrap();

  let tx2 = engine.new_tx().unwrap();
  let mut iter = tx2.scan_all().unwrap();
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

  let mut tx1 = engine.new_tx().unwrap();
  tx1.insert(b"key".to_vec(), b"v1".to_vec()).unwrap();
  tx1.commit().unwrap();

  let mut tx2 = engine.new_tx().unwrap();
  tx2.insert(b"key".to_vec(), b"v2".to_vec()).unwrap();
  tx2.commit().unwrap();

  let tx3 = engine.new_tx().unwrap();
  let val = tx3.get(&b"key".to_vec()).unwrap();
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
          if tx.insert(key.clone(), value).is_err() {
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
  let keys = committed_keys.lock().unwrap();

  assert!(!keys.is_empty(), "should have committed at least some keys");

  for key in keys.iter() {
    let val = tx.get(key).unwrap();
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

  // tx1 and tx2 both start
  let mut tx1 = engine.new_tx().unwrap();
  let tx2 = engine.new_tx().unwrap();

  // tx1 inserts and commits AFTER tx2 started
  tx1
    .insert(b"after".to_vec(), b"should-not-see".to_vec())
    .unwrap();
  tx1.commit().unwrap();

  // tx2 should NOT see tx1's write (snapshot isolation)
  let val = tx2.get(&b"after".to_vec()).unwrap();
  assert_eq!(val, None, "tx2 should not see tx1's post-start commit");
  drop(tx2);

  // tx3 starts AFTER tx1 committed — should see it
  let tx3 = engine.new_tx().unwrap();
  let val = tx3.get(&b"after".to_vec()).unwrap();
  assert_eq!(val, Some(b"should-not-see".to_vec()));
}

// ============================================================
// 11. Entry Split (many versions on single key)
// ============================================================
#[test]
fn test_entry_split() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let key = b"hot-key".to_vec();
  // 100-byte value * 50 txs → well over 4KB page, forces split
  let iterations = 50;

  for i in 0..iterations {
    let mut tx = engine.new_tx().unwrap();
    let value = vec![i as u8; 100];
    tx.insert(key.clone(), value).unwrap();
    tx.commit().unwrap();
  }

  // verify latest value is readable
  let tx = engine.new_tx().unwrap();
  let val = tx.get(&key).unwrap();
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
          let key = format!("key-{:06}", i).into_bytes();
          let value = format!("val-{:06}", i).into_bytes();
          cursor.insert(key, value).unwrap();
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
    let mut iter = tx.scan_all().unwrap();
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
      let val = tx.get(&key).unwrap();
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

    let mut iter = tx.scan_all().unwrap();
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

// Child process: concurrent writes then process::exit (no drop, no flush)
#[test]
#[ignore]
fn crash_writer() {
  let dir = std::env::var("CRASH_DIR").expect("CRASH_DIR not set");
  let engine = EngineBuilder::new(std::path::Path::new(&dir))
    .group_commit_count(10)
    .build()
    .expect("engine bootstrap failed");
  let rng = &mut thread_rng();

  let key_count: usize = 30_000;
  let thread_count = 1000;
  let (task_tx, task_rx) =
    crossbeam::channel::unbounded::<(usize, crossbeam::channel::Sender<()>)>();
  let engine = Arc::new(engine);

  for _ in 0..thread_count {
    let rx = task_rx.clone();
    let e = engine.clone();
    thread::spawn(move || {
      while let Ok((i, done)) = rx.recv() {
        let mut cursor = e.new_tx().unwrap();
        let key = format!("key-{:06}", i).into_bytes();
        let value = format!("val-{:06}", i).into_bytes();
        cursor.insert(key, value).unwrap();
        cursor.commit().unwrap();
        let _ = std::io::Write::write_all(
          &mut std::io::stdout().lock(),
          format!("{:07}\n", i).as_bytes(),
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

// Parent process: spawn crash_writer, collect committed keys, verify recovery
#[test]
fn test_process_crash_recovery() {
  use std::io::BufRead;
  use std::process::{Command, Stdio};

  let dir = tempdir_in(".").unwrap();

  // Phase 1: spawn child that writes concurrently then crashes
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

  let mut committed = std::collections::HashSet::new();
  for s in reader.lines().map_while(|l| l.ok()) {
    if s.len() < 7 {
      continue;
    }
    if let Ok(i) = s.trim().parse::<usize>() {
      committed.insert(i);
    }
  }

  let _ = child.wait();

  assert!(
    !committed.is_empty(),
    "child should have committed at least some keys"
  );

  // Phase 2: reopen engine and verify all committed keys survived
  {
    let engine = build_engine(&dir);
    let mut tx = engine.new_tx().unwrap();
    let mut c = 0;
    let mut iter = tx.scan_all().unwrap();
    while let Ok(Some(_)) = iter.try_next() {
      c += 1;
    }
    assert!(
      c >= committed.len(),
      "exists {c} committed {}",
      committed.len()
    );
    for i in &committed {
      let key = format!("key-{:06}", i).into_bytes();
      let expected = format!("val-{:06}", i).into_bytes();
      let val = tx.get(&key).unwrap();
      assert_eq!(
        val,
        Some(expected),
        "committed key {} missing after crash recovery",
        i
      );
    }
    tx.commit().unwrap();
  }

  eprintln!(
    "crash recovery: {} keys committed, all verified",
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

  let key_count: usize = 100000;
  let thread_count: usize = 1000;
  let mut keys: Vec<Vec<u8>> = (0..key_count)
    .map(|i| format!("123{:06}", i))
    .map(|s| s.as_bytes().to_vec())
    .choose_multiple(rng, key_count);

  let mut waiting = Vec::with_capacity(key_count);
  let mut threads = Vec::with_capacity(thread_count);
  let (tx, rx) = unbounded::<(Vec<u8>, Sender<()>)>();
  for _ in 0..thread_count {
    let e = engine.clone();
    let rx = rx.clone();
    let th = std::thread::spawn(move || {
      while let Ok((vec, t)) = rx.recv() {
        let mut r = e.new_tx().expect("start error");
        r.insert(vec.clone(), vec).expect("insert error");
        r.commit().expect("commit error");
        t.send(()).unwrap();
      }
    });
    threads.push(th);
  }

  for i in 0..key_count {
    let (t, r) = unbounded();
    tx.send((keys[i].clone(), t)).unwrap();
    waiting.push(r);
  }

  waiting.into_iter().for_each(|r| r.recv().unwrap());
  drop(tx);

  threads.into_iter().for_each(|h| h.join().unwrap());

  keys.sort();
  let mut count = 0;
  let mut t = engine.new_tx().expect("tx start error");

  let mut iter = t.scan_all().expect("scan start error");
  while let Ok(Some((k, _))) = iter.try_next() {
    assert_eq!(keys[count], k);
    count += 1;
  }

  assert_eq!(key_count, count);
  t.commit().expect("commit error");
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
      let value = (i as u32).to_le_bytes().to_vec();
      tx.insert(writer_key.clone(), value).unwrap();
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
      let _ = tx.get(&reader_key);
      read_count += 1;
    }
    read_count
  });

  writer.join().unwrap();
  stop.store(true, Ordering::Release);
  let read_count = reader.join().unwrap();

  // phase 1: verify final value
  let final_val = *last_value.lock().unwrap();
  let tx = engine.new_tx().unwrap();
  let val = tx.get(&key).unwrap();
  assert!(val.is_some(), "key should exist after heavy writes");
  let bytes = val.unwrap();
  let stored = u32::from_le_bytes(bytes.try_into().unwrap());
  assert_eq!(stored, final_val, "final value mismatch");
  drop(tx);

  eprintln!(
    "heavy gc: {} writes, {} reads, final value = {}",
    iterations, read_count, final_val
  );

  // phase 2: restart and verify persistence
  drop(engine);
  let engine = build_engine(&dir);
  let tx = engine.new_tx().unwrap();
  let val = tx.get(&key).unwrap();
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

  let count: usize = 1000;
  for i in 0..count {
    let mut t = engine.new_tx().expect("tx start error");
    let bytes: Vec<u8> = i.to_le_bytes().into();
    t.insert(bytes.clone(), bytes).expect("insert error");
    t.commit().expect("commit error")
  }

  for i in 0..count {
    let mut t = engine.new_tx().expect("tx start error");
    let bytes: Vec<u8> = i.to_le_bytes().into();
    t.remove(&bytes).expect("insert error");
    t.commit().expect("commit error")
  }
  let e = engine.clone();
  let th = std::thread::spawn(move || {
    for _ in 0..count {
      let tx = e.new_tx().expect("tx start error");
      let mut iter = tx.scan_all().expect("scan error");
      while let Ok(Some(_)) = iter.try_next() {}
    }
  });
  std::thread::sleep(Duration::from_secs(60));

  for i in count..count << 1 {
    let mut t = engine.new_tx().expect("tx start error");
    let bytes: Vec<u8> = i.to_le_bytes().into();
    t.insert(bytes.clone(), bytes).expect("insert error");
    t.commit().expect("commit error")
  }
  th.join().unwrap();

  let engine = build_engine(&dir);

  let mut t = engine.new_tx().expect("tx start error");
  let mut iter = t.scan_all().expect("scan start error");

  let mut c = 0;
  while let Ok(Some(_)) = iter.try_next() {
    c += 1;
  }
  assert_eq!(c, count);

  for i in 0..count {
    let bytes: Vec<u8> = i.to_le_bytes().into();
    t.insert(bytes.clone(), bytes).expect("insert error");
  }

  t.commit().expect("commit error");

  let mut t = engine.new_tx().expect("tx start error");
  let mut iter = t.scan_all().expect("scan start error");

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

  let t = engine.new_tx().expect("start failed.");
  t.insert(key.as_bytes().to_vec(), key.as_bytes().to_vec())
    .expect("insert failed");

  let mut t2 = engine.new_tx().expect("start failed.");
  t2.insert(vec![1, 2, 3], vec![1, 2, 3])
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
  assert_eq!(t.get(&key.into_bytes()).expect("find error"), None);
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

  let tx = engine.new_tx_timeout(Duration::from_secs(1)).unwrap();

  std::thread::sleep(Duration::from_secs(5));

  match tx.get(b"123") {
    Err(lfkv_db::Error::TransactionClosed) => {}
    _ => panic!("must timeout"),
  }

  let tx = engine.new_tx_timeout(Duration::from_secs(1)).unwrap();
  let _ = tx.get(b"123123");
}

// ============================================================
// 19. Large key/value
// ============================================================
#[test]
fn test_large_key_value() {
  let dir = tempdir_in(".").unwrap();
  let engine = build_engine(&dir);

  let tx = engine.new_tx().expect("start error");
  let large_key = vec![0; 257];
  let large_value = vec![0; (1 << 16) + 1];

  assert_eq!(tx.insert(large_key, vec![]).is_err(), true);
  assert_eq!(tx.insert(vec![], large_value).is_err(), true);
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

    let mut tx = engine.new_tx().unwrap();
    for i in 0..(count / 2) {
      tx.insert(keys[i].clone(), values[i].clone()).unwrap();
    }
    tx.commit().unwrap();

    let mut tx = engine.new_tx().unwrap();
    for i in 0..(count / 2) {
      tx.remove(&keys[i]).unwrap();
    }
    tx.commit().unwrap();

    let mut tx = engine.new_tx().unwrap();
    for i in (count / 2)..count {
      tx.insert(keys[i].clone(), values[i].clone()).unwrap();
    }
    tx.commit().unwrap();
  }

  {
    let engine = build_engine(&dir);

    let tx = engine.new_tx().unwrap();
    let mut iter = tx.scan_all().unwrap();

    let mut found = 0;
    while let Ok(Some(_)) = iter.try_next() {
      found += 1;
    }
    assert_eq!(found, count / 2);

    let mut tx = engine.new_tx().unwrap();
    for i in (count / 2)..count {
      assert_eq!(tx.get(&keys[i]).unwrap(), Some(values[i].clone()));
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

  let mut t1 = e.new_tx().unwrap();
  t1.insert(b"111".to_vec(), b"111".to_vec()).unwrap();

  let t2 = e.new_tx().unwrap();
  assert_eq!(t2.get(b"111").unwrap(), None);

  t1.commit().unwrap();

  assert_eq!(t2.get(b"111").unwrap(), None);
}
