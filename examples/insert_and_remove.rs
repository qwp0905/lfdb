use lfdb::{EngineBuilder, LogLevel, Logger};

struct DebugLogger;
impl Logger for DebugLogger {
  fn log(&self, level: LogLevel, msg: &[u8]) {
    println!("[{}] {}", level.to_str(), String::from_utf8_lossy(msg))
  }
}
fn main() {
  let engine = EngineBuilder::new("./.local")
    .logger(DebugLogger)
    .log_level(LogLevel::Trace)
    .build()
    .expect("bootstrap error");

  let table = "test";
  {
    let t = engine.new_tx().unwrap();
    t.open_table(table).unwrap();
    t.commit().unwrap();
  }

  let count = 1_000_usize;
  {
    for i in 0..count {
      let t = engine.new_tx().expect("tx start error");
      let bytes: Vec<u8> = i.to_le_bytes().into();
      t.table(table)
        .unwrap()
        .insert(bytes.clone(), bytes)
        .expect("insert error");
      t.commit().expect("commit error")
    }

    println!("insert done");

    for i in 0..count {
      let t = engine.new_tx().expect("tx start error");
      let bytes: Vec<u8> = i.to_le_bytes().into();
      t.table(table)
        .unwrap()
        .remove(&bytes)
        .expect("insert error");
      t.commit().expect("commit error")
    }
    println!("remove done");

    let tt = engine.new_tx().expect("tx start error");
    tt.table(table)
      .unwrap()
      .insert(count.to_le_bytes().into(), count.to_le_bytes().into())
      .expect("insert error");
    tt.commit().expect("commit error");
  }
  drop(engine);

  let engine = EngineBuilder::new("./.local")
    .build()
    .expect("bootstrap error");

  let t = engine.new_tx().expect("tx start error");
  let tt = t.table(table).unwrap();
  let mut iter = tt.scan_all().expect("scan start error");

  let mut c = 0;
  while let Ok(Some(_)) = iter.try_next() {
    c += 1;
  }
  println!("key count {c}");

  for i in 0..count {
    let bytes: Vec<u8> = i.to_le_bytes().into();
    t.table(table)
      .unwrap()
      .insert(bytes.clone(), bytes)
      .expect("insert error");
  }
  t.commit().expect("commit error");
}
