use lfdb::{EngineBuilder, LogLevel, Logger};

struct DebugLogger;
impl Logger for DebugLogger {
  fn log(&self, level: LogLevel, msg: &[u8]) {
    println!("[{}] {}", level.to_str(), String::from_utf8_lossy(msg))
  }
}
fn main() {
  let engine = EngineBuilder::new("./.local")
    .block_cache_memory_capacity(100 << 20)
    .logger(DebugLogger)
    .log_level(LogLevel::Trace)
    .build()
    .expect("bootstrap error");

  let mut t = engine.new_tx().unwrap();
  t.open_table("test").unwrap();
  t.commit().unwrap();

  let mut w = engine.new_tx().expect("write tx error");
  w.table("test")
    .unwrap()
    .insert(b"123".to_vec(), b"456".to_vec())
    .expect("insert error");
  w.commit().expect("write commit error");

  let mut r = engine.new_tx().expect("read tx error");
  println!(
    "{:?}",
    r.table("test")
      .unwrap()
      .get(&b"123".to_vec())
      .expect("find error")
  );
  r.commit().expect("read commit error");
}
