use lfdb::EngineBuilder;

struct DebugLogger;
impl log::Log for DebugLogger {
  fn enabled(&self, _: &log::Metadata) -> bool {
    true
  }

  fn log(&self, record: &log::Record) {
    println!("[{}] {}", record.level(), record.args())
  }

  fn flush(&self) {}
}
fn main() {
  let _ = log::set_logger(&DebugLogger);
  log::set_max_level(log::LevelFilter::Trace);
  let engine = EngineBuilder::new("./.local")
    .block_cache_memory_capacity(100 << 20)
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
