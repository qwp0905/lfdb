use loom::thread;

use super::*;
use crate::background::create_task;

fn model(f: impl Fn() + Sync + Send + 'static) {
  let mut builder = loom::model::Builder::new();
  builder.preemption_bound = Some(2);
  builder.check(f);
}

#[test]
fn every_fulfilled_result_is_selected() {
  model(|| {
    let (tx1, rx1) = create_task::<usize>();
    let (tx2, rx2) = create_task::<usize>();
    let mut select = Select::new();
    select.push(rx1);
    select.push(rx2);

    let s1 = thread::spawn(move || tx1.fulfill(1));
    let s2 = thread::spawn(move || tx2.fulfill(2));

    let mut results = [select.select().unwrap(), select.select().unwrap()];
    results.sort();
    assert_eq!(results, [1, 2]);
    assert!(select.is_empty());

    s1.join().unwrap();
    s2.join().unwrap();
  });
}

#[test]
fn disconnect_and_value_are_both_selected() {
  model(|| {
    let (tx1, rx1) = create_task::<usize>();
    let (tx2, rx2) = create_task::<usize>();
    let mut select = Select::new();
    select.push(rx1);
    select.push(rx2);

    let s1 = thread::spawn(move || tx1.fulfill(1));
    let s2 = thread::spawn(move || drop(tx2));

    let results = [select.select(), select.select()];
    assert_eq!(results.iter().filter(|r| matches!(r, Ok(1))).count(), 1);
    assert_eq!(results.iter().filter(|r| r.is_err()).count(), 1);

    s1.join().unwrap();
    s2.join().unwrap();
  });
}
