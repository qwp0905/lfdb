use super::*;

const PAGE_SIZE: usize = 4 << 10;

#[test]
fn test_return_and_reuse() {
  let pool = PagePool::<PAGE_SIZE>::new(10);
  assert_eq!(pool.len(), 0);

  let page = pool.acquire();
  assert_eq!(page.as_slice().len(), PAGE_SIZE);

  drop(page);
  assert_eq!(pool.len(), 1);

  let page = pool.acquire();
  assert_eq!(page.as_slice().len(), PAGE_SIZE);
  assert_eq!(pool.len(), 0);

  drop(page);
  assert_eq!(pool.len(), 1);
}

#[test]
fn test_drop() {
  let cap = 3;
  let pool = PagePool::<PAGE_SIZE>::new(cap);

  let mut pages = Vec::new();
  for _ in 0..cap {
    pages.push(pool.acquire());
  }
  pages.drain(..).for_each(drop);
  assert_eq!(pool.len(), 3);

  for _ in 0..=cap {
    pages.push(pool.acquire());
  }

  for i in 0..cap {
    drop(pages.pop().unwrap());
    assert_eq!(pool.len(), i + 1);
  }

  drop(pages);
  assert_eq!(pool.len(), 3)
}
