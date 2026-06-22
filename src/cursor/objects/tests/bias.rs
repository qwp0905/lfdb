use super::*;

#[test]
fn default_bias_is_middle_filled() {
  assert_eq!(SPLIT_BIAS_BYTES, SplitBias::BITS as usize >> 3);
  assert_eq!(count_directions(DEFAULT_BIAS), [0, CAP as usize, 0]);
}

#[test]
fn update_bias_records_insert_position_bucket() {
  let bias = update_bias(DEFAULT_BIAS, 9, 0);
  assert_eq!(count_directions(bias), [1, CAP as usize - 1, 0]);

  let bias = update_bias(DEFAULT_BIAS, 9, 4);
  assert_eq!(count_directions(bias), [0, CAP as usize, 0]);

  let bias = update_bias(DEFAULT_BIAS, 9, 8);
  assert_eq!(count_directions(bias), [0, CAP as usize - 1, 1]);
}

#[test]
fn update_bias_keeps_only_last_sixteen_directions() {
  let mut bias = DEFAULT_BIAS;
  assert_eq!(count_directions(bias), [0, CAP as usize, 0]);

  let left_count = CAP as usize / 2 + 1;
  let right_count = CAP as usize / 2 + 1;
  for _ in 0..left_count {
    bias = update_bias(bias, 3, 0);
  }
  assert_eq!(
    count_directions(bias),
    [left_count, CAP as usize - left_count, 0]
  );

  for _ in 0..right_count {
    bias = update_bias(bias, 3, 2);
  }
  assert_eq!(
    count_directions(bias),
    [CAP as usize - right_count, 0, right_count]
  );
}
