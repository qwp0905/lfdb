use super::*;

#[test]
fn default_bias_is_middle_filled() {
  assert_eq!(SPLIT_BIAS_BYTES, 4);
  assert_eq!(count_directions(DEFAULT_BIAS), [0, 16, 0]);
}

#[test]
fn update_bias_records_insert_position_bucket() {
  let bias = update_bias(DEFAULT_BIAS, 9, 0);
  assert_eq!(count_directions(bias), [1, 15, 0]);

  let bias = update_bias(DEFAULT_BIAS, 9, 4);
  assert_eq!(count_directions(bias), [0, 16, 0]);

  let bias = update_bias(DEFAULT_BIAS, 9, 8);
  assert_eq!(count_directions(bias), [0, 15, 1]);
}

#[test]
fn update_bias_keeps_only_last_sixteen_directions() {
  let mut bias = DEFAULT_BIAS;

  for _ in 0..16 {
    bias = update_bias(bias, 3, 0);
  }
  assert_eq!(count_directions(bias), [16, 0, 0]);

  for _ in 0..8 {
    bias = update_bias(bias, 3, 2);
  }
  assert_eq!(count_directions(bias), [8, 0, 8]);
}
