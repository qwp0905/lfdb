use super::*;

#[test]
fn rejects_names_with_a_disallowed_character_and_names_it_in_the_error() {
  let err = TableName::from_str("bad.name").unwrap_err();

  assert_eq!(
    err.to_string(),
    "character '.' is not allowed in table names (only alphanumeric, '-', and '_' are permitted)"
  );
}

#[test]
fn accepts_alphanumeric_dash_and_underscore() {
  assert!(TableName::from_str("valid-table_Name123").is_ok());
}
