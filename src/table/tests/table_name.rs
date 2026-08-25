use super::*;
use crate::Error;

#[test]
fn rejects_names_with_a_disallowed_character_and_names_it_in_the_error() {
  assert!(matches!(
    TableName::from_str("bad.name"),
    Err(Error::NotAllowedChar(_))
  ));
}

#[test]
fn accepts_alphanumeric_dash_and_underscore() {
  assert!(TableName::from_str("valid-table_Name123").is_ok());
}
