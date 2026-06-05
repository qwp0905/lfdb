use uuid::Uuid;

pub fn uuid_simple() -> String {
  Uuid::new_v4().as_simple().to_string()
}
