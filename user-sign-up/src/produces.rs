use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};

pub const USER_TOPIC: &str = "Users";

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct User {
    pub id: u32,
    pub username: String,
    pub first_name: String,
    pub last_name: String,
    pub password: String,
}
