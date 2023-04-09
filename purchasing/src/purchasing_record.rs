use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct Purchase {
    pub bike_id: u32,
    pub cost: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PurchaseRequest {
    pub cost: f64,
}
