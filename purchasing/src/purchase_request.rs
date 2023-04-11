use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PurchaseRequest {
    pub cost: f64,
}
