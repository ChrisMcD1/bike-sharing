mod produces;
mod purchase_request;

use crate::produces::Purchase;
use actix_web::{get, middleware::Logger, post, web::Json, App, HttpServer, Responder};
use apache_avro::{to_avro_datum, to_value, AvroSchema};
use env_logger::Env;
use kafka::producer::Producer;
use purchase_request::PurchaseRequest;
use serde::{Deserialize, Serialize};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Hello, world!");
    let config: Configuration = confy::load("purchasing", None).unwrap();
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    HttpServer::new(move || {
        App::new()
            .service(hello_world)
            .service(purchase)
            .wrap(Logger::default())
    })
    .bind((config.purchasing_ip, config.purchasing_port))?
    .run()
    .await
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct Configuration {
    purchasing_ip: String,
    purchasing_port: u16,
    kafka_host: String,
}

#[get("/hello")]
pub async fn hello_world() -> impl Responder {
    "Hi"
}

#[post("/purchase")]
pub async fn purchase(record: Json<PurchaseRequest>) -> impl Responder {
    let bike_id = 1;
    let purchase = Purchase {
        cost: record.cost,
        bike_id,
    };

    let config: Configuration = confy::load("purchasing", None).unwrap();
    let mut producer = PurchasesProducer::new(&config);
    producer.send_record(purchase);
    "got it bossman"
}

struct PurchasesProducer {
    kafka_producer: Producer,
}

impl PurchasesProducer {
    pub fn new(config: &Configuration) -> Self {
        let kafka_producer = Producer::from_hosts(vec![config.kafka_host.to_owned()])
            .with_ack_timeout(std::time::Duration::from_secs(1))
            .with_required_acks(kafka::producer::RequiredAcks::One)
            .create()
            .expect("Unable to make kafka producer");
        Self { kafka_producer }
    }
    pub fn send_record(&mut self, record: Purchase) {
        let avro_value = to_value(record).unwrap();
        let avro_binary = to_avro_datum(&Purchase::get_schema(), avro_value).unwrap();
        let kafka_record = kafka::producer::Record::from_value("bike-purchases", avro_binary);

        self.kafka_producer.send(&kafka_record).unwrap();
    }
}
