use crate::purchasing_record::Purchase;
use actix_web::{get, middleware::Logger, post, web::Json, App, HttpServer, Responder};
use apache_avro::{from_value, to_avro_datum, to_value, AvroSchema, Reader, Schema, Writer};
use env_logger::Env;
use kafka::producer::Producer;
use purchasing_record::PurchaseRequest;
mod purchasing_record;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Hello, world!");
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    HttpServer::new(move || {
        App::new()
            .service(hello_world)
            .service(purchase)
            .wrap(Logger::default())
    })
    .bind("0.0.0.0:9000")?
    .run()
    .await
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

    let mut producer = PurchasesProducer::new();
    producer.send_record(purchase);
    "got it bossman"
}

struct PurchasesProducer {
    kafka_producer: Producer,
}

impl PurchasesProducer {
    pub fn new() -> Self {
        let kafka_producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
            .with_ack_timeout(std::time::Duration::from_secs(1))
            .with_required_acks(kafka::producer::RequiredAcks::One)
            .create()
            .expect("Unable to make kafka producer");
        Self { kafka_producer }
    }
    pub fn send_record(&mut self, record: Purchase) {
        let avro_value = to_value(record).unwrap();
        let avro_binary = to_avro_datum(&Purchase::get_schema(), avro_value).unwrap();
        let kafka_record = kafka::producer::Record::from_value("TEST", avro_binary);

        self.kafka_producer.send(&kafka_record).unwrap();
    }
}
