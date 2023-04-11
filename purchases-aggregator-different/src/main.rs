use std::{io::Cursor, time::Duration, vec};

use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value, AvroSchema, Schema};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    let mut producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Producer creation failed");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "123")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&vec!["bike-purchases"]).unwrap();

    let purchase_schema = Purchase::get_schema();

    let mut bike_count = 0;
    let mut total_cost = 0f64;

    let aggregate_schema = PurchaseAggregate::get_schema();

    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error :{}", e),
            Ok(message) => {
                let payload = message.payload().unwrap();
                let mut message_clone = Cursor::new(payload);
                let purchase = from_value::<Purchase>(
                    &from_avro_datum(&purchase_schema, &mut message_clone, None).unwrap(),
                )
                .unwrap();

                bike_count += 1;
                total_cost += purchase.cost;
                let aggregate = PurchaseAggregate {
                    bike_count,
                    total_cost,
                };
                send_to_producer(&mut producer, &aggregate_schema, &aggregate).await;
                println!("{:?}", aggregate);
            }
        }
    }
}

async fn send_to_producer(
    producer: &mut FutureProducer,
    schema: &Schema,
    aggregate: &PurchaseAggregate,
) {
    let avro_value = to_value(aggregate).unwrap();
    let avro_binary = to_avro_datum(schema, avro_value).unwrap();

    let delivery_status = producer
        .send(
            FutureRecord::to("bike-purchases-aggregate-different")
                .key(&())
                .payload(&avro_binary),
            Duration::from_secs(0),
        )
        .await
        .unwrap();
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct Purchase {
    pub bike_id: u32,
    pub cost: f64,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct PurchaseAggregate {
    pub bike_count: u32,
    pub total_cost: f64,
}
