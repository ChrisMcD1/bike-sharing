use std::{io::Cursor, time::Duration, vec};

use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value, AvroSchema, Schema};
use kafka::{consumer::Consumer, producer::Producer};
use serde::{Deserialize, Serialize};

fn main() {
    println!("Hello, world!");

    let mut kafka_consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("bike-purchases".to_owned())
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .with_fetch_max_wait_time(Duration::from_secs(1))
        .with_fetch_min_bytes(1000)
        .with_fetch_max_bytes_per_partition(100_000)
        .with_retry_max_bytes_limit(1_000_000)
        .create()
        .expect("Unable to make kafka consumer");

    let mut kafka_producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_ack_timeout(std::time::Duration::from_secs(1))
        .with_required_acks(kafka::producer::RequiredAcks::One)
        .create()
        .expect("Unable to make kafka producer");

    let purchase_schema = Purchase::get_schema();

    let mut bike_count = 0;
    let mut total_cost = 0f64;
    let mut message_set_count = 0;

    let aggregate_schema = PurchaseAggregate::get_schema();

    loop {
        for message_set in kafka_consumer.poll().unwrap().iter() {
            for message in message_set.messages() {
                let mut message_clone = Cursor::new(message.value);
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
                send_to_producer(&mut kafka_producer, &aggregate_schema, &aggregate);
                println!("{:?}", aggregate);
            }
            message_set_count += 1;
            println!("Finished Message set {message_set_count}");
        }
    }
}

fn send_to_producer(producer: &mut Producer, schema: &Schema, aggregate: &PurchaseAggregate) {
    let avro_value = to_value(aggregate).unwrap();
    let avro_binary = to_avro_datum(schema, avro_value).unwrap();
    let kafka_record = kafka::producer::Record::from_value("bike-purchases-aggregate", avro_binary);

    producer.send(&kafka_record).unwrap();
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
