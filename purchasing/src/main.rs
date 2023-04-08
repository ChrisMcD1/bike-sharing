use avro_rs::Schema;
use std::fs;

fn main() {
    println!("Hello, world!");
    let avro_spec = fs::read_to_string("./src/avro-spec.json").unwrap();

    let schema = Schema::parse_str(&avro_spec).unwrap();

    println!("{:?}", schema);
}
