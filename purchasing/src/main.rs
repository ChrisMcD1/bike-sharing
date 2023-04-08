use crate::purchasing_record::Purchase;
use actix_web::{get, middleware::Logger, post, web::Json, App, HttpServer, Responder};
use apache_avro::{from_value, AvroSchema, Reader, Writer};
use env_logger::Env;
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
pub async fn purchase(record: Json<Purchase>) -> impl Responder {
    println!("We received {:?}", record);
    "got it bossman"
}
