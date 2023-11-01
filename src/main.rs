use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use chrono::Utc;
use deadpool_lapin::{PoolConfig, Runtime};
use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions, QueueDeleteOptions},
    protocol::basic::AMQPProperties,
    types::{AMQPValue, FieldTable, ShortString},
};
use rand::{seq::SliceRandom, thread_rng, Rng};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Queue {
    queue: String,
}
pub struct AppState {
    pool: deadpool_lapin::Pool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let username = std::env::var("AMQP_USERNAME").unwrap_or("guest".into());
    let password = std::env::var("AMQP_PASSWORD").unwrap_or("guest".into());
    let host = std::env::var("AMQP_HOST").unwrap_or("localhost".into());
    let amqp_port = std::env::var("AMQP_PORT").unwrap_or("5672".into());
    let cfg = deadpool_lapin::Config {
        url: Some(format!(
            "amqp://{}:{}@{}:{}/%2f",
            username, password, host, amqp_port
        )),
        pool: Some(PoolConfig::new(10)),
        ..Default::default()
    };
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let app_state = Arc::new(AppState { pool: pool.clone() });

    init(pool).await?;
    // build our application with a single route
    let app = Router::new()
        .route("/publish", post(publish))
        .with_state(app_state);

    // run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
    Ok(())
}

async fn publish(app_state: State<Arc<AppState>>, Query(queue): Query<Queue>) -> impl IntoResponse {
    println!("Publishing to queue {}", queue.queue);
    let connection = app_state.pool.get().await.unwrap();
    let channel = connection.create_channel().await.unwrap();
    let queue_name = queue.queue;
    let data = lipsum::lipsum_words_with_rng(thread_rng(), 23);

    for _ in 0..10 {
        channel
            .basic_publish(
                "",
                queue_name.as_str(),
                BasicPublishOptions::default(),
                data.as_bytes(),
                AMQPProperties::default(),
            )
            .await
            .unwrap();
    }
    (StatusCode::CREATED, "Published")
}

async fn init(pool: deadpool_lapin::Pool) -> Result<()> {
    let queue_names = std::env::var("AMQP_QUEUE_NAMES")
        .to_owned()
        .unwrap_or("demo".to_string())
        .split(',')
        .map(|s| s.to_string())
        .enumerate()
        .map(|(i, name)| match i == 0 {
            true => (name, 4),
            false => (name, thread_rng().gen_range(1..2)),
        })
        .collect::<Vec<(String, u8)>>();

    let connection = pool.get().await?;

    let channel = connection.create_channel().await?;

    for queue_name in queue_names.clone() {
        channel
            .queue_delete(
                queue_name.0.as_str(),
                QueueDeleteOptions {
                    ..Default::default()
                },
            )
            .await?;
        let mut queue_args = FieldTable::default();
        queue_args.insert(
            ShortString::from("x-queue-type"),
            AMQPValue::LongString("stream".into()),
        );

        channel
            .queue_declare(
                queue_name.0.as_str(),
                QueueDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    ..Default::default()
                },
                queue_args,
            )
            .await?;
    }

    for _ in 0..100 {
        let data = lipsum::lipsum_words_with_rng(thread_rng(), 23);
        let data = data.as_bytes();
        let uuid = uuid::Uuid::new_v4();
        let timestamp = Utc::now().timestamp_millis() as u64;
        let transaction_id = format!("transaction_{}", uuid);
        let mut headers = FieldTable::default();
        headers.insert(
            ShortString::from("x-stream-transaction-id"),
            AMQPValue::LongString(transaction_id.clone().into()),
        );

        channel
            .basic_publish(
                "",
                queue_names
                    .choose_weighted(&mut thread_rng(), |item| item.1)
                    .expect("No queue name found")
                    .0
                    .as_str(),
                BasicPublishOptions::default(),
                data,
                AMQPProperties::default()
                    .with_headers(headers.clone())
                    .with_timestamp(timestamp),
            )
            .await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    Ok(())
}
