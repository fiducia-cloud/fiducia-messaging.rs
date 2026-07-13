use fiducia_messaging::transactional::OutboxPublisher;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be configured")?;
    let nats_url = std::env::var("NATS_URL").map_err(|_| "NATS_URL must be configured")?;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    sqlx::migrate!().run(&pool).await?;
    let nats = async_nats::connect(nats_url).await?;
    eprintln!("fiducia compatibility outbox publisher started");
    OutboxPublisher::new(pool, nats)
        .run(Duration::from_millis(250))
        .await?;
    Ok(())
}
