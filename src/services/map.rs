use tokio_postgres::GenericClient;

/// Create a geometry from the database h3 tiles
pub async fn run(pool_tp: deadpool_postgres::Pool) -> Result<(), anyhow::Error> {
    let mapper = pool_tp.get().await?;
    let client = mapper.client();
    crate::db::get_map(client).await
}
