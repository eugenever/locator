use crate::CONFIG;

// curl -X POST "http://localhost:8990/tasks/gc"
// run io.dropwizard.servlets.tasks.GarbageCollectionTask
pub async fn gh_gc_request(gh_client: reqwest::Client) -> Result<(), anyhow::Error> {
    let gh_url = format!(
        "http://{}:{}/tasks/gc",
        CONFIG.graphhopper.admin.host, CONFIG.graphhopper.admin.port,
    );
    let gh_response_status = gh_client.post(gh_url).send().await?.status();
    let status_code = gh_response_status.as_u16();
    if status_code != 200 {
        return Err(anyhow::anyhow!("status code {}", status_code));
    }
    Ok(())
}
