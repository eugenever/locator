use log::error;
use tokio::task::JoinHandle;
use tokio_schedule::Job;

use crate::services::routing;

// periodically run the io.dropwizard.servlets.tasks.GarbageCollectionTask
pub fn gc_task(gc_frequency: u32, gh_client: reqwest::Client) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio_schedule::every(gc_frequency)
            .seconds()
            .perform(|| async {
                if let Err(err) = routing::gc::gh_gc_request(gh_client.clone()).await {
                    error!("request GC GraphHopper: {}", err);
                }
            })
            .await;
    })
}
