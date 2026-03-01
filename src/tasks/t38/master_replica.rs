use std::time::Duration;
use std::{collections::HashMap, process::exit};

use log::{error, info};
use redis::{AsyncConnectionConfig, Client, aio::MultiplexedConnection};
use tokio::{sync::oneshot, task::JoinHandle};

use super::config::{IpAddress, T38Config, read_t38_config_from_file, write_t38_config_to_file};
use crate::{
    config::CONFIG,
    constants::{T38RoleName, T38StateName},
    db::t38::{T38Node, follow, follow_no_one, get_role, t38_client},
};

const TIMEOUT_CONNECTION: u64 = 5000; // milliseconds

// loading the big AOF file into memory may take a long time
const COUNT_ATTEMPTS_RECOVER: u64 = 600; // equivalent to 600 seconds

// main storage config
const T38CONFIG_JSON_FILE: &str = "t38_config.json";
// auxiliary storage configuration for various services
const T38CONFIG_SERVICE_JSON_FILE: &str = "t38_config_service.json";

const ERROR_SEND_CONNECTION: &str = "send tile38 connection";

pub enum T38StorageType {
    Main,
    Service,
}

pub enum T38ConnectionManageMessage {
    GetConnection {
        tx: oneshot::Sender<Option<MultiplexedConnection>>,
        error: Option<String>,
    },
    GetConnectionService {
        tx: oneshot::Sender<Option<MultiplexedConnection>>,
        error: Option<String>,
    },
    RecoverFailedNode {
        recovered_node: T38Node,
    },
    RecoverFailedNodeService {
        recovered_node: T38Node,
    },
}

async fn t38_conf(file: &str) -> Result<Option<T38Config>, anyhow::Error> {
    let mut t38_config: Option<T38Config> = None;
    if tokio::fs::try_exists(file).await? {
        let config = read_t38_config_from_file(file).await?;
        t38_config = Some(config);
    }
    Ok(t38_config)
}

pub async fn manage_master_replica(
    rx: flume::Receiver<T38ConnectionManageMessage>,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
) -> Result<JoinHandle<()>, anyhow::Error> {
    let t38_conn_config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(5)))
        .set_response_timeout(Some(Duration::from_secs(5)));

    let t38_config = t38_conf(T38CONFIG_JSON_FILE).await?;
    let (t38_nodes, mut t38_master_connections) =
        connection_identification(t38_config, &t38_conn_config, T38StorageType::Main).await;
    if t38_master_connections.is_none() {
        error!("master instance Tile38 is not running or not defined");
        exit(1);
    }

    let t38_config_service = t38_conf(T38CONFIG_SERVICE_JSON_FILE).await?;
    let (t38_nodes_service, mut t38_master_connections_service) = connection_identification(
        t38_config_service,
        &t38_conn_config,
        T38StorageType::Service,
    )
    .await;
    if t38_master_connections_service.is_none() {
        error!("master instance Tile38 for service is not running or not defined");
        exit(1);
    }

    let tx_t38_conn_clone = tx_t38_conn.clone();
    let jh = tokio::spawn(async move {
        let mut t38_nodes = t38_nodes.clone();
        let mut t38_nodes_service = t38_nodes_service.clone();
        let tx_t38_conn = tx_t38_conn_clone.clone();

        let mut index_conn = 0;
        let mut index_conn_srv = 0;

        while let Ok(message) = rx.recv_async().await {
            match message {
                T38ConnectionManageMessage::GetConnection { tx, error } => {
                    // An error occurred, changing the master
                    // TODO: check type error
                    if let Some(_e) = error {
                        let (nodes, mn) =
                            get_node_by_role(t38_nodes, T38RoleName::Master.as_ref()).await;
                        t38_nodes = nodes;

                        if let Some(mut master_node) = mn {
                            if master_node
                                .client
                                .get_connection_with_timeout(Duration::from_millis(
                                    TIMEOUT_CONNECTION,
                                ))
                                .is_err()
                            {
                                let (nodes, sn) =
                                    get_node_by_role(t38_nodes, T38RoleName::Slave.as_ref()).await;
                                t38_nodes = nodes;

                                if let Some(mut slave_node) = sn {
                                    if let Ok(slave_connection) = slave_node
                                        .client
                                        .get_multiplexed_async_connection_with_config(
                                            &t38_conn_config,
                                        )
                                        .await
                                    {
                                        if let Err(e) =
                                            follow_no_one(slave_connection.clone()).await
                                        {
                                            error!("Tile38 FOLLOW no one: {}", e);
                                            if tx.send(None).is_err() {
                                                error!("{}", ERROR_SEND_CONNECTION);
                                            }
                                        } else {
                                            tokio::time::sleep(Duration::from_millis(1000)).await;

                                            if tx.send(Some(slave_connection.clone())).is_err() {
                                                error!("send new tile38 connection");
                                            }
                                            slave_node.role =
                                                Some(T38RoleName::Master.as_ref().to_string());
                                            t38_nodes.insert(
                                                format!("{}:{}", slave_node.host, slave_node.port),
                                                slave_node.clone(),
                                            );

                                            master_node.role =
                                                Some(T38RoleName::Slave.as_ref().to_string());
                                            t38_nodes.insert(
                                                format!(
                                                    "{}:{}",
                                                    master_node.host, master_node.port
                                                ),
                                                master_node.clone(),
                                            );

                                            t38_master_connections = connection_pool(
                                                slave_connection,
                                                &slave_node.client,
                                                &t38_conn_config,
                                            )
                                            .await;

                                            let t38c = T38Config {
                                                master: IpAddress {
                                                    host: slave_node.host.clone(),
                                                    port: slave_node.port,
                                                },
                                                slaves: vec![IpAddress {
                                                    host: master_node.host.clone(),
                                                    port: master_node.port,
                                                }],
                                            };
                                            if let Err(e) =
                                                write_t38_config_to_file(t38c, T38CONFIG_JSON_FILE)
                                                    .await
                                            {
                                                error!("save Tile38 json config: {}", e);
                                            }

                                            // waiting for the failed node to recover
                                            let _jh = recover_failed_node(
                                                master_node,
                                                slave_node,
                                                tx_t38_conn.clone(),
                                            );
                                        }
                                    } else {
                                        if tx.send(None).is_err() {
                                            error!("{}", ERROR_SEND_CONNECTION);
                                        }
                                    }
                                } else {
                                    if tx.send(None).is_err() {
                                        error!("{}", ERROR_SEND_CONNECTION);
                                    }
                                }
                            } else {
                                // successful connection to the master
                                if let Ok(master_connection) = master_node
                                    .client
                                    .get_multiplexed_async_connection_with_config(&t38_conn_config)
                                    .await
                                {
                                    if tx.send(Some(master_connection.clone())).is_err() {
                                        error!("{}", ERROR_SEND_CONNECTION);
                                    }

                                    t38_master_connections = connection_pool(
                                        master_connection,
                                        &master_node.client,
                                        &t38_conn_config,
                                    )
                                    .await;
                                }
                            }
                        } else {
                            if tx.send(None).is_err() {
                                error!("{}", ERROR_SEND_CONNECTION);
                            }
                        }
                    } else {
                        if let Some(connections) = t38_master_connections.as_ref() {
                            if index_conn > connections.len() - 1 {
                                index_conn = 0
                            }
                            if let Some(c) = connections.get(index_conn) {
                                if let Err(_) = tx.send(Some(c.clone())) {
                                    error!("{}", ERROR_SEND_CONNECTION);
                                }
                            }
                            index_conn += 1;
                        }
                    }
                }
                T38ConnectionManageMessage::GetConnectionService { tx, error } => {
                    // An error occurred, changing the master
                    // TODO: check type error
                    if let Some(_e) = error {
                        let (nodes, mn) =
                            get_node_by_role(t38_nodes_service, T38RoleName::Master.as_ref()).await;
                        t38_nodes_service = nodes;

                        if let Some(mut master_node) = mn {
                            if master_node
                                .client
                                .get_connection_with_timeout(Duration::from_millis(
                                    TIMEOUT_CONNECTION,
                                ))
                                .is_err()
                            {
                                let (nodes, sn) = get_node_by_role(
                                    t38_nodes_service,
                                    T38RoleName::Slave.as_ref(),
                                )
                                .await;
                                t38_nodes_service = nodes;

                                if let Some(mut slave_node) = sn {
                                    if let Ok(slave_connection) = slave_node
                                        .client
                                        .get_multiplexed_async_connection_with_config(
                                            &t38_conn_config,
                                        )
                                        .await
                                    {
                                        if let Err(e) =
                                            follow_no_one(slave_connection.clone()).await
                                        {
                                            error!("Tile38 FOLLOW no one: {}", e);
                                            if tx.send(None).is_err() {
                                                error!("{}", ERROR_SEND_CONNECTION);
                                            }
                                        } else {
                                            tokio::time::sleep(Duration::from_millis(1000)).await;

                                            if tx.send(Some(slave_connection.clone())).is_err() {
                                                error!("send new tile38 connection");
                                            }
                                            slave_node.role =
                                                Some(T38RoleName::Master.as_ref().to_string());
                                            t38_nodes_service.insert(
                                                format!("{}:{}", slave_node.host, slave_node.port),
                                                slave_node.clone(),
                                            );

                                            master_node.role =
                                                Some(T38RoleName::Slave.as_ref().to_string());
                                            t38_nodes_service.insert(
                                                format!(
                                                    "{}:{}",
                                                    master_node.host, master_node.port
                                                ),
                                                master_node.clone(),
                                            );

                                            t38_master_connections_service = connection_pool(
                                                slave_connection,
                                                &slave_node.client,
                                                &t38_conn_config,
                                            )
                                            .await;

                                            let t38c = T38Config {
                                                master: IpAddress {
                                                    host: slave_node.host.clone(),
                                                    port: slave_node.port,
                                                },
                                                slaves: vec![IpAddress {
                                                    host: master_node.host.clone(),
                                                    port: master_node.port,
                                                }],
                                            };
                                            if let Err(e) = write_t38_config_to_file(
                                                t38c,
                                                T38CONFIG_SERVICE_JSON_FILE,
                                            )
                                            .await
                                            {
                                                error!("save Tile38 service json config: {}", e);
                                            }

                                            // waiting for the failed node to recover
                                            let _jh = recover_failed_node_service(
                                                master_node,
                                                slave_node,
                                                tx_t38_conn.clone(),
                                            );
                                        }
                                    } else {
                                        if tx.send(None).is_err() {
                                            error!("{}", ERROR_SEND_CONNECTION);
                                        }
                                    }
                                } else {
                                    if tx.send(None).is_err() {
                                        error!("{}", ERROR_SEND_CONNECTION);
                                    }
                                }
                            } else {
                                // successful connection to the master
                                if let Ok(master_connection) = master_node
                                    .client
                                    .get_multiplexed_async_connection_with_config(&t38_conn_config)
                                    .await
                                {
                                    if tx.send(Some(master_connection.clone())).is_err() {
                                        error!("{}", ERROR_SEND_CONNECTION);
                                    }

                                    t38_master_connections_service = connection_pool(
                                        master_connection,
                                        &master_node.client,
                                        &t38_conn_config,
                                    )
                                    .await;
                                }
                            }
                        } else {
                            if tx.send(None).is_err() {
                                error!("{}", ERROR_SEND_CONNECTION);
                            }
                        }
                    } else {
                        if let Some(connections) = t38_master_connections_service.as_ref() {
                            if index_conn_srv > connections.len() - 1 {
                                index_conn_srv = 0
                            }
                            if let Some(c) = connections.get(index_conn_srv) {
                                if let Err(_) = tx.send(Some(c.clone())) {
                                    error!("{}", ERROR_SEND_CONNECTION);
                                }
                            }
                            index_conn_srv += 1;
                        }
                    }
                }
                T38ConnectionManageMessage::RecoverFailedNode { recovered_node } => {
                    let node_key = format!("{}:{}", recovered_node.host, recovered_node.port);
                    t38_nodes.insert(node_key.clone(), recovered_node);
                    info!("failed node '{}' is recover", node_key);
                }
                T38ConnectionManageMessage::RecoverFailedNodeService { recovered_node } => {
                    let node_key = format!("{}:{}", recovered_node.host, recovered_node.port);
                    t38_nodes_service.insert(node_key.clone(), recovered_node);
                    info!("failed service node '{}' is recover", node_key);
                }
            }
        }
    });

    Ok(jh)
}

async fn connection_identification(
    t38_config: Option<T38Config>,
    t38_conn_config: &AsyncConnectionConfig,
    t38_type: T38StorageType,
) -> (HashMap<String, T38Node>, Option<Vec<MultiplexedConnection>>) {
    let mut t38_nodes = HashMap::new();
    let mut t38_master_connections = None;

    let t38_instances = match t38_type {
        T38StorageType::Main => CONFIG.t38.instances.as_ref(),
        T38StorageType::Service => CONFIG.t38.service.as_ref(),
    };

    if let Some(instances) = t38_instances {
        for instance in instances {
            let client = t38_client(&instance.host, instance.port).unwrap();
            if let Ok(connection) = client
                .get_multiplexed_async_connection_with_config(&t38_conn_config)
                .await
            {
                if let Ok(Some(r)) = get_role(connection.clone()).await {
                    // Tile38 config loaded from json file
                    if let Some(t38c) = t38_config.as_ref() {
                        if r.contains(T38RoleName::Master.as_ref()) {
                            let mut actual_role = T38RoleName::Master.as_ref().to_string();

                            if instance.host != t38c.master.host
                                || instance.port != t38c.master.port
                            {
                                // current instance == t38c.slaves[0]
                                // the data from the automatic configuration file and the instance not match
                                // configuration file takes precedence
                                {
                                    let slave_client =
                                        t38_client(&t38c.master.host, t38c.master.port).unwrap();
                                    if let Ok(slave_connection) = slave_client
                                        .get_multiplexed_async_connection_with_config(
                                            &t38_conn_config,
                                        )
                                        .await
                                    {
                                        // unsubscribe from a slave
                                        if let Err(e) = follow_no_one(slave_connection).await {
                                            error!("Tile38 FOLLOW no one error: {}", e);
                                        }
                                    }
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }

                                if let Err(e) =
                                    follow(connection.clone(), &t38c.master.host, t38c.master.port)
                                        .await
                                {
                                    error!(
                                        "follow instance '{}:{}' to master '{}:{}': {}",
                                        instance.host,
                                        instance.port,
                                        t38c.master.host,
                                        t38c.master.port,
                                        e,
                                    );
                                }
                                actual_role = T38RoleName::Slave.as_ref().to_string();
                            } else {
                                t38_master_connections =
                                    connection_pool(connection, &client, &t38_conn_config).await;
                            }

                            t38_nodes.insert(
                                format!("{}:{}", instance.host, instance.port),
                                T38Node {
                                    host: instance.host.clone(),
                                    port: instance.port,
                                    role: Some(actual_role),
                                    state: Some(T38StateName::Active.as_ref().to_string()),
                                    client,
                                },
                            );
                        } else if r.contains(T38RoleName::Slave.as_ref()) {
                            let mut actual_role = T38RoleName::Slave.as_ref().to_string();

                            if instance.host != t38c.slaves[0].host
                                || instance.port != t38c.slaves[0].port
                            {
                                actual_role = T38RoleName::Master.as_ref().to_string();

                                t38_master_connections =
                                    connection_pool(connection, &client, &t38_conn_config).await;
                            }

                            t38_nodes.insert(
                                format!("{}:{}", instance.host, instance.port),
                                T38Node {
                                    host: instance.host.clone(),
                                    port: instance.port,
                                    role: Some(actual_role),
                                    state: Some(T38StateName::Active.as_ref().to_string()),
                                    client,
                                },
                            );
                        }
                    } else {
                        if r.contains(T38RoleName::Master.as_ref()) {
                            t38_master_connections =
                                connection_pool(connection, &client, &t38_conn_config).await;
                        } else {
                            drop(connection);
                        }
                        t38_nodes.insert(
                            format!("{}:{}", instance.host, instance.port),
                            T38Node {
                                host: instance.host.clone(),
                                port: instance.port,
                                role: Some(r),
                                state: Some(T38StateName::Active.as_ref().to_string()),
                                client,
                            },
                        );
                    }
                }
            } else {
                // can't determine the role of the tile38 instance.
                t38_nodes.insert(
                    format!("{}:{}", instance.host, instance.port),
                    T38Node {
                        host: instance.host.clone(),
                        port: instance.port,
                        role: None,
                        state: Some(T38StateName::Inactive.as_ref().to_string()),
                        client,
                    },
                );
            }
        }
    }
    (t38_nodes, t38_master_connections)
}

// role == "master" or "slave"
async fn get_node_by_role(
    mut nodes: HashMap<String, T38Node>,
    role_name: &str,
) -> (HashMap<String, T38Node>, Option<T38Node>) {
    for (key, mut node) in nodes.clone() {
        if let Some(r) = node.role.as_ref() {
            if r.contains(role_name) {
                return (nodes, Some(node));
            }
        } else {
            // try connect to Tile38 instance and clarify the role
            if let Ok(connection) = node.client.get_multiplexed_async_connection().await {
                if let Ok(role) = get_role(connection).await {
                    if let Some(r) = role {
                        node.role = Some(r.clone());
                        nodes.insert(key, node.clone());
                        if r.contains(role_name) {
                            return (nodes, Some(node));
                        }
                    }
                }
            }
        }
    }
    (nodes, None)
}

fn recover_failed_node(
    mut failed_node: T38Node,
    new_master_node: T38Node,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
) -> JoinHandle<()> {
    tokio::spawn({
        async move {
            // waiting indefinitely for the tile38 instance to be restored
            while failed_node
                .client
                .get_connection_with_timeout(Duration::from_millis(TIMEOUT_CONNECTION))
                .is_err()
            {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // while cycle has completed, connection has been restored
            if let Ok(c) = failed_node.client.get_multiplexed_async_connection().await {
                let mut i = 0;
                while let Err(e) =
                    follow(c.clone(), &new_master_node.host, new_master_node.port).await
                {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if i > COUNT_ATTEMPTS_RECOVER {
                        /*
                            error follow failed node '127.0.0.1:9852' to new master node '127.0.0.1:9851':
                            BusyLoading: Tile38 is loading the dataset in memory

                            redis: e.category() == "busy loading"
                        */
                        error!(
                            "error follow failed node '{}:{}' to new master node '{}:{}': {}",
                            failed_node.host,
                            failed_node.port,
                            new_master_node.host,
                            new_master_node.port,
                            e,
                        );
                        break;
                    }
                    i += 1;
                }
                failed_node.role = Some(T38RoleName::Slave.as_ref().to_string());
                failed_node.state = Some(T38StateName::Active.as_ref().to_string());

                if let Err(e) = tx_t38_conn
                    .send_async(T38ConnectionManageMessage::RecoverFailedNode {
                        recovered_node: failed_node,
                    })
                    .await
                {
                    error!("send recovered Tile38 node: {}", e);
                }
            }
        }
    })
}

fn recover_failed_node_service(
    mut failed_node: T38Node,
    new_master_node: T38Node,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
) -> JoinHandle<()> {
    tokio::spawn({
        async move {
            // waiting indefinitely for the tile38 instance to be restored
            while failed_node
                .client
                .get_connection_with_timeout(Duration::from_millis(TIMEOUT_CONNECTION))
                .is_err()
            {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // while cycle has completed, connection has been restored
            if let Ok(c) = failed_node.client.get_multiplexed_async_connection().await {
                let mut i = 0;
                while let Err(e) =
                    follow(c.clone(), &new_master_node.host, new_master_node.port).await
                {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if i > COUNT_ATTEMPTS_RECOVER {
                        /*
                            error follow failed node '127.0.0.1:9852' to new master node '127.0.0.1:9851':
                            BusyLoading: Tile38 is loading the dataset in memory

                            redis: e.category() == "busy loading"
                        */
                        error!(
                            "error follow failed node '{}:{}' to new master node '{}:{}': {}",
                            failed_node.host,
                            failed_node.port,
                            new_master_node.host,
                            new_master_node.port,
                            e,
                        );
                        break;
                    }
                    i += 1;
                }
                failed_node.role = Some(T38RoleName::Slave.as_ref().to_string());
                failed_node.state = Some(T38StateName::Active.as_ref().to_string());

                if let Err(e) = tx_t38_conn
                    .send_async(T38ConnectionManageMessage::RecoverFailedNodeService {
                        recovered_node: failed_node,
                    })
                    .await
                {
                    error!("send recovered Tile38 node: {}", e);
                }
            }
        }
    })
}

pub async fn connection_pool(
    connection: MultiplexedConnection,
    client: &Client,
    t38_conn_config: &AsyncConnectionConfig,
) -> Option<Vec<MultiplexedConnection>> {
    if CONFIG.t38.pool_size == 1 {
        Some(vec![connection])
    } else {
        let mut connections = Vec::new();
        connections.push(connection);
        let n = CONFIG.t38.pool_size - 1;
        for _ in 0..n {
            if let Ok(c) = client
                .get_multiplexed_async_connection_with_config(&t38_conn_config)
                .await
            {
                connections.push(c);
            }
        }
        Some(connections)
    }
}
