use std::error::Error;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::path::Path;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use redis::aio::ConnectionManager;
use redis::AsyncCommands;

use serde_json::Value;
use serde_json::json;

use aws_sdk_sqs::Client;
use dotenv::from_filename;

use chrono::Utc;
use tracing::{error, warn, info, debug};

const MAX_REQUEST_SIZE: usize = 65536;
const BUFFER_SIZE: usize = 2048;

struct Stats {
    processed: AtomicU64,
    errors:    AtomicU64,
    active:    AtomicU64,
}

// Decrementa o contador de conexões ativas em qualquer caminho de retorno da task
struct ActiveGuard(Arc<Stats>);
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn worker_sqs(mut rx: mpsc::UnboundedReceiver<String>) {
    info!("Starting SQS worker");

    from_filename(Path::new(".env")).ok();

    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    let account_id = match env::var("AWS_ACCOUNT_ID") {
        Ok(id) => id,
        Err(e) => { error!("AWS_ACCOUNT_ID not defined: {}", e); return; }
    };
    let queue_name = match env::var("AWS_SQS_QUEUE_NAME") {
        Ok(name) => name,
        Err(e) => { error!("AWS_SQS_QUEUE_NAME not defined: {}", e); return; }
    };

    let queue_url = format!("https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}");
    info!(
        region     = %region,
        account_id = %account_id,
        queue_name = %queue_name,
        queue_url  = %queue_url,
        "SQS configuration validated"
    );

    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);

    while let Some(message) = rx.recv().await {
        let start   = Instant::now();
        let payload = message.trim_end_matches('\0').to_string();

        match client
            .send_message()
            .queue_url(&queue_url)
            .message_body(payload)
            .send()
            .await
        {
            Ok(resp) => info!(
                message_id = ?resp.message_id(),
                duration_us = start.elapsed().as_micros(),
                "Message sent to SQS"
            ),
            Err(e) => error!(error = %e, "Failed to send message to SQS"),
        }
    }
}

async fn get_device_auth(redis: &mut ConnectionManager, imei: &str) -> String {
    redis.get::<_, String>(imei).await.unwrap_or_default()
}

async fn save_last_transmission(
    redis: &mut ConnectionManager,
    imei:  &str,
    json:  &Value,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let reported = &json["state"]["reported"];

    let (latitude, longitude) = reported
        .get("latlng")
        .and_then(|v| v.as_str())
        .and_then(|s| s.split_once(','))
        .map(|(lat, lng)| (lat.parse::<f64>().unwrap_or(0.0), lng.parse::<f64>().unwrap_or(0.0)))
        .unwrap_or((0.0, 0.0));

    let ignition_status = reported.get("239").unwrap_or(&Value::from(0)).clone();

    let data = json!({
        "imei":              imei,
        "latitude":          latitude,
        "longitude":         longitude,
        "ignition_status":   ignition_status,
        "last_transmission": Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    });

    let key = format!("{}/last_transmission", imei);
    redis.set::<_, _, ()>(&key, data.to_string()).await?;
    Ok(())
}

// Envia ao dispositivo qualquer comando pendente na fila Redis e registra o ACK
async fn flush_pending_updates(socket: &mut TcpStream, redis: &mut ConnectionManager, imei: &str) {
    let update_key = format!("{}/update", imei);

    let values: Result<Vec<String>, _> = redis.lrange(&update_key, 0, 100).await;
    match values {
        Ok(list) if !list.is_empty() => {
            if let Some(content) = list.first() {
                if let Err(e) = socket.write_all(content.as_bytes()).await {
                    error!(imei = %imei, error = %e, "Failed to write update to device");
                    return;
                }
                let _: Result<Option<String>, _> = redis::cmd("RPOP")
                    .arg(&update_key)
                    .query_async(redis)
                    .await;
                let verify_key = format!("{}/verify_ack", imei);
                let _: Result<(), _> = redis.set::<_, _, ()>(&verify_key, "1").await;
            }
        }
        Ok(_)  => debug!(imei = %imei, "No pending updates"),
        Err(e) => error!(imei = %imei, error = %e, "Failed to read pending updates"),
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    addr:       SocketAddr,
    mut redis:  ConnectionManager,
    sqs_tx:     mpsc::UnboundedSender<String>,
    stats:      Arc<Stats>,
) {
    stats.active.fetch_add(1, Ordering::Relaxed);
    let _guard = ActiveGuard(stats.clone()); // garante decremento em qualquer return

    info!(remote_addr = %addr, "New connection accepted");

    let mut buffer = [0u8; BUFFER_SIZE];

    // --- Fase 1: leitura do IMEI ---
    let n = match socket.read(&mut buffer).await {
        Ok(0) | Err(_) => return,
        Ok(n) => n,
    };

    let raw            = std::str::from_utf8(&buffer[..n]).unwrap_or("");
    let alphanumeric   = raw.chars().filter(|c| c.is_alphanumeric()).count();

    if alphanumeric != 15 {
        debug!(remote_addr = %addr, "Invalid IMEI length, dropping connection");
        return;
    }

    let imei: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    info!(imei = %imei, "IMEI registered");

    // --- Fase 2: ACK ---
    if let Err(e) = socket.write_all(&[0x01]).await {
        error!(imei = %imei, error = %e, "Failed to send ACK");
        return;
    }
    debug!(imei = %imei, "ACK sent");

    // --- Fase 3: loop de payloads ---
    let mut request_buf: Vec<u8> = Vec::with_capacity(4096);

    loop {
        let n = match socket.read(&mut buffer).await {
            Ok(0) => { debug!(imei = %imei, "Connection closed by device"); break; }
            Ok(n) => n,
            Err(e) => { error!(imei = %imei, error = %e, "Read error"); break; }
        };

        if request_buf.len() + n > MAX_REQUEST_SIZE {
            error!(imei = %imei, "Request buffer overflow");
            break;
        }
        request_buf.extend_from_slice(&buffer[..n]);

        let payload_str = match std::str::from_utf8(&request_buf) {
            Ok(s) => s.trim_end_matches('\0').trim().to_string(),
            Err(_) => continue,
        };

        if payload_str.is_empty() || !payload_str.starts_with('{') {
            request_buf.clear();
            continue;
        }

        match serde_json::from_str::<Value>(&payload_str) {
            Ok(mut json) => {
                request_buf.clear();

                if let Some(reported) = json["state"]["reported"].as_object_mut() {
                    reported.insert("imei".to_string(), Value::String(imei.clone()));
                }

                // Guard rail: autenticação re-verificada a cada payload
                let device_id = get_device_auth(&mut redis, &imei).await;
                if device_id.is_empty() {
                    warn!(imei = %imei, "Device not authorized");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                info!(imei = %imei, payload = %json, "Payload Received");

                let to_send = serde_json::to_string(&json).unwrap_or_default();
                if !to_send.is_empty() {
                    if sqs_tx.send(to_send).is_ok() {
                        stats.processed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        error!(imei = %imei, "Failed to queue SQS message");
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }

                if let Err(e) = save_last_transmission(&mut redis, &imei, &json).await {
                    warn!(imei = %imei, error = %e, "Failed to save last transmission");
                }

                flush_pending_updates(&mut socket, &mut redis, &imei).await;
            }
            Err(_) => {} // JSON incompleto: continua acumulando
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("Starting FMC920 TCP Server");

    from_filename(Path::new(".env")).ok();

    // Redis async — ConnectionManager é clonável e multiplexa sobre uma única conexão
    let redis_url    = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/".to_string());
    let redis_client = redis::Client::open(redis_url.clone())?;
    let redis        = ConnectionManager::new(redis_client).await?;
    info!(redis_url = ?redis_url, "Redis async connection manager created");

    // Canal SQS async + task worker
    let (sqs_tx, sqs_rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(worker_sqs(sqs_rx));

    // Contadores compartilhados entre tasks
    let stats = Arc::new(Stats {
        processed: AtomicU64::new(0),
        errors:    AtomicU64::new(0),
        active:    AtomicU64::new(0),
    });

    // Task de log periódico de estatísticas (a cada 10 minutos)
    {
        let stats = stats.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(600));
            interval.tick().await; // descarta o primeiro tick imediato
            loop {
                interval.tick().await;
                info!(
                    processed   = stats.processed.load(Ordering::Relaxed),
                    errors      = stats.errors.load(Ordering::Relaxed),
                    connections = stats.active.load(Ordering::Relaxed),
                    "Statistics update"
                );
            }
        });
    }

    // TCP listener
    let address  = "0.0.0.0:50005";
    let listener = TcpListener::bind(address).await?;
    info!(address = address, "TCP listener bound");

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                tokio::spawn(handle_connection(
                    socket,
                    addr,
                    redis.clone(),
                    sqs_tx.clone(),
                    stats.clone(),
                ));
            }
            Err(e) => error!(error = %e, "Error accepting connection"),
        }
    }
}
