use std::error::Error;
use std::env;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::path::Path;
use std::fs;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tokio::sync::mpsc;

use socket2::{Socket, TcpKeepalive};

use redis::aio::ConnectionManager;
use redis::AsyncCommands;

use serde_json::json;

use aws_sdk_sqs::Client;
use dotenv::from_filename;

use chrono::Utc;
use tracing::{error, warn, info, debug};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const MAX_REQUEST_SIZE: usize = 65536;
const BUFFER_SIZE: usize = 2048;
const IDLE_TIMEOUT: Duration = Duration::from_secs(600); // fecha conexão sem dados por 10 min
const REDIS_TIMEOUT: Duration = Duration::from_secs(3);
// Autenticação é revalidada periodicamente por conexão, em vez de um GET a cada payload
const AUTH_RECHECK_AUTHORIZED: Duration = Duration::from_secs(600);
const AUTH_RECHECK_DENIED:     Duration = Duration::from_secs(60);

fn apply_keepalive(stream: &TcpStream) {
    let fd = stream.as_raw_fd();
    // Safety: apenas configura opções no fd, sem transferir ownership
    let socket = unsafe { Socket::from_raw_fd(fd) };
    let ka = TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(10))
        .with_retries(3);
    if let Err(e) = socket.set_tcp_keepalive(&ka) {
        warn!(error = %e, "Failed to set TCP keepalive");
    }
    std::mem::forget(socket);
}

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

// Some(autorizado) quando o Redis responde; None em erro/timeout (quem chama mantém o estado anterior)
async fn check_device_auth(redis: &mut ConnectionManager, imei: &str) -> Option<bool> {
    match timeout(REDIS_TIMEOUT, redis.get::<_, Option<String>>(imei)).await {
        Ok(Ok(value)) => Some(value.is_some_and(|v| !v.is_empty())),
        Ok(Err(e)) => { warn!(imei = %imei, error = %e, "Redis auth check failed"); None }
        Err(_)     => { warn!(imei = %imei, "Redis auth check timed out"); None }
    }
}

struct AuthState {
    authorized: bool,
    checked_at: Option<Instant>,
}

impl AuthState {
    fn new() -> Self {
        Self { authorized: false, checked_at: None }
    }

    fn needs_check(&self) -> bool {
        let interval = if self.authorized { AUTH_RECHECK_AUTHORIZED } else { AUTH_RECHECK_DENIED };
        self.checked_at.map_or(true, |t| t.elapsed() >= interval)
    }

    async fn refresh(&mut self, redis: &mut ConnectionManager, imei: &str) {
        if let Some(authorized) = check_device_auth(redis, imei).await {
            if self.checked_at.is_none() || authorized != self.authorized {
                info!(imei = %imei, authorized = authorized, "Device authorization checked");
            }
            self.authorized = authorized;
            self.checked_at = Some(Instant::now());
        }
    }
}

// Estado inicial da ignição ao abrir a conexão; None em erro/timeout (tenta de novo no próximo payload)
async fn load_ignition_status(redis: &mut ConnectionManager, imei: &str) -> Option<String> {
    let key = format!("{}/ign_status", imei);
    match timeout(REDIS_TIMEOUT, redis.get::<_, Option<String>>(&key)).await {
        Ok(Ok(value)) => Some(value.unwrap_or_else(|| "0".to_string())),
        Ok(Err(e)) => { warn!(imei = %imei, error = %e, "Failed to load ign_status"); None }
        Err(_)     => { warn!(imei = %imei, "Load ign_status timed out"); None }
    }
}

fn ignition_event(payload: &str) -> Option<&'static str> {
    if payload.contains("GTIGN") {
        Some("1")
    } else if payload.contains("GTIGF") {
        Some("0")
    } else {
        None
    }
}

async fn save_ignition_status(redis: &mut ConnectionManager, imei: &str, value: &str) {
    let key = format!("{}/ign_status", imei);
    match timeout(REDIS_TIMEOUT, redis.set::<_, _, ()>(&key, value)).await {
        Ok(Ok(())) => info!(imei = %imei, ign_status = %value, "Ignition status updated"),
        Ok(Err(e)) => warn!(imei = %imei, error = %e, "Failed to update ign_status"),
        Err(_)     => warn!(imei = %imei, "Update ign_status timed out"),
    }
}

// Extrai lat/lon/speed do GTERI: parts[9]=speed, parts[12]=lon, parts[13]=lat (protocolo Queclink)
fn parse_gteri_lat_lon(payload: &str) -> Option<(f64, f64, f64)> {
    let dollar_idx = payload.find('$').unwrap_or(payload.len());
    let parts: Vec<&str> = payload[..dollar_idx].split(',').collect();
    if parts.len() < 14 {
        return None;
    }
    let speed = parts[9].trim().parse::<f64>().unwrap_or(0.0);
    let lon = parts[12].trim().parse::<f64>().ok()?;
    let lat = parts[13].trim().parse::<f64>().ok()?;
    Some((lat, lon, speed))
}

async fn save_last_transmission(
    redis:          &mut ConnectionManager,
    imei:           &str,
    payload:        &str,
    ignition_status: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if !payload.contains("GTERI") {
        return Ok(());
    }

    let (latitude, longitude, speed) = parse_gteri_lat_lon(payload).unwrap_or((0.0, 0.0, 0.0));

    let ign_int: i32 = ignition_status.parse().unwrap_or(0);

    let data = json!({
        "imei":              imei,
        "speed":             speed,
        "latitude":          latitude,
        "longitude":         longitude,
        "ignition_status":   ign_int,
        "last_transmission": Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    });

    let key = format!("{}/last_transmission", imei);
    redis.set::<_, _, ()>(&key, data.to_string()).await?;
    Ok(())
}

// Envia ao dispositivo o comando mais antigo da fila e registra o ACK.
// O backend enfileira com LPUSH, então o mais antigo fica no fim: lê com LINDEX -1 e só remove
// (RPOP) depois de enviado. Se o envio falhar ou der timeout, o comando continua na fila.
async fn flush_pending_updates(socket: &mut TcpStream, redis: &mut ConnectionManager, imei: &str) {
    let update_key = format!("{}/update", imei);

    let content: String = match redis.lindex::<_, Option<String>>(&update_key, -1).await {
        Ok(Some(content)) => content,
        Ok(None) => {
            debug!(imei = %imei, "No pending updates");
            return;
        }
        Err(e) => {
            error!(imei = %imei, error = %e, "Failed to read pending updates");
            return;
        }
    };

    info!(imei = %imei, command = %content, "Sending command to device");
    let packet = content.as_bytes().to_vec();
    if let Err(e) = socket.write_all(&packet).await {
        error!(imei = %imei, error = %e, "Failed to write update to device");
        return;
    }
    let _: Result<Option<String>, _> = redis.rpop(&update_key, None).await;
    let verify_key = format!("{}/verify_ack", imei);
    let _: Result<(), _> = redis.set::<_, _, ()>(&verify_key, "1").await;
    info!(imei = %imei, command = %content, "Command sent and verify_ack set");
}

fn extract_imei(payload: &str) -> Option<String> {
    let dollar_idx = payload.find('$').unwrap_or(payload.len());
    let parts: Vec<&str> = payload[..dollar_idx].split(',').collect();
    let imei = parts.get(2).map(|s| s.trim().to_string()).filter(|s| s.len() == 15 && s.chars().all(|c| c.is_ascii_digit()));
    imei
}

async fn handle_connection(
    mut socket: TcpStream,
    addr:       SocketAddr,
    mut redis:  ConnectionManager,
    sqs_tx:     mpsc::UnboundedSender<String>,
    stats:      Arc<Stats>,
) {
    stats.active.fetch_add(1, Ordering::Relaxed);
    let _guard = ActiveGuard(stats.clone());

    info!(remote_addr = %addr, "New connection accepted");

    let mut buffer     = [0u8; BUFFER_SIZE];
    let mut request_buf: Vec<u8> = Vec::with_capacity(4096);
    let mut imei: Option<String> = None;
    let mut auth = AuthState::new();
    // Só o TCP server grava {imei}/ign_status: lido do Redis uma vez por conexão e mantido em memória
    let mut ign_cache: Option<String> = None;

    // Queclink GV350 envia mensagens AT diretamente — sem handshake de IMEI.
    // O IMEI é extraído de parts[2] da primeira mensagem válida.
    loop {
        let n = match timeout(IDLE_TIMEOUT, socket.read(&mut buffer)).await {
            Ok(Ok(0)) => { debug!(remote_addr = %addr, "Connection closed by device"); break; }
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                if e.raw_os_error() == Some(110) {
                    debug!(remote_addr = %addr, "Keepalive timeout — peer unreachable, closing");
                } else {
                    error!(remote_addr = %addr, error = %e, "Read error");
                }
                break;
            }
            Err(_) => { warn!(remote_addr = %addr, "Idle timeout — closing stale connection"); break; }
        };

        if request_buf.len() + n > MAX_REQUEST_SIZE {
            let preview = std::str::from_utf8(&request_buf)
                .unwrap_or("<binary>")
                .chars()
                .take(200)
                .collect::<String>();
            error!(remote_addr = %addr, buffer_len = request_buf.len(), preview = %preview, "Request buffer overflow — clearing and continuing");
            request_buf.clear();
            continue;
        }
        request_buf.extend_from_slice(&buffer[..n]);

        // Processa todas as mensagens completas no buffer (cada uma termina em '$')
        while let Some(end_pos) = request_buf.iter().position(|&b| b == b'$') {
            let message_bytes = request_buf[..=end_pos].to_vec();
            request_buf.drain(..=end_pos);

            let payload_str = match std::str::from_utf8(&message_bytes) {
                Ok(s) => s.trim_end_matches('\0').trim().to_string(),
                Err(_) => {
                    warn!(remote_addr = %addr, bytes = message_bytes.len(), "Binary data received — discarding message");
                    continue;
                }
            };

            if payload_str.is_empty() {
                continue;
            }

            if payload_str.contains("+ACK") {
                debug!(remote_addr = %addr, "ACK message ignored");
                continue;
            }

            if !payload_str.contains("+RESP") && !payload_str.contains("+BUF") {
                debug!(remote_addr = %addr, payload = %payload_str, "Unknown message format, skipping");
                continue;
            }

            // Extrai o IMEI da primeira mensagem válida e mantém para as demais
            if imei.is_none() {
                match extract_imei(&payload_str) {
                    Some(extracted) => {
                        info!(remote_addr = %addr, imei = %extracted, "IMEI identified from first message");
                        imei = Some(extracted);
                    }
                    None => {
                        warn!(remote_addr = %addr, payload = %payload_str, "Could not extract IMEI, skipping");
                        continue;
                    }
                }
            }

            let imei = imei.as_deref().unwrap();

            // Guard rail: autenticação revalidada periodicamente (10 min autorizado / 1 min negado)
            if auth.needs_check() {
                auth.refresh(&mut redis, imei).await;
            }
            if !auth.authorized {
                warn!(imei = %imei, "Device not authorized");
                stats.errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            info!(imei = %imei, payload = %payload_str, "Payload received");

            // Ignição: GTIGN/GTIGF definem o estado; nas demais usa o valor da conexão
            // (lido do Redis só na primeira mensagem)
            let ign_event = ignition_event(&payload_str);
            if let Some(value) = ign_event {
                ign_cache = Some(value.to_string());
            } else if ign_cache.is_none() {
                ign_cache = load_ignition_status(&mut redis, imei).await;
            }
            let ign_status = ign_cache.clone().unwrap_or_else(|| "0".to_string());

            let to_send = json!({
                "ignition": ign_status,
                "payload":  payload_str,
            })
            .to_string();

            if sqs_tx.send(to_send).is_ok() {
                stats.processed.fetch_add(1, Ordering::Relaxed);
            } else {
                error!(imei = %imei, "Failed to queue SQS message");
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }

            // Escritas no Redis só depois do SQS: Redis lento não atrasa a telemetria
            if let Some(value) = ign_event {
                save_ignition_status(&mut redis, imei, value).await;
            }

            // Salva last_transmission no Redis (apenas para a mensagem de posição do modelo)
            match timeout(REDIS_TIMEOUT, save_last_transmission(&mut redis, imei, &payload_str, &ign_status)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(imei = %imei, error = %e, "Failed to save last transmission"),
                Err(_)     => warn!(imei = %imei, "Save last transmission timed out"),
            }

            if timeout(REDIS_TIMEOUT, flush_pending_updates(&mut socket, &mut redis, imei)).await.is_err() {
                warn!(imei = %imei, "flush_pending_updates timed out");
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let log_dir = env::var("LOG_DIR").unwrap_or_else(|_| "./logs".to_string());
    fs::create_dir_all(&log_dir)?;

    let file_appender = tracing_appender::rolling::daily(&log_dir, "gv350.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(tracing_subscriber::fmt::layer().with_target(true).with_thread_ids(true))
        .with(tracing_subscriber::fmt::layer().with_target(true).with_thread_ids(true).with_writer(non_blocking))
        .init();

    // Task de limpeza: remove arquivos de log com mais de 7 dias a cada 24h
    {
        let log_dir_clone = log_dir.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(86400));
            interval.tick().await;
            loop {
                interval.tick().await;
                let cutoff = std::time::SystemTime::now()
                    .checked_sub(Duration::from_secs(7 * 86400))
                    .unwrap();
                if let Ok(entries) = fs::read_dir(&log_dir_clone) {
                    for entry in entries.flatten() {
                        if let Ok(meta) = entry.metadata() {
                            if let Ok(modified) = meta.modified() {
                                if modified < cutoff {
                                    let _ = fs::remove_file(entry.path());
                                    info!(file = ?entry.path(), "Old log file removed");
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    info!("Starting GV350 TCP Server");

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
    let address  = "0.0.0.0:50002";
    let listener = TcpListener::bind(address).await?;
    info!(address = address, "TCP listener bound");

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                apply_keepalive(&socket);
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
