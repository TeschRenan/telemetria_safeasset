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

const FRAME_START: u8 = b'~';
const FRAME_END:   u8 = b'$';

// --- Protocolo J-R07 ---
// Formato: ~MASCARA;campo1;campo2;...;$
// A máscara (hex, 64 bits) indica quais IDs estão presentes: bit N ativo => ID N+1 presente.
// Ex.: "0000000007FFFFFF" (bits 0..26) => IDs 1..27, na ordem crescente de ID.
// O payload segue raw para o lambda; aqui só extraímos o necessário para auth e Redis.

const ID_IMEI:      u32 = 2;
const ID_LATITUDE:  u32 = 6;
const ID_LONGITUDE: u32 = 7;
const ID_SPEED:     u32 = 10;
const ID_IGNITION:  u32 = 15;
const ID_IO_STATE:  u32 = 28; // bit 0 = entrada digital 1 (ignição)

struct Jr07Message {
    imei:      Option<String>,
    latitude:  f64,
    longitude: f64,
    speed:     f64,
    ignition:  i64,
}

fn parse_jr07(message: &str) -> Result<Jr07Message, String> {
    let body = message
        .trim()
        .trim_start_matches('~')
        .trim_end_matches('$');

    let mut parts: Vec<&str> = body.split(';').map(|s| s.trim()).collect();
    // A string termina com ";$", o que gera um último campo vazio
    if parts.last() == Some(&"") {
        parts.pop();
    }

    let (mask_str, values) = parts.split_first().ok_or("Empty message")?;
    let mask = u64::from_str_radix(mask_str, 16)
        .map_err(|e| format!("Invalid mask '{}': {}", mask_str, e))?;

    let expected = mask.count_ones() as usize;
    if values.len() != expected {
        return Err(format!(
            "Field count mismatch: mask {} expects {} values, got {}",
            mask_str, expected, values.len()
        ));
    }

    // Posição do valor de um ID = quantidade de bits ativos abaixo dele
    let get = |id: u32| -> Option<&str> {
        let n = id - 1;
        if (mask >> n) & 1 == 0 {
            return None;
        }
        let idx = (mask & ((1u64 << n) - 1)).count_ones() as usize;
        values.get(idx).copied()
    };
    let num = |id: u32| get(id).and_then(|v| v.parse::<f64>().ok());

    // Ignição: campo 15; na ausência, bit 0 do Estado I/O
    let ignition = get(ID_IGNITION)
        .and_then(|v| v.parse::<i64>().ok())
        .or_else(|| get(ID_IO_STATE).and_then(|v| v.parse::<i64>().ok()).map(|io| io & 1))
        .unwrap_or(0);

    Ok(Jr07Message {
        imei:      get(ID_IMEI).filter(|s| is_valid_imei(s)).map(|s| s.to_string()),
        latitude:  num(ID_LATITUDE).unwrap_or(0.0),
        longitude: num(ID_LONGITUDE).unwrap_or(0.0),
        speed:     num(ID_SPEED).unwrap_or(0.0),
        ignition,
    })
}

fn is_valid_imei(imei: &str) -> bool {
    imei.len() == 15 && imei.chars().all(|c| c.is_ascii_digit())
}

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

async fn get_device_auth(redis: &mut ConnectionManager, imei: &str) -> String {
    match timeout(REDIS_TIMEOUT, redis.get::<_, String>(imei)).await {
        Ok(result) => result.unwrap_or_default(),
        Err(_) => {
            warn!(imei = %imei, "Redis auth check timed out");
            String::new()
        }
    }
}

async fn save_last_transmission(
    redis: &mut ConnectionManager,
    imei:  &str,
    msg:   &Jr07Message,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let data = json!({
        "imei":              imei,
        "speed":             msg.speed,
        "latitude":          msg.latitude,
        "longitude":         msg.longitude,
        "ignition_status":   msg.ignition,
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

// Remove do buffer a próxima mensagem completa "~...$", descartando lixo anterior ao '~'
fn next_frame(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    let start = match buf.iter().position(|&b| b == FRAME_START) {
        Some(pos) => pos,
        None => {
            buf.clear();
            return None;
        }
    };
    if start > 0 {
        buf.drain(..start);
    }
    let end = buf.iter().position(|&b| b == FRAME_END)?;
    let frame = buf[..=end].to_vec();
    buf.drain(..=end);
    Some(frame)
}

async fn handle_connection(
    mut socket:   TcpStream,
    addr:         SocketAddr,
    redis_client: Arc<redis::Client>,
    sqs_tx:       mpsc::UnboundedSender<String>,
    stats:        Arc<Stats>,
) {
    stats.active.fetch_add(1, Ordering::Relaxed);
    let _guard = ActiveGuard(stats.clone());

    info!(remote_addr = %addr, "New connection accepted");

    // Conexão Redis dedicada para este handler — sem contenção com outros dispositivos
    let mut redis = match timeout(REDIS_TIMEOUT, ConnectionManager::new((*redis_client).clone())).await {
        Ok(Ok(cm)) => cm,
        Ok(Err(e)) => {
            error!(remote_addr = %addr, error = %e, "Failed to create Redis connection");
            return;
        }
        Err(_) => {
            error!(remote_addr = %addr, "Redis connection timed out");
            return;
        }
    };

    let mut buffer      = [0u8; BUFFER_SIZE];
    let mut request_buf: Vec<u8> = Vec::with_capacity(4096);

    // J-R07 não faz handshake de IMEI — ele é extraído do campo 2 da primeira mensagem válida
    let mut imei: Option<String> = None;

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
            let preview = String::from_utf8_lossy(&request_buf)
                .chars()
                .take(200)
                .collect::<String>();
            error!(remote_addr = %addr, buffer_len = request_buf.len(), preview = %preview, "Request buffer overflow — clearing and continuing");
            request_buf.clear();
            continue;
        }
        request_buf.extend_from_slice(&buffer[..n]);

        // Processa todas as mensagens completas no buffer
        while let Some(frame) = next_frame(&mut request_buf) {
            let payload_str = match std::str::from_utf8(&frame) {
                Ok(s) => s.trim_end_matches('\0').trim().to_string(),
                Err(_) => {
                    warn!(remote_addr = %addr, bytes = frame.len(), "Binary data received — discarding message");
                    continue;
                }
            };

            let msg = match parse_jr07(&payload_str) {
                Ok(m) => m,
                Err(e) => {
                    warn!(remote_addr = %addr, error = %e, payload = %payload_str, "Failed to parse J-R07 message");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let current = match (&imei, msg.imei.clone()) {
                // Primeira mensagem: registra IMEI e autentica uma única vez por conexão
                (None, Some(extracted)) => {
                    info!(remote_addr = %addr, imei = %extracted, "IMEI registered");
                    let device_id = get_device_auth(&mut redis, &extracted).await;
                    if device_id.is_empty() {
                        warn!(imei = %extracted, "Device not authorized, closing connection");
                        return;
                    }
                    imei = Some(extracted.clone());
                    extracted
                }
                (None, None) => {
                    warn!(remote_addr = %addr, payload = %payload_str, "Could not extract IMEI, skipping");
                    continue;
                }
                (Some(known), Some(extracted)) if *known != extracted => {
                    warn!(imei = %known, received = %extracted, "IMEI changed within connection, skipping");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                (Some(known), _) => known.clone(),
            };

            info!(imei = %current, payload = %payload_str, "Payload Received");

            // O lambda recebe a string raw "~...$" exatamente como enviada pelo dispositivo
            if sqs_tx.send(payload_str.clone()).is_ok() {
                stats.processed.fetch_add(1, Ordering::Relaxed);
            } else {
                error!(imei = %current, "Failed to queue SQS message");
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }

            match tokio::time::timeout(
                REDIS_TIMEOUT,
                save_last_transmission(&mut redis, &current, &msg),
            ).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(imei = %current, error = %e, "Redis save_last_transmission failed"),
                Err(_)     => warn!(imei = %current, "Redis save_last_transmission timed out"),
            }

            match tokio::time::timeout(
                REDIS_TIMEOUT,
                flush_pending_updates(&mut socket, &mut redis, &current),
            ).await {
                Ok(()) => {}
                Err(_) => warn!(imei = %current, "flush_pending_updates timed out"),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let log_dir = env::var("LOG_DIR").unwrap_or_else(|_| "./logs".to_string());
    fs::create_dir_all(&log_dir)?;

    let file_appender = tracing_appender::rolling::daily(&log_dir, "jr07.log");
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

    info!("Starting J-R07 TCP Server");

    from_filename(Path::new(".env")).ok();

    // Redis — cada handler cria sua própria conexão dedicada (sem contenção)
    let redis_url    = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/".to_string());
    let redis_client = Arc::new(redis::Client::open(redis_url.clone())?);
    info!(redis_url = ?redis_url, "Redis client created");

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
    let address  = "0.0.0.0:50006";
    let listener = TcpListener::bind(address).await?;
    info!(address = address, "TCP listener bound");

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                apply_keepalive(&socket);
                tokio::spawn(handle_connection(
                    socket,
                    addr,
                    redis_client.clone(),
                    sqs_tx.clone(),
                    stats.clone(),
                ));
            }
            Err(e) => error!(error = %e, "Error accepting connection"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Exemplo da seção 6 do manual (com espaços e quebras de linha como no PDF)
    const MANUAL_EXAMPLE: &str = "~ 0000000007FFFFFF ;202401-00033-0010466;123456789012345;1234;teste1;1705687209759;
-19.87417;-43.96894;840.45;3;60.99;12;31;1.21;270.25;1;0;12345.67;67543.21;118;1;
2;13300;4200;TIM;4;1705687209759;1; $";

    #[test]
    fn parses_manual_example() {
        let m = parse_jr07(MANUAL_EXAMPLE).unwrap();
        assert_eq!(m.imei.as_deref(), Some("123456789012345"));
        assert_eq!(m.latitude, -19.87417);
        assert_eq!(m.longitude, -43.96894);
        assert_eq!(m.speed, 60.99);
        assert_eq!(m.ignition, 1);
    }

    #[test]
    fn mask_skips_absent_ids() {
        // IDs 2, 6, 7 e 28 (sem velocidade e sem ignição): ignição vem do bit 0 do Estado I/O
        let mask: u64 = [2, 6, 7, 28].iter().map(|id| 1u64 << (id - 1)).sum();
        let msg = format!("~{:016X};123456789012345;-1.5;-2.5;5;$", mask);
        let m = parse_jr07(&msg).unwrap();
        assert_eq!(m.imei.as_deref(), Some("123456789012345"));
        assert_eq!(m.latitude, -1.5);
        assert_eq!(m.longitude, -2.5);
        assert_eq!(m.speed, 0.0);
        assert_eq!(m.ignition, 1);
    }

    #[test]
    fn rejects_field_count_mismatch() {
        assert!(parse_jr07("~0000000000000007;a;b;$").is_err());
        assert!(parse_jr07("~ZZZ;a;$").is_err());
    }

    #[test]
    fn frames_split_across_reads() {
        let mut buf = b"lixo~0000000000000002;123456789012345;$~00000000000".to_vec();
        let f1 = next_frame(&mut buf).unwrap();
        assert_eq!(f1, b"~0000000000000002;123456789012345;$");
        assert!(next_frame(&mut buf).is_none());
        buf.extend_from_slice(b"00002;123456789012345;$");
        assert!(next_frame(&mut buf).is_some());
        assert!(buf.is_empty());
    }
}
