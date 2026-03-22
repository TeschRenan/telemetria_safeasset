// Benchmarks:
// $ ab -n 1000000 -c 128 -k http://127.0.0.1:8080/
// $ wrk -d 30s -t 4 -c 128 http://127.0.0.1:8080/

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::mpsc::{self};

use redis;
use redis::Commands;

use std::time::Instant;
use std::error::Error;

use mio::event::Source;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use serde_json::Value;
use serde_json::json;

use aws_sdk_sqs::Client;
use dotenv::from_filename;
use std::path::Path;
use std::env;

use chrono::Utc;
use tracing::{error, warn, info, debug, instrument};

const MAX_REQUEST_SIZE: usize = 65536; // 64KB max per request
const BUFFER_SIZE: usize = 2048;

fn is_json_end(window: &[u8]) -> bool {
    !window.is_empty() && window[0] == b'}'
}

fn start_worker_sqs(rx: std::sync::mpsc::Receiver<String>) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        rt.block_on(worker_sqs(rx));
    });
}

#[instrument(skip(rx))]
async fn worker_sqs(rx: std::sync::mpsc::Receiver<String>) {
    info!("Starting SQS worker thread");

    from_filename(Path::new(".env")).ok();

    // Validate SQS Configuration
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let account_id = match env::var("AWS_ACCOUNT_ID") {
        Ok(id) => id,
        Err(e) => {
            error!("AWS_ACCOUNT_ID not defined: {}", e);
            return;
        }
    };
    
    let queue_name = match env::var("AWS_SQS_QUEUE_NAME") {
        Ok(name) => name,
        Err(e) => {
            error!("AWS_SQS_QUEUE_NAME not defined: {}", e);
            return;
        }
    };

    let queue_url = format!(
        "https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
    );

    info!(
        region = %region,
        account_id = %account_id,
        queue_name = %queue_name,
        queue_url = %queue_url,
        "SQS configuration validated"
    );

    let config = aws_config::load_from_env().await;
    let aws_client = Client::new(&config);

    for message in rx {
        let start = Instant::now();

        let payload = message.trim_end_matches('\0').to_string();
        debug!("Processing SQS message");

        match aws_client
            .send_message()
            .queue_url(&queue_url)
            .message_body(payload)
            .send()
            .await
        {
            Ok(resp) => {
                let duration = start.elapsed().as_micros();
                info!(
                    message_id = ?resp.message_id(),
                    duration_us = duration,
                    "Message sent to SQS"
                );
            }
            Err(err) => {
                error!(error = %err, "Failed to send message to SQS");
            }
        }
    }
}

#[instrument(skip(redis_conn))]
fn get_device_auth(redis_conn: &mut redis::Connection, imei: &str) -> Result<String, Box<dyn Error>> {
    redis_conn.get(imei)
        .map_err(|e| {
            error!(imei = imei, error = %e, "Failed to get device from Redis");
            Box::new(e) as Box<dyn Error>
        })
}

#[instrument(skip(redis_conn, json))]
fn save_last_transmission(
    redis_conn: &mut redis::Connection,
    imei: &str,
    json: &Value,
) -> Result<(), Box<dyn Error>> {
    let latitude = json.get("latitude").unwrap_or(&Value::from(0.0)).clone();
    let longitude = json.get("longitude").unwrap_or(&Value::from(0.0)).clone();
    let ignition_status = json.get("239").unwrap_or(&Value::from(0)).clone();

    let json_data = json!({
        "imei": Value::String(imei.to_string()),
        "latitude": latitude,
        "longitude": longitude,
        "ignition_status": ignition_status,
        "last_transmission": Value::String(Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string())
    });

    let key = format!("{}/last_transmission", imei);
    
    redis_conn.set(&key, json_data.to_string())
        .map_err(|e| {
            error!(imei = imei, key = &key, error = %e, "Failed to save last_transmission");
            Box::new(e) as Box<dyn Error>
        })
}

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("Starting FMC150 TCP Server");

    from_filename(Path::new(".env")).ok();

    let (tx_sqs, rx_sqs) = mpsc::channel::<String>();

    start_worker_sqs(rx_sqs);

    let address = "0.0.0.0:50005";
    let mut listener = TcpListener::bind(address.parse()?)?;
    info!(address = address, "TCP listener bound");

    let mut poll = Poll::new()?;

    poll.registry()
        .register(&mut listener, Token(0), Interest::READABLE)?;

    let mut counter: usize = 0;
    let mut sockets: HashMap<Token, TcpStream> = HashMap::new();
    let mut requests: HashMap<Token, Vec<u8>> = HashMap::new();
    let mut devices: HashMap<Token, String> = HashMap::new();
    let mut buffer = [0_u8; BUFFER_SIZE];

    // Load Redis connection from .env
    let redis_url = env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://localhost:6379/".to_string());
    
    let redis_client = redis::Client::open(redis_url.clone())?;
    let mut redis_conn = redis_client.get_connection()?;
    
    info!(redis_url = ?redis_url, "Connected to Redis");

    let mut events = Events::with_capacity(4096);
    let mut stats = (0u64, 0u64); // (processed, errors)

    loop {
        poll.poll(&mut events, None)?;
        
        for event in &events {
            match event.token() {
                Token(0) => loop {
                    match listener.accept() {
                        Ok((mut socket, addr)) => {
                            counter += 1;
                            let token = Token(counter);
                            
                            if let Err(e) = socket.register(poll.registry(), token, Interest::READABLE) {
                                error!(token = counter, error = %e, "Failed to register socket");
                                continue;
                            }

                            info!(token = counter, remote_addr = %addr, "New connection accepted");

                            sockets.insert(token, socket);
                            requests.insert(token, Vec::with_capacity(4096)); // Smaller default
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(e) => {
                            warn!(error = %e, "Error accepting connection");
                            break;
                        }
                    }
                },
                
                token if event.is_readable() => {
                    loop {
                        let read = sockets.get_mut(&token)
                            .ok_or("Socket not found")?
                            .read(&mut buffer);

                        match read {
                            Ok(0) => {
                                debug!(token = ?token, "Connection closed");
                                sockets.remove(&token);
                                devices.remove(&token);
                                requests.remove(&token);
                                break;
                            }
                            Ok(n) => {
                                let request_str = std::str::from_utf8(&buffer[..n])
                                    .unwrap_or("");

                                let valid_chars = request_str.chars()
                                    .filter(|c| c.is_alphanumeric())
                                    .count();
                                
                                debug!(token = ?token, valid_chars = valid_chars, "Received data");

                                // IMEI Detection Phase (15 digits)
                                if valid_chars == 15 {
                                    if devices.get(&token).is_none() {
                                        let cleaned_imei: String = request_str.chars()
                                            .filter(|c| c.is_ascii_digit())
                                            .collect();
                                        
                                        devices.insert(token, cleaned_imei.clone());
                                        debug!(token = ?token, imei = cleaned_imei, "IMEI registered");
                                    }
                                } else {
                                    // Telemetry Data Phase
                                    if let Some(device_str) = devices.get(&token) {
                                        debug!(imei = device_str, "Processing telemetry");

                                        // Authentication & Processing
                                        match get_device_auth(&mut redis_conn, device_str) {
                                            Ok(device_id) if !device_id.is_empty() => {
                                                let payload = request_str.trim_end_matches('\0');

                                                match serde_json::from_str::<Value>(payload) {
                                                    Ok(mut json) => {
                                                        if let Some(reported) = json["state"]["reported"].as_object_mut() {
                                                            reported.insert("imei".to_string(), Value::String(device_str.to_string()));
                                                        }

                                                        // Send to SQS (non-blocking)
                                                        let to_send = serde_json::to_string(&json)
                                                            .unwrap_or_else(|e| {
                                                                error!(error = %e, "Failed to serialize JSON");
                                                                String::new()
                                                            });

                                                        if !to_send.is_empty() {
                                                            if let Err(e) = tx_sqs.send(to_send) {
                                                                error!(error = %e, "Failed to queue SQS message");
                                                                stats.1 += 1;
                                                            } else {
                                                                stats.0 += 1;
                                                            }
                                                        }

                                                        // Save last transmission
                                                        if let Err(e) = save_last_transmission(&mut redis_conn, device_str, &json) {
                                                            warn!(error = %e, "Failed to save transmission timestamp");
                                                        }
                                                    }
                                                    Err(err) => {
                                                        error!(error = %err, imei = device_str, "Invalid JSON payload");
                                                        stats.1 += 1;
                                                    }
                                                }
                                            }
                                            Ok(_) => {
                                                warn!(imei = device_str, "Device not authenticated");
                                                stats.1 += 1;
                                            }
                                            Err(e) => {
                                                error!(imei = device_str, error = %e, "Auth lookup failed");
                                                stats.1 += 1;
                                            }
                                        }
                                    }
                                }

                                // Add to request buffer with size limit
                                if let Some(req) = requests.get_mut(&token) {
                                    if req.len() + n > MAX_REQUEST_SIZE {
                                        error!(token = ?token, "Request buffer overflow");
                                        requests.remove(&token);
                                    } else {
                                        req.extend_from_slice(&buffer[..n]);
                                    }
                                }
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                error!(token = ?token, error = %e, "Read error");
                                sockets.remove(&token);
                                devices.remove(&token);
                                break;
                            }
                        }
                    }

                    // Check if JSON is complete
                    if let Some(requests_vec) = requests.get(&token) {
                        if is_json_end(&requests_vec) {
                            if let Some(socket) = sockets.get_mut(&token) {
                                if let Err(e) = socket.reregister(poll.registry(), token, Interest::WRITABLE) {
                                    error!(token = ?token, error = %e, "Failed to change to WRITABLE");
                                }
                            }
                        }
                    }
                }
                
                token if event.is_writable() => {
                    if let Some(device_id) = devices.get(&token) {
                        let update_key = format!("{}/update", device_id);

                        match redis_conn.lrange::<_, Vec<String>>(&update_key, 0, 100) {
                            Ok(value) if !value.is_empty() => {
                                if let Some(content_send) = value.first() {
                                    if let Some(socket) = sockets.get_mut(&token) {
                                        if let Err(e) = socket.write_all(content_send.as_bytes()) {
                                            error!(token = ?token, error = %e, "Failed to write to socket");
                                        }
                                    }

                                    if let Some(req) = requests.get_mut(&token) {
                                        req.clear();
                                    }

                                    // Remove from queue
                                    if let Err(e) = redis_conn.rpop::<_, Option<String>>(&update_key, None) {
                                        warn!(key = &update_key, error = %e, "Failed to RPOP");
                                    }

                                    // Set ACK flag
                                    let verify_ack = format!("{}/verify_ack", device_id);
                                    if let Err(e) = redis_conn.set::<_, _, ()>(&verify_ack, "1") {
                                        warn!(key = &verify_ack, error = %e, "Failed to set ACK");
                                    }
                                }
                            }
                            Ok(_) => debug!("No updates in queue"),
                            Err(e) => error!(key = &update_key, error = %e, "Failed to get updates"),
                        }
                    }

                    if let Some(socket) = sockets.get_mut(&token) {
                        if let Err(e) = socket.reregister(poll.registry(), token, Interest::READABLE) {
                            error!(token = ?token, error = %e, "Failed to change to READABLE");
                        }
                    }
                }
                
                _ => unreachable!(),
            }
        }

        // Log stats every 10000 events (optional, for debugging)
        if (stats.0 + stats.1) % 10000 == 0 && stats.0 > 0 {
            info!(
                processed = stats.0,
                errors = stats.1,
                connections = sockets.len(),
                "Statistics update"
            );
        }
    }
}
