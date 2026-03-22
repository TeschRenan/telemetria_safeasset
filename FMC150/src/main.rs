// Benchmarks:
// $ ab -n 1000000 -c 128 -k http://127.0.0.1:8080/
// $ wrk -d 30s -t 4 -c 128 http://127.0.0.1:8080/

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::io::{Read, Write};

use std::sync::mpsc::{self};

use redis;

use std::time::Instant;
use std::error::Error;

use mio::event::Source;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use amiquip::{Connection,Publish, Result, Exchange, AmqpProperties};

use serde_json::Value;

use aws_sdk_sqs::Client;
use dotenv::from_filename;
use std::path::Path;

use std::env;


fn is_dolar_crnl(window: &[u8]) -> bool {
    window.len() >= 1
        && (window[0] == b'$')
}

fn start_worker_sqs(rx: std::sync::mpsc::Receiver<String>) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(worker_sqs(rx));
    });
}


async fn worker_sqs(rx: std::sync::mpsc::Receiver<String>) {

    println!("Starting worker");

    from_filename(Path::new("/home/ec2-user/rust/compilado/FMC150/.env")).ok();



    let region = env::var("REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let account_id = env::var("ACCOUNT_ID").expect("ACCOUNT_ID não definido");
    let queue_name = env::var("QUEUE_FMC150").expect("QUEUE_FMC150 não definido");

    let queue_url = format!(
        "https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
    );

    let config = aws_config::load_from_env().await;
    let aws_client = Client::new(&config);

    let mut start = Instant::now();
    let mut duration;

    for message in rx {

        start = Instant::now();

        let payload = message.trim_end_matches('\0').to_string();

        println!("Message received {}", payload);

        let send_resp = aws_client
        .send_message()
        .queue_url(&queue_url)
        .message_body(payload)
        .send()
        .await;

        println!("Mensagem enviada com ID: {:?}", send_resp.expect("REASON").message_id());

        duration = start.elapsed().as_micros();

        println!("Tempo de envio SQS {} us", duration);

        // match aws_client
        //     .send_message()
        //     .queue_url(&queue_url)
        //     .message_body(message)
        //     .send()
        //     .await
        // {
        //     Ok(resp) => println!("Mensage sended with ID: {:?}", resp.message_id()),
        //     Err(err) => eprintln!("Erro to send SQS: {}", err),
        // }
    }
}

fn main() -> Result<(), Box<dyn Error>> {

    let (tx_sqs, rx_sqs) = mpsc::channel::<String>();

    start_worker_sqs(rx_sqs);

    let address = "0.0.0.0:50004";
    let mut listener = TcpListener::bind(address.parse().unwrap()).unwrap();

    let mut poll = Poll::new().unwrap();

    poll.registry()
        .register(&mut listener, Token(0), Interest::READABLE)
        .unwrap();

    let mut counter: usize = 0;
    let mut sockets: HashMap<Token, TcpStream> = HashMap::new();
    let mut requests: HashMap<Token, Vec<u8>> = HashMap::new();
    let mut devices: HashMap<Token, String> = HashMap::new();
    let mut buffer = [0_u8; 2048];

    let redis_client = redis::Client::open("redis://telemetria:T3l3m3tr1@2025!@localhost:6379/");
    let mut redis_conn = redis_client.expect("REASON").get_connection().expect("failed to connect to Redis");

    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    // Open a channel - None says let the library choose the channel ID.
    let channel = connection.open_channel(None)?;

    let exchange = Exchange::direct(&channel);
    
    let mut events = Events::with_capacity(4096);
    loop {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            match event.token() {
                Token(0) => loop {
                    match listener.accept() {
                        Ok((mut socket, _)) => {
                            counter += 1;
                            let token = Token(counter);
                            socket
                                .register(poll.registry(), token, Interest::READABLE)
                                .unwrap();

                            let remote_ip = socket.peer_addr().expect("REASON").ip();

                            println!("Remote IP: {}", remote_ip);

                            sockets.insert(token, socket);
                            requests.insert(token, Vec::with_capacity(10000));
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                },
                token if event.is_readable() => {

                    loop {
                        let read = sockets.get_mut(&token).unwrap().read(&mut buffer);

                        match read {
                            Ok(0) => {
                                sockets.remove(&token);
                                devices.remove(&token);
                                break;
                            }
                            Ok(n) => {
                            
                                let request = String::from_utf8_lossy(&buffer[..]);

                                let valid_chars = request.chars()
                                    .filter(|c| c.is_alphanumeric())
                                    .count();
                                
                                println!("valid_chars {}", valid_chars);

                                println!(" Request {} and token {}", request, token.0);

                                if valid_chars == 15 {

                                    let device_id = devices.get_mut(&token);

                                    if device_id.is_none(){

                                        let cleaned_imei = request.chars()
                                        .filter(|c| c.is_ascii_digit())
                                        .collect::<String>();


                                        devices.insert(token, cleaned_imei.to_string());

                                    }    
                                    
                                }
                                else{
                                    
                                    if let Some(device_str) = devices.get_mut(&token) {
                                        //println!("Imei {}", device_str);

                                        let value: String = redis::cmd("GET")
                                            .arg(&[device_str.to_string()])
                                            .query(&mut redis_conn)
                                            .expect("failed to execute GET");

                                            println!("{}",value);

                                            let payload = request.trim_end_matches('\0');

                                            match serde_json::from_str::<Value>(&payload) {
                                                Ok(mut json) => {                                          

                                                    if let Some(reported) = json["state"]["reported"].as_object_mut() {
                                                        reported.insert("id_telemetria_device".to_string(), Value::String(value.to_string()));
                                                        reported.insert("imei".to_string(), Value::String(device_str.to_string()));
                                                    }
                                                
                                                    let payload = serde_json::to_string(&json).unwrap();

                                                    println!("{}", payload);
        
                                                    if !value.is_empty() {

                                                        tx_sqs.send(payload.to_string()).expect("Fail to send to worker");
                                                        
                                                        exchange.publish(Publish::with_properties(
                                                            payload.as_bytes(),
                                                            "fmc150",
                                                            // delivery_mode 2 makes message persistent
                                                            AmqpProperties::default().with_delivery_mode(2),
                                                        ))?;
                
                                                        exchange.publish(Publish::with_properties(
                                                            payload.as_bytes(),
                                                            "fmc150_log",
                                                            // delivery_mode 2 makes message persistent
                                                            AmqpProperties::default().with_delivery_mode(2),
                                                        ))?;
                
                                                    }
                                                    
                                                },
                                                Err(err) => {
                                                    
                                                    println!("Erro to read JSON: {}", err);
                                                    
                                                }
                                            }

                                    } else {
                                        println!("Device_id not found removing device...");

                                    }
                                    
                                }

                                // inserir aqui a parte do ACK

                                if let Some(req) = requests.get_mut(&token){

                                    for b in &buffer[0..n] {

                                        req.push(*b);
                                    }
    
                                    for elem in buffer.iter_mut() { *elem = 0; }  

                                }                        
                                else {

                                    // Sporadic events happen, we can safely ignore them.
                                    println!("Error to clear the socket");

                                };

  
                                
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(_) => break,
                        }
                    }

                    let ready = requests.get(&token).unwrap().windows(1).any(is_dolar_crnl);

                    if ready {

                        let _done = if let Some(socket) = sockets.get_mut(&token) {
                            socket
                            .borrow_mut()
                            .reregister(poll.registry(), token, Interest::WRITABLE)
                            .expect("Error to change to writable");
                        } else {
                            // Sporadic events happen, we can safely ignore them.
                            println!("Error to obtain socket to change to writable");
                        };

                    }
                    else{

                        println!("Error to find $");

                    }
                    
                }
                token if event.is_writable() => {

                    if let Some(device_id) = devices.get_mut(&token){

                        let update_key = format!("{}{}", device_id, "/update");

                        let remove_key = format!("{}", update_key);
    
                        let value:Vec<String> = redis::cmd("LRANGE")
                        .arg(&[update_key])
                        .arg("0")
                        .arg("100")
                        .query(&mut redis_conn)
                        .expect("failed to execute LRANGE");
                        
                        //println!("{:?}",value);
                        
                        if value.len() != 0 {
    
                            let content_send:String = value[0].clone();                     
                            
                            requests.get_mut(&token).unwrap().clear();
                            sockets
                                .get_mut(&token)
                                .unwrap()
                                .write_all(&content_send.into_bytes())
                                .unwrap();                   
                            
    
                            let value:String = redis::cmd("RPOP")
                            .arg(&[remove_key])
                            .query(&mut redis_conn)
                            .expect("failed to execute RPOP");
    
                            println!("Key removed {}", value);
    
                            let verify_ack = format!("{}{}", device_id, "/verify_ack");
    
                            let _value:String = redis::cmd("SET")
                            .arg(&[verify_ack])
                            .arg(&"1")
                            .query(&mut redis_conn)
                            .expect("failed to execute SET");
    
                        }

                    }    
                    
                    
                    let _done = if let Some(socket) = sockets.get_mut(&token) {
                        socket
                        .borrow_mut()
                        .reregister(poll.registry(), token, Interest::READABLE)
                        .expect("Error to change to readable");
                    } else {
                        // Sporadic events happen, we can safely ignore them.
                        println!("Error to obtain socket to change to readable");
                    };

                }
                _ => unreachable!(),
            }
        }
    }
}