use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, sleep},
    time::Duration,
};

use clap::Parser;

use rustdds::DomainParticipant;

use serde::{Deserialize, Serialize};
use rustdds::{policy::Reliability, QosPolicyBuilder, TopicKind};

extern crate log;

#[derive(Debug, Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// String to send by writer 
    #[clap(short, long, value_parser, required = false, default_value = "1000")]
    size: u32,
    #[clap(short, long, value_parser)]
    reader_topic: Option<String>,
    #[clap(short, long, value_parser)]
    writer_topic: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub content: String,
    pub id: u64,
}

fn main() {
    env_logger::init();
    let running_0 = Arc::new(AtomicBool::new(true));
    let r = running_0.clone();
    ctrlc::set_handler(move || r.store(false, Ordering::SeqCst)).unwrap();

    let args = Args::parse();

    let domain_participant = DomainParticipant::new(0).unwrap();
    let qos = QosPolicyBuilder::new()
        .reliability(Reliability::Reliable {
            max_blocking_time: rustdds::Duration::DURATION_ZERO,
        })
        .build();

    if let Some(topic) = args.reader_topic {
        let subscriber = domain_participant.create_subscriber(&qos).unwrap();
        let topic = domain_participant
            .create_topic(
                topic.to_string(),
                "Test of RustDDS".to_string(),
                &qos,
                TopicKind::NoKey,
            )
            .unwrap();

        let mut reader = subscriber
            .create_datareader_no_key_cdr::<Message>(&topic, None)
            .unwrap();

        // let mut reader = subscriber
        //     .create_datareader_no_key::<Message, CDRDeserializerAdapter<Message>>(&topic, None)
        //     .unwrap();

        let running = running_0.clone();
        thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                if let Ok(Some(sample)) = reader.read_next_sample() {
                    let msg = sample.into_value();
                    println!("read <<< {:?} #{}", msg.content.len(), msg.id);
                }
            }
        });
    }

    if let Some(topic) = args.writer_topic {
        let publisher = domain_participant.create_publisher(&qos).unwrap();
        let topic = domain_participant
            .create_topic(
                topic.to_string(),
                "Test of RustDDS".to_string(),
                &qos,
                TopicKind::NoKey,
            )
            .unwrap();

        let writer = publisher
            .create_datawriter_no_key_cdr(&topic, None)
            .unwrap();

        // let writer = publisher
        //     .create_datawriter_no_key::<Message, CDRSerializerAdapter<Message>>(&topic, None)
        //     .unwrap();

        let running = running_0.clone();
        let mut s = String::new();
        for _ in 0..args.size {
            s.push('z');
        }
        thread::spawn(move || {
            let mut msg = Message {
                content: s, 
                id: 0,
            };
            while running.load(Ordering::SeqCst) {
                msg.id += 1;
                writer.write(msg.clone(), None).unwrap();
                println!("write >>> {:?} #{}", msg.content.len(), msg.id);
                sleep(Duration::from_secs(1));
            }
        });
    }

    while running_0.load(Ordering::SeqCst) {}
}
