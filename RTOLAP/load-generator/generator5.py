# Copyright (c) 2026 Dynatrace LLC. All rights reserved.
# Licensed under the terms in the repository LICENSE file.

import os
from kafka import KafkaProducer
import json
import random
import string
import time
from datetime import datetime, timedelta

# Environment variables
kafka_topic = os.getenv("KAFKA_TOPIC", "test")
broker = os.getenv("KAFKA_BROKER", "localhost:9092")
messages_per_second = int(os.getenv("MESSAGES_PER_SECOND", "10000"))
duration_seconds = int(os.getenv("DURATION_SECONDS", "10"))

# Keywords and frequencies
special_frequency1 = int(os.getenv("SPECIAL_FREQUENCY1", "1000000")) 
matcheable1 = os.getenv("MATCHEABLE1", "melborpmelborp")

special_frequency2 = int(os.getenv("SPECIAL_FREQUENCY2", "50000"))
matcheable2 = os.getenv("MATCHEABLE2", "problem_id")

special_frequency3 = int(os.getenv("SPECIAL_FREQUENCY3", "999"))
matcheable3 = os.getenv("MATCHEABLE3", "Error_999")

special_frequency4 = int(os.getenv("SPECIAL_FREQUENCY4", "2000"))
matcheable4 = matcheable2

special_frequency5 = int(os.getenv("SPECIAL_FREQUENCY5", "500000"))
matcheable5 = os.getenv("MATCHEABLE5", "123456789")

special_frequency6 = int(os.getenv("SPECIAL_FREQUENCY6", "10011"))
matcheable6 = matcheable1

min_words_per_string_content = int(os.getenv("STRINGSIZE", "60"))

def generate_random_sentence(min_words=min_words_per_string_content):
    words = [''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 10))) for _ in range(min_words)]
    return ' '.join(words)

def generate_random_sentence_with_keyword(min_words, keyword=None, frequency=0, index=0):
    sentence = generate_random_sentence(min_words)
    if frequency > 0 and (index + 1) % frequency == 0 and keyword:
        sentence += " " + keyword
    return sentence

def generate_log_entry(timestamp, content, additionalInfo, field1, field2, field3, status="INFO", event_type="LOG", loglevel="DEBUG"):
    return {
        "timestamp": int(timestamp.timestamp() * 1000),
        "content1": content,
        "status": status,
        "eventType": event_type,
        "content2": additionalInfo,
        "content3": field1,
        "content4": field2,
        "content5": field3
    }

def send_logs_to_kafka(producer, kafka_topic):
    start_time = datetime.utcnow()
    total_messages = messages_per_second * duration_seconds

    for i in range(total_messages):
        current_time = start_time + timedelta(milliseconds=i)
        content = generate_random_sentence()
        field1 = generate_random_sentence()
        field2 = generate_random_sentence()
        field3 = generate_random_sentence()
        if (i + 1) % special_frequency1 == 0:
            content += " " + matcheable1

        if (i + 1) % special_frequency2 == 0:
            additionalInfo = generate_random_sentence() + " " +  matcheable2
        else:
            additionalInfo = generate_random_sentence()

        if (i + 1) % special_frequency3 == 0:
            field1 += " " + matcheable3
        else:
            field1 = generate_random_sentence_with_keyword(min_words_per_string_content, matcheable4, special_frequency4, i)   

        if (i + 1) % special_frequency4 == 0:
            field2 += " " + matcheable4
        else:
            field2 = generate_random_sentence_with_keyword(min_words_per_string_content, matcheable5, special_frequency5, i)
        
        if (i + 1) % special_frequency5 == 0:
            field3 += " " + matcheable5
        else:
            field3 = generate_random_sentence_with_keyword(min_words_per_string_content, matcheable6, special_frequency6, i)

        log_entry = generate_log_entry(current_time, content, additionalInfo, field1, field2, field3)
        producer.send(kafka_topic, value=log_entry)
        time.sleep(1 / messages_per_second)

    producer.flush()

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers=broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send logs to Kafka topic
send_logs_to_kafka(producer, kafka_topic)