# Copyright (c) 2026 Dynatrace LLC. All rights reserved.
# Licensed under the terms in the repository LICENSE file.

from kafka import KafkaProducer
import os
import json
import random
import string
from datetime import datetime
from time import sleep
import time

# Environment variables for Kafka
kafka_topic = os.getenv("KAFKA_TOPIC", "test")
broker = os.getenv("KAFKA_BROKER", "localhost:9092")
messages_per_second = int(os.getenv("MESSAGES_PER_SECOND", "100"))
duration_seconds = int(os.getenv("DURATION_SECONDS", "10"))

# Custom configurations for special frequencies and keywords
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

# Fixed number of words for the entire content field
words_per_string_content = int(os.getenv("STRINGSIZE", "60"))

# Debug mode - set to True for local testing
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"
DEBUG_SAMPLE_RATE = int(os.getenv("DEBUG_SAMPLE_RATE", "100000"))  # Print every Nth message

# Statistics tracking
stats = {
    "total_messages": 0,
    "total_content1_word_counts": [],
    "total_content2_word_counts": [],
    "keyword_insertions": {
        matcheable1: 0,
        matcheable2: 0,
        matcheable3: 0,
        matcheable4: 0,
        matcheable5: 0
    },
    "timing": {
        "behind_schedule_count": 0,
        "max_delay": 0.0,
        "total_delay": 0.0
    }
}

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10,
    batch_size=1048576,
    compression_type='gzip'
)

def debug_print(message):
    """Print debug messages if DEBUG_MODE is enabled."""
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")

def generate_and_send_log(index):
    """
    Generate a single log entry and send it to Kafka immediately.

    Args:
        index (int): The index of the log entry.
    """
    timestamp = datetime.utcnow()
    
    # Generate exactly words_per_string_content random words for content1
    words_content1 = [''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 10))) 
                      for _ in range(words_per_string_content)]
    
    # Generate exactly words_per_string_content random words for content2
    words_content2 = [''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 10))) 
                      for _ in range(words_per_string_content)]
    
    # Determine which keywords need to be inserted based on frequencies
    keywords_to_insert_content1 = []
    keywords_to_insert_content2 = []
    
    if (index + 1) % special_frequency1 == 0:
        keywords_to_insert_content1.append(matcheable1)
    
    if (index + 1) % special_frequency2 == 0:
        keywords_to_insert_content1.append(matcheable2)
    
    if (index + 1) % special_frequency3 == 0:
        keywords_to_insert_content1.append(matcheable3)
    
    if (index + 1) % special_frequency4 == 0:
        keywords_to_insert_content1.append(matcheable4)
    
    # matcheable5 goes to content2
    if (index + 1) % special_frequency5 == 0:
        keywords_to_insert_content2.append(matcheable5)
    
    # Replace random words with keywords in content1 (maintaining total word count)
    used_positions_content1 = set()
    for keyword in keywords_to_insert_content1:
        # Find a position that hasn't been used yet
        available_positions = [i for i in range(len(words_content1)) if i not in used_positions_content1]
        if available_positions:
            replace_pos = random.choice(available_positions)
            words_content1[replace_pos] = keyword
            used_positions_content1.add(replace_pos)
            if DEBUG_MODE:
                stats["keyword_insertions"][keyword] += 1
    
    # Replace random words with keywords in content2 (maintaining total word count)
    used_positions_content2 = set()
    for keyword in keywords_to_insert_content2:
        # Find a position that hasn't been used yet
        available_positions = [i for i in range(len(words_content2)) if i not in used_positions_content2]
        if available_positions:
            replace_pos = random.choice(available_positions)
            words_content2[replace_pos] = keyword
            used_positions_content2.add(replace_pos)
            if DEBUG_MODE:
                stats["keyword_insertions"][keyword] += 1
    
    # Combine all words into the content fields
    combined_content1 = ' '.join(words_content1)
    combined_content2 = ' '.join(words_content2)
    
    total_word_count_content1 = len(combined_content1.split())
    total_word_count_content2 = len(combined_content2.split())
    
    stats["total_content1_word_counts"].append(total_word_count_content1)
    stats["total_content2_word_counts"].append(total_word_count_content2)
    stats["total_messages"] += 1

    log_entry = {
        "timestamp": int(timestamp.timestamp() * 1000),
        "content1": combined_content1,
        "content2": combined_content2,
        "status": "INFO",
        "eventType": "LOG"
    }

    # Debug output for sampled messages
    if DEBUG_MODE and (index + 1) % DEBUG_SAMPLE_RATE == 0:
        debug_print(f"\n{'='*80}")
        debug_print(f"Message #{index + 1}")
        debug_print(f"{'='*80}")
        debug_print(f"  Content1 word count: {total_word_count_content1}")
        debug_print(f"  Content2 word count: {total_word_count_content2}")
        debug_print(f"  Expected word count: {words_per_string_content}")
        debug_print(f"  Keywords inserted in content1: {keywords_to_insert_content1 if keywords_to_insert_content1 else 'None'}")
        debug_print(f"  Keywords inserted in content2: {keywords_to_insert_content2 if keywords_to_insert_content2 else 'None'}")
        debug_print(f"  Content1 length (chars): {len(combined_content1)}")
        debug_print(f"  Content2 length (chars): {len(combined_content2)}")
        debug_print(f"  JSON size (bytes): {len(json.dumps(log_entry))}")
        if total_word_count_content1 != words_per_string_content:
            debug_print(f"  ⚠️  WARNING: Content1 word count mismatch!")
        if total_word_count_content2 != words_per_string_content:
            debug_print(f"  ⚠️  WARNING: Content2 word count mismatch!")
        debug_print(f"{'='*80}\n")

    # Send the log entry to Kafka
    producer.send(kafka_topic, value=log_entry)

def print_statistics():
    """Print comprehensive statistics about generated messages."""
    if not DEBUG_MODE or stats["total_messages"] == 0:
        return
    
    print("\n" + "="*80)
    print("FINAL STATISTICS")
    print("="*80)
    print(f"Total messages generated: {stats['total_messages']}")
    print(f"\nExpected words per content field: {words_per_string_content}")
    
    print("\n" + "-"*80)
    print("CONTENT1 FIELD WORD COUNT STATISTICS:")
    print("-"*80)
    if stats["total_content1_word_counts"]:
        min_total = min(stats["total_content1_word_counts"])
        max_total = max(stats["total_content1_word_counts"])
        avg_total = sum(stats["total_content1_word_counts"]) / len(stats["total_content1_word_counts"])
        print(f"  Min: {min_total} words")
        print(f"  Max: {max_total} words")
        print(f"  Avg: {avg_total:.2f} words")
        print(f"  Expected: {words_per_string_content} words")
        
        # Check if all messages have exactly the expected word count
        all_correct = all(count == words_per_string_content for count in stats["total_content1_word_counts"])
        if all_correct:
            print(f"  ✓ All messages have exactly {words_per_string_content} words!")
        else:
            print(f"  ⚠️  WARNING: Some messages have incorrect word counts!")
            incorrect_count = sum(1 for count in stats["total_content1_word_counts"] if count != words_per_string_content)
            print(f"  Incorrect messages: {incorrect_count}/{stats['total_messages']}")
    
    print("\n" + "-"*80)
    print("CONTENT2 FIELD WORD COUNT STATISTICS:")
    print("-"*80)
    if stats["total_content2_word_counts"]:
        min_total = min(stats["total_content2_word_counts"])
        max_total = max(stats["total_content2_word_counts"])
        avg_total = sum(stats["total_content2_word_counts"]) / len(stats["total_content2_word_counts"])
        print(f"  Min: {min_total} words")
        print(f"  Max: {max_total} words")
        print(f"  Avg: {avg_total:.2f} words")
        print(f"  Expected: {words_per_string_content} words")
        
        # Check if all messages have exactly the expected word count
        all_correct = all(count == words_per_string_content for count in stats["total_content2_word_counts"])
        if all_correct:
            print(f"  ✓ All messages have exactly {words_per_string_content} words!")
        else:
            print(f"  ⚠️  WARNING: Some messages have incorrect word counts!")
            incorrect_count = sum(1 for count in stats["total_content2_word_counts"] if count != words_per_string_content)
            print(f"  Incorrect messages: {incorrect_count}/{stats['total_messages']}")
    
    print("\n" + "-"*80)
    print("KEYWORD INSERTION STATISTICS:")
    print("-"*80)
    for keyword, count in stats["keyword_insertions"].items():
        field = "content2" if keyword == matcheable5 else "content1"
        print(f"  '{keyword}' ({field}): {count} insertions")
    
    print("\n" + "-"*80)
    print("TIMING STATISTICS:")
    print("-"*80)
    if stats["timing"]["behind_schedule_count"] > 0:
        print(f"  Messages sent behind schedule: {stats['timing']['behind_schedule_count']}")
        print(f"  Maximum delay: {stats['timing']['max_delay']:.4f} seconds")
        print(f"  Average delay (when behind): {stats['timing']['total_delay'] / stats['timing']['behind_schedule_count']:.4f} seconds")
        print(f"  ⚠️  WARNING: System struggled to maintain target rate!")
    else:
        print(f"  ✓ All messages sent on time!")
        print(f"  Target rate maintained successfully.")
    
    print("\n" + "="*80 + "\n")

def run_real_time_producer():
    """
    Run the Kafka producer in real-time, generating and sending messages dynamically.
    Uses time-based pacing to maintain accurate message rate regardless of execution time.
    """
    total_messages = messages_per_second * duration_seconds
    interval = 1.0 / messages_per_second

    print(f"\n{'='*80}")
    print(f"KAFKA PRODUCER CONFIGURATION")
    print(f"{'='*80}")
    print(f"Broker: {broker}")
    print(f"Topic: {kafka_topic}")
    print(f"Messages per second: {messages_per_second}")
    print(f"Duration: {duration_seconds} seconds")
    print(f"Total messages to send: {total_messages}")
    print(f"Interval between messages: {interval*1000:.2f} ms")
    print(f"Words per content field: {words_per_string_content}")
    print(f"Debug mode: {DEBUG_MODE}")
    if DEBUG_MODE:
        print(f"Debug sample rate: Every {DEBUG_SAMPLE_RATE} messages")
    print(f"\nKeyword Frequencies:")
    print(f"  {matcheable1} (content1): every {special_frequency1} messages")
    print(f"  {matcheable2} (content1): every {special_frequency2} messages")
    print(f"  {matcheable3} (content1): every {special_frequency3} messages")
    print(f"  {matcheable4} (content1): every {special_frequency4} messages")
    print(f"  {matcheable5} (content2): every {special_frequency5} messages")
    print(f"{'='*80}\n")

    print(f"Producing {messages_per_second} messages per second for {duration_seconds} seconds...")
    print("Using time-based pacing for accurate rate control...\n")

    # Record the start time for accurate pacing
    start_time = time.time()
    
    for i in range(total_messages):
        # Generate and send the message
        generate_and_send_log(i)
        
        # Calculate when the next message should be sent
        next_send_time = start_time + (i + 1) * interval
        current_time = time.time()
        sleep_time = next_send_time - current_time
        
        # If we're ahead of schedule, sleep until the next message time
        if sleep_time > 0:
            sleep(sleep_time)
        else:
            # We're behind schedule - track this for statistics
            delay = -sleep_time
            stats["timing"]["behind_schedule_count"] += 1
            stats["timing"]["total_delay"] += delay
            if delay > stats["timing"]["max_delay"]:
                stats["timing"]["max_delay"] = delay
            
            # Warn if we're significantly behind (more than one interval)
            if DEBUG_MODE and delay > interval:
                debug_print(f"⚠️  Running behind schedule by {delay:.4f}s at message {i+1}")
                debug_print(f"    This is {delay/interval:.2f}x the message interval")

    # Ensure all messages are sent before reporting completion
    producer.flush()
    
    # Calculate actual duration and effective rate
    actual_duration = time.time() - start_time
    actual_rate = total_messages / actual_duration
    
    print(f"\nCompleted sending {total_messages} messages to topic '{kafka_topic}'.")
    print(f"Actual duration: {actual_duration:.2f} seconds")
    print(f"Target rate: {messages_per_second} msg/s")
    print(f"Actual rate: {actual_rate:.2f} msg/s")
    print(f"Rate accuracy: {(actual_rate/messages_per_second)*100:.2f}%")
    
    # Print final statistics
    print_statistics()

# Start the real-time Kafka producer
if __name__ == "__main__":
    run_real_time_producer()
