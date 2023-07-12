import mysql
import mysql.connector
import threading
import time
import random
import queue
import requests

# Global Variables
EVENTS_PER_USER = 10
NUM_USERS = 100
DATABASE_FILE = 'event_database'

# Event stages
EVENT_STAGES = [
    'App Access',
    'Banner Click',
    'Product List View',
    'Product Selection',
    'Add to Cart',
    'Order Placement'
]

# Database Schema
CREATE_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    user_id INTEGER,
    event_stage TEXT,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
'''

# Event Producer
class Driver:
    def __init__(self, user_id):
        self.user_id = user_id

    def generate_event(self, event_stage):
        event = {
            'user_id': self.user_id,
            'event_stage': event_stage
        }
        return event

    def run(self):
        for _ in range(EVENTS_PER_USER):
            event_stage = random.choice(EVENT_STAGES)
            event = self.generate_event(event_stage)
            event_webhook(event)
            time.sleep(random.uniform(0.1, 1))

# Webhook
def event_webhook(event):
    webhook_url = 'http://localhost:8888/webhook'
    response = requests.post(webhook_url, json=event)
    if response.status_code != 200:
        print(f'Error sending event to webhook: {response.status_code}')

# In-memory Queue
class InMemoryQueue:
    def __init__(self):
        self.queue = queue.Queue()

    def put_event(self, event):
        self.queue.put(event)

    def get_event(self):
        return self.queue.get()

# Queue Consumer
class QueueConsumer(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            event = self.queue.get_event()
            process_event(event)
            self.queue.task_done()

# Database
class EventRepository:
    def __init__(self):
        self.conn = mysql.connector.connect(host='localhost',database=DATABASE_FILE,user='root',password='mysql')
        self.cursor = self.conn.cursor()
        self.cursor.execute(CREATE_TABLE_QUERY)
        self.conn.commit()

    def insert_event(self, event):
        query = '''
        INSERT INTO events (user_id, event_stage)
        VALUES (?, ?)
        '''
        self.cursor.execute(query, (event['user_id'], event['event_stage']))
        self.conn.commit()

# Process Event
def process_event(event):
    event_repository.insert_event(event)

# Create Database
def create_database():
    conn = mysql.connector.connect(host='localhost',database=DATABASE_FILE,user='root',password='mysql')
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_QUERY)
    conn.commit()
    cursor.close()
    conn.close()

# Main function
if __name__ == '__main__':
    # Create the database if it doesn't exist
    create_database()

    event_repository = EventRepository()
    in_memory_queue = InMemoryQueue()

    # Start the Queue Consumer
    queue_consumer = QueueConsumer(in_memory_queue)
    queue_consumer.start()

    # Start the Event Producers
    for user_id in range(1, NUM_USERS + 1):
        driver = Driver(user_id)
        driver_thread = threading.Thread(target=driver.run)
        driver_thread.start()
        time.sleep(0.1)  # Small delay between starting each driver thread

    # Wait for all events to be processed
    in_memory_queue.queue.join()

    # Print the percentage of users in each stage
    total_users = NUM_USERS
    stages_count = {stage: 0 for stage in EVENT_STAGES}

    conn = mysql.connector.connect(host='localhost',database=DATABASE_FILE,user='root',password='mysql')
    cursor = conn.cursor()

    for stage in EVENT_STAGES:
        query = '''
        SELECT COUNT(DISTINCT user_id)
        FROM events
        WHERE event_stage = ?
        '''
        cursor.execute(query, (stage,))
        count = cursor.fetchone()[0]
        stages_count[stage] = count
        percentage = (count / total_users) * 100
        print(f'Percentage of users at {stage}: {percentage:.2f}%')

    cursor.close()
    conn.close()
