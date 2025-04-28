import argparse
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from config.config_loader import KAFKA_CONFIG

class ClickstreamEventProducer:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.pages = [
            '/home', '/products', '/category/electronics', '/category/clothing',
            '/cart', '/checkout', '/search', '/account', '/about', '/contact'
        ]
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X)',
            'Mozilla/5.0 (Linux; Android 11; SM-G991B)'
        ]
        self.locations = ['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'IN']
        self.event_types = ['pageview', 'click', 'search', 'add_to_cart', 'purchase']

    def generate_user_id(self):
        return f'user_{random.randint(1, 1000)}'

    def generate_session_id(self):
        return f'session_{random.randint(1, 10000)}'

    def generate_event(self):
        user_id = self.generate_user_id()
        session_id = self.generate_session_id()
        timestamp = datetime.now().isoformat()
        event_type = random.choice(self.event_types)
        
        event = {
            'user_id': user_id,
            'session_id': session_id,
            'timestamp': timestamp,
            'event_type': event_type,
            'page': random.choice(self.pages),
            'user_agent': random.choice(self.user_agents),
            'location': random.choice(self.locations),
            'device_type': random.choice(['desktop', 'mobile', 'tablet'])
        }

        if event_type == 'search':
            event['search_query'] = random.choice(['laptop', 'phone', 'camera', 'headphones'])
        elif event_type == 'add_to_cart':
            event['product_id'] = f'prod_{random.randint(1, 100)}'
            event['quantity'] = random.randint(1, 5)
        elif event_type == 'purchase':
            event['order_id'] = f'order_{random.randint(1, 1000)}'
            event['total_amount'] = round(random.uniform(10, 500), 2)

        return event

    def produce_events(self, topic, duration_seconds):
        end_time = time.time() + duration_seconds
        events_produced = 0

        print(f'Starting to produce events to topic: {topic}')
        while time.time() < end_time:
            event = self.generate_event()
            self.producer.send(topic, event)
            events_produced += 1
            
            if events_produced % 100 == 0:
                print(f'Produced {events_produced} events')
            
            time.sleep(random.uniform(0.1, 0.5))

        self.producer.flush()
        print(f'Finished producing {events_produced} events')

def main():
    parser = argparse.ArgumentParser(description='Clickstream Event Producer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='clickstream',
                        help='Kafka topic to produce to')
    parser.add_argument('--duration', type=int, default=60,
                        help='Duration to run in seconds')
    
    args = parser.parse_args()
    
    producer = ClickstreamEventProducer(args.bootstrap_servers)
    producer.produce_events(args.topic, args.duration)

if __name__ == '__main__':
    main()