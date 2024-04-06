import os
import random
import time
import uuid
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer
import simplejson as json

# Start and Stop locations
CHARLOTTE_COORDINATES = {"latitude": 35.2271, "longitude": 80.8431}
RALEIGH_COORDINATES = {"latitude": 35.7796, "longitude": 78.6382}

# Calculate movement increments
LATITUDE_INCREMENT = (RALEIGH_COORDINATES['latitude'] - CHARLOTTE_COORDINATES['latitude']) / 100                         #  0.005525 increments
LONGITUDE_INCREMENT = (RALEIGH_COORDINATES['longitude'] - CHARLOTTE_COORDINATES['longitude']) / 100                      # -0.022049 increments

# Environment Variable for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_CAMERA_TOPIC', 'traffic_camera_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

# random.seed(42)
start_time = datetime.now()
start_location = CHARLOTTE_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60))
    return start_time

def simulate_vehicle_movement():
    global start_location

    # Move towards Raleigh
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # Add randomness to simulate traffic
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(45, 85),
        'direction': 'North-East',
        'make': 'Honda',
        'model': 'Accord',
        'year': 2017,
        'fuelType': 'Gas'
    }

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(45, 85),
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'cameraID': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(70,90),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0,26),
        'windSpeed': random.uniform(0,21),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0,51)
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'incidentID': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'location': location,
        'timestamp': timestamp,
        'status': random.choice(['Active, Resolved'])
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message failed to delivery: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], 'Camera-A')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        # Destination Check
        if vehicle_data['location'][0] >= RALEIGH_COORDINATES['latitude'] and vehicle_data['location'][1] <= RALEIGH_COORDINATES['longitude']:
            print('Vehicle has reached destination. Simulation ending ... ')
            break

        stream_data(producer, VEHICLE_TOPIC, vehicle_data)
        stream_data(producer, GPS_TOPIC, gps_data)
        stream_data(producer, TRAFFIC_TOPIC, traffic_camera_data)
        stream_data(producer, WEATHER_TOPIC, weather_data)
        stream_data(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(1)

def stream_data(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()



if __name__ == "__main__":
    producer_configs = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda e: print(f'Kafka Producer error: {e}')
    }
    producer = SerializingProducer(producer_configs)

    try:
        simulate_journey(producer, 'Vehicle-SVC-1')
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected Error occurred: {e}')
