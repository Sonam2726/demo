from kafka import KafkaConsumer
import json

def admin():
    consumer=KafkaConsumer(
            "fraud-notification1",
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    for message in consumer:
        alert_data = message.value
            
        print("\n[CRITICAL ALERT]")
        print(f"User_id: {alert_data.get('userId')}")
        print(f"Name: {alert_data.get('name')}")
        print(f"Tx ID: {alert_data.get('tx_id')}")
        print(f"Amount: ${alert_data.get('amount'):.2f}\n")
        
   
if __name__ == "__main__":
    admin()