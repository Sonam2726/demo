from kafka import KafkaConsumer
import json

def user_login_and_listen():
    print("==== FRAUD ALERT SYSTEM ====")
    user_id_input=input("enter user id to login (without password) :")

    try:
        user_id=int(user_id_input)
    except ValueError:
        print("Invalid ID Existing")
        return

    print(f"Logged in as User {user_id}. Listening for alerts...")
    #Listening Kafka
    consumer=KafkaConsumer(
        "fraud-notification",
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        alert_data = message.value
        if alert_data.get('userId') == user_id:
            print("\n[CRITICAL ALERT]")
            print(f"Name: {alert_data.get('name')}")
            print(f"Tx ID: {alert_data.get('tx_id')}")
            print(f"Amount: ${alert_data.get('amount'):.2f}\n")
        
   
if __name__ == "__main__":
 user_login_and_listen()