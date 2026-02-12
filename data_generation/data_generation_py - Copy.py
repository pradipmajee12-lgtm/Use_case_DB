"""
====================================================================================
NAME: Trade Event Generator & Publisher 
Amendment date- 2026-02-11
===================================================================================
DESCRIPTION:
    A trade simulation publisher that generates synthetic trade events 
    and continuously streams them to a Google Cloud Pub/Sub topic. 
    This component acts as the upstream data source for the real-time 
    trade processing pipeline.
    The program simulates realistic trade attributes including versioning, 
    pricing, maturity dates, and metadata to support downstream validation 
    and analytics processing.
LOGIC FLOW:
    Initialize Pub/Sub Publisher Client and configure target topic.
    Generate synthetic trade event with randomized attributes (trade_id, version, party_id, product, price, etc.).
    Populate timestamps (trade_date, maturity_date, created_at) in ISO format.
    Construct trade payload in JSON format.
    Publish message to Pub/Sub topic.
    Repeat every 2 seconds to simulate continuous streaming.

CHANGE HISTORY:
    Date        Version  Author      Description
    ----------  -------  ----------  -----------------------------------------------
    2026-02-11  1.0      Initial     Source--> Pub/Sub--> BQ flow.
    ====================================================================================
"""


import json, random, time, string
#from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import argparse

#publisher = pubsub_v1.PublisherClient()

## Please mention project_id & pub/sub topic name and it should be as parameter . for testing we did harded coded 
#topic_path = publisher.topic_path("my-first-project", "trade-transaction-topic")

# Argument parser to pass project_id and topic as parameters  - Just commented the hardcoded above line and remove commented below lines
#parser = argparse.ArgumentParser(description="Publish simulated trade events to Pub/Sub")
#parser.add_argument("--project_id", required=True, help="GCP Project ID")
#parser.add_argument("--topic", required=True, help="Pub/Sub Topic Name")
#args = parser.parse_args()

def generate_trade():
    trade_id = random.randint(1000, 9999)
    version = random.randint(1, 5)
    party_id = ''.join(random.choices(string.ascii_uppercase, k=5)) + \
           ''.join(random.choices(string.digits, k=4)) + \
           random.choice(string.ascii_uppercase)
    book_id= random.randint(1, 999)
    product_id = random.choice(["option", "future", "delivery"])
    price = round(random.uniform(100.00, 500.00), 2)
    currency= random.choice(["INR", "USD", "EUR"])
    trade_date = datetime.now() - timedelta(seconds=random.randint(1, 3600))
    #trade_date= (datetime.now() + timedelta(days=random.randint(-5, 10))).strftime("%Y-%m-%d")
    maturity_date = (datetime.now() + timedelta(days=random.randint(-5, 10))).strftime("%Y-%m-%d")
    trade = {
        "trade_id": trade_id,
        "version": version,
        "party_id": party_id,
        "book_id": 100,
        "product_id":product_id,
        "price": price,
        "currency": currency,
        "trade_date": trade_date,
        "source":"broker_X",
        "maturity_date": maturity_date,
        "created_at": datetime.now().isoformat()
    }
    return trade

while True:
    trade = generate_trade()
    #publisher.publish(topic_path, json.dumps(trade).encode("utf-8"))
    print(f"Published trade: {trade}")
    time.sleep(2)

if __name__ == "__main__":
    main()