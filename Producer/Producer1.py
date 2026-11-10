from datetime import datetime
import requests
from google.transit import gtfs_realtime_pb2
import os,json
from dotenv import load_dotenv
import time
import logging
from kafka import KafkaProducer,KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


load_dotenv()
URL=os.getenv("URL")
API_KEY=os.getenv("API_KEY")
POLL_SECONDS=int(os.getenv("POLL_SECONDS"))
TOPIC_NAME=os.getenv("TOPIC_NAME")
BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP")

logging.basicConfig(level=logging.INFO,format="%(asctime)s,%(levelname)s,%(message)s")
log=logging.getLogger("bus-producer")

def topic_creation():
    try:
        admin=KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        admin.create_topics([NewTopic(TOPIC_NAME,replication_factor=1,num_partitions=3)])
        log.info("Topic created %s",TOPIC_NAME)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
def Get_From_Api():
    resp=requests.get(URL,params={"api_key":API_KEY},timeout=POLL_SECONDS)
    log.info("feed status = %s , bytes = %s ",resp.status_code,len(resp.content))
    if resp.status_code in (401,403):
        raise SystemExit("RESPONSE CODE (401,403)")
    resp.raise_for_status()
    feed=gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed
def byte_to_message(feed):
    pulled_at=feed.header.timestamp
    ingest_at=int(time.time())
    for e in feed.entity:
        v=e.vehicle
        yield{
            "vehicle_id":v.vehicle.id,
            "trip_id":v.trip.trip_id,
            "start_time":v.trip.start_time,
            "start_date":v.trip.start_date,
            "schedule_relationship":v.trip.schedule_relationship,
            "route_id":v.trip.route_id,
            "latitude":v.position.latitude,
            "longitude":v.position.longitude,
            "speed":v.position.speed,
            "pulled_at":pulled_at,
            "ingest_at":ingest_at,
            "event_ts":v.timestamp

    
        }    
def main():
    failed=0
    topic_creation()
    producer=KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k:k.encode(),
        value_serializer=lambda v:json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50
    )
    while True:
        feed=Get_From_Api()
        started_at=time.time()
        n=0
        try:
            for msg in byte_to_message(feed):
                producer.send(TOPIC_NAME,key=msg["vehicle_id"],value=msg)
            producer.flush()
            n+=1
            log.info("send %s messages ,messaged is %s aged",n,int(time.time()-feed.header.timestamp))
            failed=0
        except Exception as ex:
            failed+=1
            wait=100
            log.warning("What is the promblem:",ex)
            log.warning("Poll failed : %s ,Type of Promblem: %s ,Retry in : %s",failed,type(ex).__name__,wait)
            time.sleep(wait)
            continue
        time.sleep(max(1,POLL_SECONDS-(time.time() - started_at)))    


if __name__=="__main__":
    
    main()
                
