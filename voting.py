import random
import time
from datetime import datetime
import psycopg2
from confluent_kafka import  Consumer , KafkaException , KafkaError , SerializingProducer
import simplejson as json

conf = {
    'bootstrap.servers' : 'localhost:9092'
}
consumer = Consumer( conf | {
    'group.id' : 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit' : False
} )

producer = SerializingProducer(conf)

if __name__ == "__main__" :
    conn = psycopg2.connect("host=localhost dbname=voting_system user=user password=password")
    cur = conn.cursor()

    candidate_query = cur.execute(
        """
        select row_to_json(col) from (select * from candidates) col ;
        """
    )
    candidates = [candidate [0] for candidate in  cur.fetchall()]
    if len(candidates) == 0 :
        raise Exception ("No candidates found in the database")
    else :
        print(candidates)

    consumer.subscribe(['voters_topic'])
    try :
        while True :
            msg = Consumer.poll(timeout=1.0)
            if msg is None :
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF :
                    continue
                else :
                    print(msg.error())
                    break
            else :
                voter = json.loads(msg.value.decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    'voting_time' :datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                        'time': 1
                }
                try :
                    print(f"user {vote['voter_id']} is voting for candidate {vote['candidate_id']}")
                    cur.execute("""
                    insert into votes (voter_id,candidate_id,voting_time) values (%s,%s,%s)
                    """ ,(vote['voter_id'],vote['candidate_id'],vote['voting_time']))
                    conn.commit()

                    producer.produce(
                        topic='votes_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote)
                    )
                    producer.poll(0)
                except Exception as e :
                    print('Error',e)
            time.sleep(0.5)
    except Exception as e :
        print(e)

