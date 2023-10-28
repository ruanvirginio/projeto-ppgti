from dotenv import load_dotenv
# import libraries
import os
import sys
import argparse
import warnings

from objects.agent import Agent
from objects.credit_card import CreditCard
from objects.movies import Movies
from objects.musics import Musics
from objects.rides import Rides
from datastore.kafka.users import UsersAvro
from datastore.kafka.kafka_json import Kafka
from objects.users import Users

from schemas import sch_music_data, sch_agent, sch_users, sch_credit_card, sch_rides

# get env
load_dotenv()

# load variables
get_dt_rows = os.getenv("EVENTS")
kafka_broker = os.getenv("KAFKA_BOOTSTRAP_SERVER")
kafka_broker_ssl = os.getenv("KAFKA_BOOTSTRAP_SERVER_SSL")
confluent_cloud_kafka_broker = os.getenv("CONFLUENT_CLOUD_BOOTSTRAP_SERVER")
hdinsight_kafka_bootstrap_server = os.getenv("HDINSIGHT_KAFKA_BOOTSTRAP_SERVER")
users_json_topic = os.getenv("KAFKA_TOPIC_USERS_JSON")
credit_card_json_topic = os.getenv("KAFKA_TOPIC_CREDIT_CARD_JSON")
musics_json_topic = os.getenv("KAFKA_TOPIC_APP_MUSICS_JSON")
movies_titles_data_json_topic = os.getenv("KAFKA_TOPIC_APP_MOVIES_JSON")
movies_keywords_data_json_topic = os.getenv("KAFKA_TOPIC_APP_KEYWORDS_JSON")
movies_ratings_data_json_topic = os.getenv("KAFKA_TOPIC_APP_RATINGS_JSON")
rides_json_topic = os.getenv("KAFKA_TOPIC_APP_RIDES_JSON")
schema_registry_server = os.getenv("KAFKA_SCHEMA_REGISTRY")
kafka_topic_users_avro = os.getenv("KAFKA_TOPIC_USERS_AVRO")
kafka_topic_credit_card_avro = os.getenv("KAFKA_TOPIC_CREDIT_CARD_AVRO")
kafka_topic_agent_avro = os.getenv("KAFKA_TOPIC_AGENT_AVRO")
kafka_topic_musics_avro = os.getenv("KAFKA_TOPIC_APP_MUSICS_AVRO")
kafka_topic_movies_avro = os.getenv("KAFKA_TOPIC_APP_MOVIES_AVRO")
kafka_topic_rides_avro = os.getenv("KAFKA_TOPIC_APP_RIDES_AVRO")
kafka_topic_users_without_log_compaction_avro = os.getenv("KAFKA_TOPIC_USERS_WITHOUT_LOG_COMPACTION_AVRO")
kafka_topic_users_with_log_compaction_avro = os.getenv("KAFKA_TOPIC_USERS_WITH_LOG_COMPACTION_AVRO")
kafka_topic_users_without_log_compaction_json = os.getenv("KAFKA_TOPIC_USERS_WITHOUT_LOG_COMPACTION_JSON")
kafka_topic_users_with_log_compaction_json = os.getenv("KAFKA_TOPIC_USERS_WITH_LOG_COMPACTION_JSON")


# main
if __name__ == '__main__':

    # instantiate arg parse
    parser = argparse.ArgumentParser(description='python application for ingesting data & events into a data store')

    # add parameters to arg parse
    parser.add_argument('entity', type=str, choices=[
        'strimzi-users-json',
        'strimzi-users-json-ssl',
        'strimzi-users-without-log-compaction-json',
        'strimzi-users-with-log-compaction-json',
        'strimzi-agent-json',
        'strimzi-credit-card-json',
        'strimzi-musics-json',
        'strimzi-movies-titles-json',
        'strimzi-movies-keywords-json',
        'strimzi-movies-ratings-json',
        'strimzi-rides-json',
        'strimzi-users-avro',
        'strimzi-users-without-log-compaction-avro',
        'strimzi-users-with-log-compaction-avro',
        'strimzi-agent-avro',
        'strimzi-credit-card-avro',
        'strimzi-musics-avro',
        'strimzi-rides-avro',
        'mssql',
        'sqldb',
        'postgres',
        'ysql',
        'mysql',
        'mongodb',
        'ycql',
        'minio',
        'minio-movies',
    ], help='entities')

    # invoke help if null
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    # init variables
    users_object_name = Users().get_multiple_rows(get_dt_rows)
    # agent_object_name = Agent().get_multiple_rows(get_dt_rows)
    # credit_card_object_name = CreditCard().get_multiple_rows(get_dt_rows)
    # musics_object_name = Musics().get_multiple_rows(get_dt_rows)
    # movies_titles_object_name = Movies().get_movies(get_dt_rows)
    # movies_keywords_object_name = Movies().get_keywords(get_dt_rows)
    # movies_ratings_object_name = Movies().get_ratings(get_dt_rows)
    # rides_object_name = Rides().get_multiple_rows(get_dt_rows)

    # apache kafka ~ strimzi
    if sys.argv[1] == 'strimzi-users-json':
        Kafka().json_producer(broker=kafka_broker, object_name=users_object_name, kafka_topic=users_json_topic)

    if sys.argv[1] == 'strimzi-users-avro':
        schema_key = sch_users.key
        schema_value = sch_users.value
        UsersAvro().avro_producer(kafka_broker, schema_registry_server, schema_key, schema_value, kafka_topic_users_avro, get_dt_rows)