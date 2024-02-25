import os

ES_HOSTS = [{'host': 'es', 'port': 9200}]
KAFKA_TOPIC_SUBS_CONFIG = 'subscription-config'
KAFKA_TOPIC_NUVLAEDGES = 'nuvlabox'
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
    KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(',')
ES_HOSTS = [{'host': 'es', 'port': 9200}]
ES_INDEX_DELETED_ENTITIES = 'subsnotifs-deleted-entities'
ES_INDEX_RXTX = 'subsnotifs-rxtx'


def es_hosts():
    """
    Expected env var: ES_HOSTS=es1:9201,es2:92002
    :return:
    """
    if 'ES_HOSTS' in os.environ:
        es_conf = []
        for es in os.environ['ES_HOSTS'].split(','):
            host, port = es.split(':')
            es_conf.append({'host': host, 'port': int(port)})
        if es_conf:
            return es_conf
    return ES_HOSTS


def prometheus_exporter_port(default_port):
    return int(os.environ.get('PROMETHEUS_EXPORTER_PORT', default_port))
