from prometheus_client import Counter, Gauge, Enum
import __main__

if __main__.__file__.__contains__('monitoring'):
    namespace = 'subs_notifs_monitoring'
else:
    namespace = 'subs_notifs'

PROCESSING_TIME = Gauge('message_processing_time', 'Time spent in processing of a telemetry packet or '
                                                                'event',
                        ['type', 'message_info'], namespace=namespace)

PACKETS_PROCESSED = Counter('packets_processed', 'Number of telemetry packets processed',
                            ['type', 'id'], namespace=namespace)
PACKETS_ERROR = Counter('packets_error', 'Number of telemetry packets processed with error',
                        ['type', 'id', 'exception'], namespace=namespace)

SUBSCRIPTION_CONFIGS = Gauge('subscription_configs', 'Number of subscription configurations',
                             ['type'], namespace=namespace)

PROCESS_STATES = Enum('process_states', 'State of the process',
                      states=['idle', 'processing', 'error - recoverable', 'error - need restart'],
                      namespace=namespace)

ES_INDEX_DOCS_DELETED = Counter('es_index_docs_deleted', 'Number of documents deleted from an index',
                                ['index', 'id'], namespace=namespace)
