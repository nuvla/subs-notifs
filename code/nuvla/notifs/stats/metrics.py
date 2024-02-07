from prometheus_client import Summary, Counter, Gauge, \
    Histogram, Info, Enum

PROCESSING_TIME = Gauge('message_processing_time', 'Time spent in processing of a telemetry packet or event',
                        ['type'])

PACKETS_PROCESSED = Counter('packets_processed', 'Number of telemetry packets processed', ['type', 'id'])
PACKETS_ERROR = Counter('packets_error', 'Number of telemetry packets processed with error',
                        ['type', 'id', 'exception'])

SUBSCRIPTION_CONFIGS = Gauge('subscription_configs', 'Number of subscription configurations', ['metric'])

PROCESS_STATES = Enum('process_states', 'State of the process',
                      states=['idle', 'processing', 'error - recoverable', 'error - need restart'])

NOTIFICATIONS_SENT = Counter('notifications_sent', 'Number of notifications sent', ['type', 'subscription_id'])
NOTIFICATIONS_ERROR = Counter('notifications_error', 'Number of notifications that could not be sent due to error',
                              ['type', 'subscription_id', 'exception'])

SUBSCRIPTIONS_CONFIGS = Gauge('subscriptions_configs', 'Subscription configurations')

ES_INDEX_DOCS_DELETED = Counter('es_index_docs_deleted', 'Number of documents deleted from an index',
                                ['index', 'id'])
