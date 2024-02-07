from prometheus_client import Counter, Gauge, Enum

namespace = 'subs-notifs'

PROCESSING_TIME = Gauge(f'{namespace}_message_processing_time', 'Time spent in processing of a telemetry packet or '
                                                                'event',
                        ['type'])

PACKETS_PROCESSED = Counter(f'{namespace}_packets_processed', 'Number of telemetry packets processed',
                            ['type', 'id'])
PACKETS_ERROR = Counter(f'{namespace}_packets_error', 'Number of telemetry packets processed with error',
                        ['type', 'id', 'exception'])

SUBSCRIPTION_CONFIGS = Gauge(f'{namespace}_subscription_configs', 'Number of subscription configurations',
                             ['type'])

PROCESS_STATES = Enum(f'{namespace}_process_states', 'State of the process',
                      states=['idle', 'processing', 'error - recoverable', 'error - need restart'])

ES_INDEX_DOCS_DELETED = Counter(f'{namespace}_es_index_docs_deleted', 'Number of documents deleted from an index',
                                ['index', 'id'])
