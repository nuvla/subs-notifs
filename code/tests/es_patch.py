from elasticsearch.client.utils import query_params
from elasticsearch.exceptions import RequestError


@query_params('consistency',
              'op_type',
              'parent',
              'refresh',
              'replication',
              'routing',
              'timeout',
              'timestamp',
              'ttl',
              'version',
              'version_type')
def es_update(self, index, _id, body, doc_type='_doc', params=None, headers=None):
    if index not in self._FakeElasticsearch__documents_dict:
        self._FakeElasticsearch__documents_dict[index] = []

    status, result, error = self._validate_action('update', index, _id, doc_type, params=params)

    if error:
        return status, result, error

    result: dict = None
    count: int = None
    for i, document in enumerate(self._FakeElasticsearch__documents_dict[index]):
        if document.get('_id') == _id:
            if doc_type == '_all':
                result = document
                count = i
                break
            else:
                if document.get('_type') == doc_type:
                    result = document
                    count = i
                    break
    result['_source'].update(body['doc'])
    result['_version'] += 1

    self._FakeElasticsearch__documents_dict[index][count] = result

    return result


@query_params('master_timeout', 'timeout')
def es_index_create(self, index, body=None, params=None, headers=None):
    documents_dict = self._FakeIndicesClient__get_documents_dict()
    if index not in documents_dict:
        documents_dict[index] = []
    else:
        raise RequestError(400, 'resource_already_exists_exception',
                           f'index [{index}] already exists')
