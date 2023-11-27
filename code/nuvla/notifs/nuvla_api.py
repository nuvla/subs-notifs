import logging
import os
import sys

from nuvla.api import Api

NUVLA_API_ENDPOINT = os.environ.get('NUVLA_API_ENDPOINT', 'http://api:8200')
NUVLA_API_AUTHN_HEADER = os.environ.get('NUVLA_API_AUTHN_HEADER', 'group/nuvla-admin')
NUVLA_API_INSECURE = os.environ.get('NUVLA_API_INSECURE') in ['True', 'true']


def init_nuvla_api(api_url=NUVLA_API_ENDPOINT,
                   api_authn_header=NUVLA_API_AUTHN_HEADER,
                   api_insecure=NUVLA_API_INSECURE,
                   api_key: str = None, api_secret: str = None,
                   api_user: str = None, api_pass: str = None) -> Api:
    reauthenticate = api_authn_header is None
    api = Api(endpoint=api_url, insecure=api_insecure,
              persist_cookie=False, reauthenticate=reauthenticate,
              authn_header=api_authn_header)
    if api_authn_header is not None:
        return api
    try:
        if api_key and api_secret:
            response = api.login_apikey(api_key, api_secret)
        elif api_user and api_pass:
            response = api.login_password(api_user, api_pass)
        else:
            logging.error('No valid credential to login to Nuvla!')
            sys.exit(1)
        if response.status_code == 403:
            raise ConnectionError(
                f'Login with following user/apikey {api_user} failed!')
        # Uncomment following lines for manual remote test
        # session_id = api.current_session()
        # api.operation(api.get(session_id), 'switch-group',
        #               {'claim': "group/nuvla-admin"})
        return api
    except ConnectionError as ex:
        logging.error('Unable to connect to Nuvla endpoint %s! %s',
                      api.endpoint, ex)
        sys.exit(1)
