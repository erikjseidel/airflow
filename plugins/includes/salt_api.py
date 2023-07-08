from __future__ import annotations

import logging, requests
from pprint import pprint
from airflow import AirflowException
from includes.secrets import SALT_MASTERS

log = logging.getLogger(__name__)

class SaltAPI:
    
    def __init__(self, master):
        url = SALT_MASTERS[master]['url'] + '/login'

        credentials = {
                'username' : SALT_MASTERS[master]['username'],
                'password' : SALT_MASTERS[master]['password'],
                'eauth'    : 'sharedsecret',
                }

        self.session = requests.Session()
        self.session.post(url, json=credentials)

        self.master = master


    def local_client(self, tgt, fun, saltargs=None, saltkwargs=None):
        url = SALT_MASTERS[self.master]['url']

        data = {
                'client' : 'local',
                'tgt'    : tgt,
                'fun'    : fun,
                'arg'    : saltargs,
                'kwarg'  : saltkwargs,
                }
        data = { k : v for k, v in data.items() if v }

        return self.session.post(url, json=data).json()['return'][0]


    def runner_client(self, fun, saltargs=None, saltkwargs=None):
        url = SALT_MASTERS[self.master]['url']

        data = {
                'client' : 'runner',
                'fun'    : fun,
                'arg'    : saltargs,
                'kwarg'  : saltkwargs,
                }
        data = { k : v for k, v in data.items() if v }

        return self.session.post(url, json=data).json()['return'][0]


def salt_api_task(func):
    """
    Stacked below '@task' python operator decorator. Post-processing for salt
    api call returns.

    Perhaps this should be refactored as a full operator?
    """
    def decorator(*args, **kwargs):
        ret = func(*args, **kwargs)

        pprint(ret)

        if not ret['result']:
            raise AirflowException(ret['comment'])

        return "Salt Task complete"

    return decorator
