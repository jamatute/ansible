#!/usr/bin/python
# elasticsearch_snapshot: Do snapshots of elasticsearch indices
#
# Copyright (C) 2017 jamatute <jamatute@paradigmadigital.com>
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

ANSIBLE_METADATA = {'metadata_version': '1.0',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: elasticsearch_snapshot
short_description: Do snapshots of elasticsearch indices
description: Do snapshots of elasticsearch indices
version_added: '2.5'
requirements:
  - python
  - requests
options:
    state:
        description:
          - If the state is 'present' it will create the snapshot, if it's
            'absent' it will delete the snapshot
        required: true
        choices: ['present', 'absent']
        default: present
    name:
        description:
          - Name of the snapshot
        required: true
    indices:
        description:
          - Create a snapshot of just the selected indices
        required: false
    snapshot_repository_url:
        description:
          - url to the snapshot repository (not just the elasticsearch)
        required: true
    timeout:
        description:
          - Timeout of the snapshot process in seconds
        default: 900
        required: false
    retries:
        description:
          - Number of retries of failed http/https requests
        default: 3
        required: false
    sleep:
        description:
          - Sleep between checks of the snapshot process
        default: 15
        required: false
author: jamatute (@jamatute)
'''

EXAMPLES = '''
- name: Create snapshot of all the indices
  modulename:
    name: elasticsearch_snapshot
    state: present
    name: full-snapshot
    snapshot_repository_url: http://localhost:9200/_snapshot/backups

- name: Create snapshot of all the indices with a specified timeout
  modulename:
    name: elasticsearch_snapshot
    state: present
    name: full-snapshot
    timeout: 600
    snapshot_repository_url: http://localhost:9200/_snapshot/backups

- name: Create snapshot of an index
  modulename:
    name: elasticsearch_snapshot
    state: present
    name: one-index-snapshot
    indices: really-interesting-index
    snapshot_repository_url: http://localhost:9200/_snapshot/backups

- name: Create snapshot of a list of  indices
  modulename:
    name: elasticsearch_snapshot
    state: present
    name: two-index-snapshot
    indices:
        - really-interesting-index
        - another-interesting-index
    snapshot_repository_url: http://localhost:9200/_snapshot/backups

- name: Delete snapshot of an index
  modulename:
    name: elasticsearch_snapshot
    state: absent
    name: one-index-snapshot
    indices: really-interesting-index
    snapshot_repository_url: http://localhost:9200/_snapshot/backups
'''

RETURN = ''' # '''


import time
import json
import requests
import datetime
from ansible.module_utils.basic import AnsibleModule


def snapshot_already_exists(url):
    "Check if the snapshot already exists"

    result = requests.get(url)

    if result.status_code == 200:
        return True
    else:
        return False

def snapshot_status(url):
    "Check if the snapshot already exists"

    try:
        result = requests.get(url)
    except Exception as e:
        raise

    if result.status_code == 200:
        return json.loads(result.text)['snapshots'][0]['state']
    else:
        raise requests.exceptions.ConnectionError('Return code not 200')



def create_snapshot(data):
    "Create Elasticsearch snapshot"

    snapshot_url = '/'.join([data['snapshot_repository_url'], data['name']])

    if snapshot_already_exists(snapshot_url):
        return False, False, {"msg": 'snapshot already exists'}

    try:
        if data['indices'] is None:
            payload = {"ignore_unavailable": True,
                       "include_global_state": False}
        else:
            payload = {"indices": data['indices'],
                       "ignore_unavailable": True,
                       "include_global_state": False}
    except KeyError:
        payload = {"ignore_unavailable": True,
                   "include_global_state": False}

    start_timestamp = datetime.datetime.now()
    try:
        result = requests.put(snapshot_url, json=payload,
                              timeout=data['timeout'])
    except requests.exceptions.ConnectionError as e:
        return True, False, {"status": result.status_code,
                             "data": result.json()}

    retries = 0
    while True:
        time.sleep(data['sleep'])
        stop_timestamp = datetime.datetime.now()
        if (stop_timestamp - start_timestamp).total_seconds() > data['timeout']:
            return True, True, \
                {"msg": 'The job timed out, probably the snapshot' +
                 'process goes on'}
        try:
            status = snapshot_status(snapshot_url)
        except Exception as e:
            retries+=1
            if retries >= data['retries']:
                return True, True, \
                    {"msg": 'We reached the maximum number of retries,' +
                     'but probably the snapshot process goes on'}
        if status == 'SUCCESS':
            return False, True, {"status": result.status_code,
                                 "data": result.json()}


def delete_snapshot(data):
    "Delete Elasticsearch snapshot"

    snapshot_url = '/'.join([data['snapshot_repository_url'], data['name']])
    if not snapshot_already_exists(snapshot_url):
        return False, False, {"msg": "snapshot doesn't exists"}

    try:
        result = requests.delete(snapshot_url)
    except:
        return True, False, {"status": result.status_code,
                             "data": result.json()}

    return False, True, {"status": result.status_code, "data": result.json()}


def main():
    choice_map = {
      "present": create_snapshot,
      "absent": delete_snapshot
    }

    module = AnsibleModule(
       argument_spec=dict(
           state=dict(default='present', choices=['present', 'absent']),
           name=dict(required=True, type='str'),
           indices=dict(required=False, type='list'),
           timeout=dict(required=False, default=900, type='int'),
           retries=dict(required=False, default=3, type='int'),
           sleep=dict(required=False, default=15, type='int'),
           snapshot_repository_url=dict(required=True, type='str')))

    is_error, has_changed, result = choice_map.get(
        module.params['state'])(module.params)

    if is_error:
        module.fail_json(msg='Error processing snapshot', meta=result)
    else:
        module.exit_json(changed=has_changed, meta=result)

if __name__ == '__main__':
    main()
