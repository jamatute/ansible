#!/usr/bin/python3
# elasticsearch_snapshot: Do snapshots of elasticsearch indices
#
# Copyright (C) 2017 Lyz <lyz@riseup.net>
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
  - "python >= 3.0"
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
author: Lyz (@lyz-code)
'''

EXAMPLES = '''
- name: Create snapshot of all the indices
  modulename:
    name: elasticsearch_snapshot
    state: present
    name: full-snapshot
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


import requests
from ansible.module_utils.basic import AnsibleModule


def snapshot_already_exists(url):
    "Check if the snapshot already exists"

    result = requests.get(url)

    if result.status_code == 200:
        return True
    else:
        return False


def create_snapshot(data):
    "Create Elasticsearch snapshot"

    snapshot_url = '/'.join([data['snapshot_repository_url'], data['name']])

    if snapshot_already_exists(snapshot_url):
        return False, False, {"msg": 'snapshot already exists'}

    snapshot_url = snapshot_url + '?wait_for_completion=true'

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
    try:
        result = requests.put(snapshot_url, json=payload)
    except:
        return True, False, {"status": result.status_code,
                             "data": result.json()}
    return False, True, {"status": result.status_code, "data": result.json()}


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
           snapshot_repository_url=dict(required=True, type='str')))

    is_error, has_changed, result = choice_map.get(
        module.params['state'])(module.params)

    if is_error:
        module.fail_json(msg='Error processing snapshot', meta=result)
    else:
        module.exit_json(changed=has_changed, meta=result)

if __name__ == '__main__':
    main()
