#! /usr/bin/env nix-shell
#! nix-shell -i python3 .
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# This script purges agents older than 25 hours in a given pool
# Pass VSTS_{ACCOUNT,POOL,TOKEN} as environment variables to it
import datetime
import os
import sys

from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication

CUTOFF_HOURS=25

VSTS_ACCOUNT = os.environ["VSTS_ACCOUNT"]
VSTS_POOL = os.environ["VSTS_POOL"]
VSTS_TOKEN = os.environ["VSTS_TOKEN"]

credentials = BasicAuthentication('', VSTS_TOKEN)
connection = Connection(base_url="https://dev.azure.com/" + VSTS_ACCOUNT,
                        creds=credentials)

agent_client = connection.clients_v5_1.get_task_agent_client()

# retrieve pool id from name
pool_id = next(filter(lambda x: x.name == VSTS_POOL, agent_client.get_agent_pools())).id

for agent in agent_client.get_agents(pool_id):
    # agents should be killed after 24 hours max
    cutoff_time = datetime.datetime.now(datetime.timezone.utc)\
                  - datetime.timedelta(hours=CUTOFF_HOURS)
    if agent.created_on < cutoff_time:
        if agent.status == 'offline':
            print("cleaning up agent #{} ({})".format(agent.id, agent.name))
            agent_client.delete_agent(pool_id, agent.id)
        else:
            print("agent still online: #{} ({})".format(agent.id, agent.name),
                  file=sys.stderr)
    else:
        print("skipping agent #{} ({}), too new".format(agent.id, agent.name))
