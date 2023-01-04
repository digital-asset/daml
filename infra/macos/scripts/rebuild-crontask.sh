#!/bin/bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

date

PATH=/usr/local/bin:$PATH

USERNAME=<User-login-to-Azure>
USER_PAT=<user-PAT-Token-with-job-access-rights>

POOL_ID='10' #macOS-pool
BUILDER_NAME="$HOSTNAME"
BUILDER_ID=`curl -s -u $USERNAME:$USER_PAT "https://dev.azure.com/digitalasset/_apis/distributedtask/pools/$POOL_ID/agents?agentName=$BUILDER_NAME&api-version=5.1" | /usr/local/bin/jq '.value[0].id'`
REQUEST_ID=''

while :
do
   REQUEST_ID=`curl -s -u $USERNAME:$USER_PAT "https://dev.azure.com/digitalasset/_apis/distributedtask/pools/$POOL_ID/agents/$BUILDER_ID/?includeAssignedRequest=true&includeLastCompletedRequest=true&api-version=5.1" | /usr/local/bin/jq .assignedRequest.requestId`
   if [ "$REQUEST_ID" != "" ] && [ "$REQUEST_ID" != "null" ] ; then
      echo "Currently job running - $REQUEST_ID"
   else
      echo "No Job running - replacing agent"
      cd /Users/builder/daml/infra/macos/3-running-box
      vagrant destroy -f
      vagrant box remove azure-ci-node

      /Users/builder/run-agent.sh
      exit 0
   fi
   sleep 120
done

