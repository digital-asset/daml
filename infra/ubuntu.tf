# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

locals {
  ubuntu = {
    gcp = [
      {
        name        = "ci-u1",
        disk_size   = 400,
        size        = 0,
        assignment  = "default",
        start_agent = <<AGENT
su --login vsts <<RUN
cd /home/vsts/agent

trap "./config.sh remove --auth PAT --unattended --token {vsts_token}" EXIT

./run.sh
RUN
AGENT
      },
      {
        name        = "ci-u2",
        disk_size   = 400,
        size        = 0,
        assignment  = "default",
        start_agent = <<AGENT
su --login vsts <<RUN
cd /home/vsts/agent

trap "./config.sh remove --auth PAT --unattended --token {vsts_token}" EXIT

./run.sh
RUN
AGENT
      },
    ],
    azure = [
      {
        name        = "du1",
        disk_size   = 400,
        size        = 15,
        assignment  = "default",
        start_agent = "su --login --command \"cd /home/vsts/agent && exec ./run.sh\" - vsts"
      },
      {
        name        = "du2",
        disk_size   = 400,
        size        = 15,
        assignment  = "default",
        start_agent = <<AGENT
su --login vsts <<RUN
cd /home/vsts/agent

trap "./config.sh remove --auth PAT --unattended --token {vsts_token}" EXIT

./run.sh
RUN
AGENT
      },

    ]
  }
}
