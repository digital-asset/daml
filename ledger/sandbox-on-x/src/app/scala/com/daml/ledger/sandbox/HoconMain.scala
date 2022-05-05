// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.runner.common.HoconCli

object HoconMain {
  def main(args: Array[String]): Unit =
    SandboxOnXRunner.run(HoconCli.loadConfigWithOverrides(SandboxOnXRunner.RunnerName, args))

}
