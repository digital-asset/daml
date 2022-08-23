// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

object Main {
  def main(args: Array[String]): Unit =
    CliSandboxOnXRunner.run(args)
}
