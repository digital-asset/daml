// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.resources.ResourceContext
import com.daml.resources.ProgramResource

object Main {
  def main(args: Array[String]): Unit =
    new ProgramResource(owner = SandboxOnXRunner.owner(args)).run(ResourceContext.apply)
}
