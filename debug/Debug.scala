// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundAll}
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import com.daml.platform.sandboxnext.SandboxNextFixture


class Debug
    extends AsyncFreeSpec
    with AkkaBeforeAndAfterAll
    with BeforeAndAfterEach
    with SuiteResourceManagementAroundAll
    with SandboxNextFixture {
  "a" - {
    "b" in {
      succeed
    }
  }
}
