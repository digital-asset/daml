// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.LocalInstanceReference

trait NodeTestingUtils {
  this: BaseTest =>

  protected def startAndWait(node: LocalInstanceReference) = {
    // ensure the node is not currently running
    eventually() {
      node.is_running shouldBe false
    }
    clue(s"starting ${node.name}") {
      node.start()
    }
    eventually() {
      node.is_initialized shouldBe true
      node.health.initialized() shouldBe true
    }
  }

  protected def stopAndWait(node: LocalInstanceReference) = {
    // ensure that the node is currently running normally
    eventually() {
      node.is_initialized shouldBe true
      node.health.initialized() shouldBe true
    }
    clue(s"stopping ${node.name}") {
      node.stop()
    }
    eventually() {
      node.is_running shouldBe false
    }
  }

}
