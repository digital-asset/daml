// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import akka.stream.scaladsl.Flow
import com.daml.lf.data.Ref._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.{value => LedgerApi}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.lf.engine.trigger.TriggerMsg

class DevOnly
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with SuiteResourceManagementAroundAll
    with TryValues {
  self: Suite =>

  this.getClass.getSimpleName can {
    "InterfaceTest" should {
      val triggerId = QualifiedName.assertFromString("Interface:test")
      val tId = LedgerApi.Identifier(packageId, "Interface", "Asset")
      "1 transfer" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create of Asset
          // 1 for completion
          // 1 for exercise
          // 1 for corresponding completion
          _ <- runner.runWithACS(acs, offset, msgFlow = Flow[TriggerMsg].take(4))._2
          acs <- queryACS(client, party)
        } yield {
          acs(tId) should have length 1
        }
      }
    }

  }
}
