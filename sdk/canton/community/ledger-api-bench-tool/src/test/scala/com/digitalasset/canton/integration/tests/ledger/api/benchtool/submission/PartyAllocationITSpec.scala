// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.BenchtoolSandboxFixture
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.AppendedClues

class PartyAllocationITSpec extends BenchtoolSandboxFixture with AppendedClues {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  "benchtool" should {
    "allow parties to be reused" onlyRunWithOrGreaterThan ProtocolVersion.dev in { env =>
      import env.*

      val config = WorkflowConfig.FooSubmissionConfig(
        numberOfInstances = 0,
        numberOfObservers = 1,
        numberOfDivulgees = 1,
        numberOfExtraSubmitters = 1,
        uniqueParties = false,
        instanceDistribution = Nil,
        nonConsumingExercises = None,
        consumingExercises = None,
        userIds = List.empty,
      )

      (for {
        (_, _, submitter) <- benchtoolFixture()
        parties1 <- submitter.prepare(config)
        parties2 <- submitter.prepare(config)
      } yield {
        parties1 shouldBe parties2
      }).futureValue
    }
  }
}
