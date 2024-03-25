// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.resources.TestResourceContext
import io.grpc.Channel
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import stakeholders.{ExplicitObservers, OnlySignatories}

import scala.concurrent.Future

trait StakeholdersTest
    extends AsyncFlatSpecLike
    with Matchers
    with TestResourceContext
    with TestLedger
    with SuiteResourceManagementAroundAll {

  import TestUtil._

  def withUniqueParties(
      testCode: (
          String,
          OnlySignatories,
          ExplicitObservers,
          Channel,
      ) => Assertion
  ): Future[Assertion] = {
    for {
      List(alice, bob, charlie) <- Future.sequence(List.fill(3)(allocateParty))
      onlySignatories = new OnlySignatories(alice)
      explicitObservers = new ExplicitObservers(alice, bob)
      result <- withClient(
        testCode(alice, onlySignatories, explicitObservers, _)
      )
    } yield result
  }

  behavior of "Stakeholders"

  they should "be exposed correctly for contracts without observers" in withUniqueParties {
    (alice, onlySignatories, _, client) =>
      sendCmd(client, alice, onlySignatories.create())

      val contract :: _ =
        readActiveContracts(OnlySignatories.Contract.fromCreatedEvent)(client, alice)

      contract.signatories should contain only onlySignatories.owner
      contract.observers shouldBe empty
  }

  they should "be exposed correctly for contracts with only explicit observers" in withUniqueParties {
    (alice, _, explicitObservers, client) =>
      sendCmd(client, alice, explicitObservers.create())

      val contract :: _ =
        readActiveContracts(ExplicitObservers.Contract.fromCreatedEvent)(client, alice)

      contract.signatories should contain only explicitObservers.owner
      contract.observers should contain only explicitObservers.thirdParty
  }
}
