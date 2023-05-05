// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.resources.TestResourceContext
import io.grpc.Channel
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import stakeholders.{ExplicitObservers, ImplicitObservers, MixedObservers, OnlySignatories}

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
          ImplicitObservers,
          ExplicitObservers,
          MixedObservers,
          Channel,
      ) => Assertion
  ): Future[Assertion] = {
    for {
      List(alice, bob, charlie) <- Future.sequence(List.fill(3)(allocateParty))
      onlySignatories = new OnlySignatories(alice)
      implicitObservers = new ImplicitObservers(alice, bob)
      explicitObservers = new ExplicitObservers(alice, bob)
      mixedObservers = new MixedObservers(alice, bob, charlie)
      result <- withClient(
        testCode(alice, onlySignatories, implicitObservers, explicitObservers, mixedObservers, _)
      )
    } yield result
  }

  behavior of "Stakeholders"

  they should "be exposed correctly for contracts without observers" in withUniqueParties {
    (alice, onlySignatories, _, _, _, client) =>
      sendCmd(client, alice, onlySignatories.create())

      val contract :: _ =
        readActiveContracts(OnlySignatories.Contract.fromCreatedEvent)(client, alice)

      contract.signatories should contain only onlySignatories.owner
      contract.observers shouldBe empty
  }

  they should "be exposed correctly for contracts with only implicit observers" in withUniqueParties {
    (alice, _, implicitObservers, _, _, client) =>
      sendCmd(client, alice, implicitObservers.create())

      val contract :: _ =
        readActiveContracts(ImplicitObservers.Contract.fromCreatedEvent)(client, alice)

      contract.signatories should contain only implicitObservers.owner
      contract.observers should contain only implicitObservers.thirdParty
  }

  they should "be exposed correctly for contracts with only explicit observers" in withUniqueParties {
    (alice, _, _, explicitObservers, _, client) =>
      sendCmd(client, alice, explicitObservers.create())

      val contract :: _ =
        readActiveContracts(ExplicitObservers.Contract.fromCreatedEvent)(client, alice)

      contract.signatories should contain only explicitObservers.owner
      contract.observers should contain only explicitObservers.thirdParty
  }

  they should "be exposed correctly for contracts with both implicit and explicit observers" in withUniqueParties {
    (alice, _, _, _, mixedObservers, client) =>
      sendCmd(client, alice, mixedObservers.create())

      val contract :: _ =
        readActiveContracts(MixedObservers.Contract.fromCreatedEvent)(client, alice)

      contract.signatories should contain only mixedObservers.owner
      contract.observers should contain.only(mixedObservers.thirdParty1, mixedObservers.thirdParty2)
  }

}
