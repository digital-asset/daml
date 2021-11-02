// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.resources.{ResourceContext, TestResourceContext}
import io.grpc.Channel
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import stakeholders.{ExplicitObservers, ImplicitObservers, MixedObservers, OnlySignatories}

import scala.concurrent.Future

class StakeholdersTest
    extends AsyncFlatSpec
    with Matchers
    with TestResourceContext
    with SandboxFixture
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
  )(implicit resourceContext: ResourceContext): Future[Assertion] = {
    val List(alice, bob, charlie) = List(Alice, Bob, Charlie).map(getUniqueParty)
    val onlySignatories = new OnlySignatories(alice)
    val implicitObservers = new ImplicitObservers(alice, bob)
    val explicitObservers = new ExplicitObservers(alice, bob)
    val mixedObservers = new MixedObservers(alice, bob, charlie)
    withClient(
      testCode(alice, onlySignatories, implicitObservers, explicitObservers, mixedObservers, _)
    )
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
