// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.resources.TestResourceContext
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import stakeholders.{ExplicitObservers, ImplicitObservers, MixedObservers, OnlySignatories}

class StakeholdersTest extends AsyncFlatSpec with Matchers with TestResourceContext {

  import TestUtil._

  val onlySignatories = new OnlySignatories(Alice)
  val implicitObservers = new ImplicitObservers(Alice, Bob)
  val explicitObservers = new ExplicitObservers(Alice, Bob)
  val mixedObservers = new MixedObservers(Alice, Bob, Charlie)

  behavior of "Stakeholders"

  they should "be exposed correctly for contracts without observers" in withClient { client =>
    sendCmd(client, onlySignatories.create())

    val contract :: _ = readActiveContracts(OnlySignatories.Contract.fromCreatedEvent)(client)

    contract.signatories should contain only onlySignatories.owner
    contract.observers shouldBe empty
  }

  they should "be exposed correctly for contracts with only implicit observers" in withClient {
    client =>
      sendCmd(client, implicitObservers.create())

      val contract :: _ = readActiveContracts(ImplicitObservers.Contract.fromCreatedEvent)(client)

      contract.signatories should contain only implicitObservers.owner
      contract.observers should contain only implicitObservers.thirdParty
  }

  they should "be exposed correctly for contracts with only explicit observers" in withClient {
    client =>
      sendCmd(client, explicitObservers.create())

      val contract :: _ = readActiveContracts(ExplicitObservers.Contract.fromCreatedEvent)(client)

      contract.signatories should contain only explicitObservers.owner
      contract.observers should contain only explicitObservers.thirdParty
  }

  they should "be exposed correctly for contracts with both implicit and explicit observers" in withClient {
    client =>
      sendCmd(client, mixedObservers.create())

      val contract :: _ = readActiveContracts(MixedObservers.Contract.fromCreatedEvent)(client)

      contract.signatories should contain only mixedObservers.owner
      contract.observers should contain only (mixedObservers.thirdParty1, mixedObservers.thirdParty2)
  }

}
