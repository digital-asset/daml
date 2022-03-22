// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.resources.TestResourceContext
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class Interfaces
    extends AsyncFlatSpec
    with SandboxTestLedger
    with Matchers
    with TestResourceContext
    with SuiteResourceManagementAroundAll {

  import TestUtil._

  behavior of "Generated Java code"

  it should "work" in withClient { client =>
    for {
      alice <- allocateParty
    } yield {
      sendCmd(client, alice, interfaces.Child.create(alice))
      val childs = readActiveContracts(interfaces.Child.Contract.fromCreatedEvent)(client, alice)
      childs.foreach { child =>
        sendCmd(client, alice, child.id.toTIf.exerciseHam(new interfaces.Ham()))
      }
      succeed
    }
  }
}
