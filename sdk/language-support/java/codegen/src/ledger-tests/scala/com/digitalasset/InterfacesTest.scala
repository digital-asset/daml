// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.javaapi.data.CreatedEvent
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.resources.TestResourceContext
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

trait InterfacesTest
    extends AsyncFlatSpecLike
    with TestLedger
    with Matchers
    with TestResourceContext
    with SuiteResourceManagementAroundAll {

  import TestUtil._

  behavior of "Generated Java code"

  it should "contain all choices of an interface in templates implementing it" in withClient {
    client =>
      def checkTemplateId[T](
          companion: ContractCompanion[T, _, _]
      ): PartialFunction[CreatedEvent, T] = {
        case event if event.getTemplateId == companion.TEMPLATE_ID =>
          companion fromCreatedEvent event
      }
      val safeChildFromCreatedEvent =
        checkTemplateId(interfaces.Child.COMPANION)
      val safeChildCloneFromCreatedEvent =
        checkTemplateId(interfaces.ChildClone.COMPANION)
      for {
        alice <- allocateParty
      } yield {
        sendCmd(client, alice, interfaces.Child.create(alice))
        sendCmd(client, alice, interfaces.ChildClone.create(alice))
        readActiveContractsSafe(safeChildFromCreatedEvent)(client, alice).foreach { child =>
          sendCmd(
            client,
            alice,
            child.id
              .toInterface(interfaces.TIf.INTERFACE)
              .exerciseHam(new interfaces.Ham()),
          )
        }
        readActiveContractsSafe(safeChildCloneFromCreatedEvent)(client, alice)
          .foreach { child =>
            val update = interfaces.Child.ContractId
              .unsafeFromInterface(
                child.id.toInterface(interfaces.TIf.INTERFACE): interfaces.TIf.ContractId
              )
              .exerciseBar()
            val ex = the[io.grpc.StatusRuntimeException] thrownBy sendCmd(client, alice, update)
            ex.getMessage should include regex "Expected contract of type .*Child@.* but got .*ChildClone"
          }
        succeed
      }
  }
}
