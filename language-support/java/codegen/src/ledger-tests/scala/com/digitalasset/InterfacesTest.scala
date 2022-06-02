// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.javaapi.data.{CreatedEvent, Identifier}
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

  it should "contain all choices of an interface in templates implementing it" in withClient {
    client =>
      def checkTemplateId[T](
          shouldBeId: Identifier,
          fn: CreatedEvent => T,
      ): PartialFunction[CreatedEvent, T] = {
        case event: CreatedEvent if event.getTemplateId == shouldBeId => fn(event)
      }
      val safeChildFromCreatedEvent =
        checkTemplateId(interfaces.Child.TEMPLATE_ID, interfaces.Child.Contract.fromCreatedEvent)
      val safeChildCloneFromCreatedEvent =
        checkTemplateId(
          interfaces.ChildClone.TEMPLATE_ID,
          interfaces.ChildClone.Contract.fromCreatedEvent,
        )
      for {
        alice <- allocateParty
      } yield {
        sendCmd(client, alice, interfaces.Child.create(alice))
        sendCmd(client, alice, interfaces.ChildClone.create(alice))
        readActiveContractsSafe[interfaces.Child.Contract](safeChildFromCreatedEvent)(
          client,
          alice,
        ).foreach { child =>
          sendCmd(
            client,
            alice,
            child.id.toInterface(interfaces.TIf.INTERFACE).exerciseHam(new interfaces.Ham()),
          )
        }
        readActiveContractsSafe(safeChildFromCreatedEvent)(client, alice).foreach { child =>
          sendCmd(
            client,
            alice,
            child.id.toInterface(interfaces.TIf.INTERFACE).exerciseHam(new interfaces.Ham()),
          )
        }
        readActiveContractsSafe(safeChildCloneFromCreatedEvent)(client, alice)
          .foreach { child =>
            assertThrows[Exception](
              sendCmd(
                client,
                alice,
                interfaces.Child.ContractId
                  .unsafeFromInterface(
                    child.id.toInterface(interfaces.TIf.INTERFACE): interfaces.TIf.ContractId
                  )
                  .toInterface(interfaces.TIf.INTERFACE)
                  .exerciseHam(new interfaces.Ham()),
              )
            )
          }
        succeed
      }
  }
}
