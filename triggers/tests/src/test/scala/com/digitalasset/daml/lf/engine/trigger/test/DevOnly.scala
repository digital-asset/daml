// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import akka.stream.scaladsl.Flow
import com.daml.lf.data.Ref._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.event.{CreatedEvent, Event}
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.api.v1.{value => LedgerApi}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.lf.engine.trigger.{TransactionMsg, TriggerMsg}

class DevOnly
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with SuiteResourceManagementAroundAll
    with TryValues {
  self: Suite =>

  import DevOnly._

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

    "trigger runner" should {
      val templateA = Identifier(packageId, "InterfaceTriggers", "A")
      val templateB = Identifier(packageId, "InterfaceTriggers", "B")

      "succeed with all templates and interfaces registered" in {
        val triggerId = QualifiedName.assertFromString("InterfaceTriggers:globalTrigger")

        var triggerMessages = Seq.empty[TriggerMsg]

        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)

          // Determine current ledger offset
          queryResult <- runner.queryACS()
          (_, offset) = queryResult

          _ <- create(
            client,
            party,
            CreateCommand(
              templateId = Some(templateA),
              createArguments = Some(
                Record(
                  fields = Seq(
                    RecordField("owner", Some(Value().withParty(party))),
                    RecordField("tag", Some(Value().withText("visible via 'AllInDar'"))),
                  )
                )
              ),
            ),
          )
          _ <- create(
            client,
            party,
            CreateCommand(
              templateId = Some(templateB),
              createArguments = Some(
                Record(
                  fields = Seq(
                    RecordField("owner", Some(Value().withParty(party))),
                    RecordField("tag", Some(Value().withText("visible via 'AllInDar'"))),
                  )
                )
              ),
            ),
          )

          // Determine ACS for this test run's setup
          queryResult <- runner.queryACS()
          (acs, _) = queryResult

          // 1 for ledger create command completion
          // 1 for ledger create command completion
          // 1 for transactional create of template A
          // 1 for transactional create of template B
          _ <- runner
            .runWithACS(
              acs,
              offset,
              msgFlow = Flow[TriggerMsg]
                .map { msg => triggerMessages = triggerMessages :+ msg; msg }
                .take(4),
            )
            ._2
          acs <- queryACS(client, party)
        } yield {
          triggerMessages.size shouldBe 4
          triggerMessages(2) shouldHaveCreateArgumentsFor templateA
          triggerMessages(3) shouldHaveCreateArgumentsFor templateB

          acs(templateA) should have length 1
          acs(templateB) should have length 1
        }
      }

      "succeed with interface registration and implementing template not registered" in {
        val triggerId = QualifiedName.assertFromString("InterfaceTriggers:triggerWithRegistration")

        var triggerMessages = Seq.empty[TriggerMsg]

        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)

          // Determine current ledger offset
          queryResult <- runner.queryACS()
          (_, offset) = queryResult

          _ <- create(
            client,
            party,
            CreateCommand(
              templateId = Some(templateA),
              createArguments = Some(
                Record(
                  fields = Seq(
                    RecordField("owner", Some(Value().withParty(party))),
                    RecordField(
                      "tag",
                      Some(Value().withText("visible via 'registeredTemplate @A'")),
                    ),
                  )
                )
              ),
            ),
          )
          _ <- create(
            client,
            party,
            CreateCommand(
              templateId = Some(templateB),
              createArguments = Some(
                Record(
                  fields = Seq(
                    RecordField("owner", Some(Value().withParty(party))),
                    RecordField(
                      "tag",
                      Some(Value().withText("visible via 'registeredTemplate @I'")),
                    ),
                  )
                )
              ),
            ),
          )

          // Determine ACS for this test run's setup
          queryResult <- runner.queryACS()
          (acs, _) = queryResult

          // 1 for ledger create command completion
          // 1 for ledger create command completion
          // 1 for transactional create of template A
          // 1 for transactional create of template B, via interface I
          _ <- runner
            .runWithACS(
              acs,
              offset,
              msgFlow = Flow[TriggerMsg]
                .map { msg => triggerMessages = triggerMessages :+ msg; msg }
                .take(4),
            )
            ._2
        } yield {
          triggerMessages.size shouldBe 4
          triggerMessages(2) shouldHaveCreateArgumentsFor templateA
          triggerMessages(3) shouldHaveNoCreateArgumentsFor templateB
        }
      }
    }
  }
}

object DevOnly extends Matchers with Inside {

  implicit class TriggerMsgTestHelper(msg: TriggerMsg) {
    def shouldHaveNoCreateArgumentsFor(templateId: Identifier): Assertion =
      inside(msg) { case TransactionMsg(transaction) =>
        transaction.events.size shouldBe 1
        inside(transaction.events.head) {
          case Event(
                Created(CreatedEvent(_, _, Some(`templateId`), _, None, _, Seq(_), _, _, _, _, _))
              ) =>
            succeed
        }
      }

    def shouldHaveCreateArgumentsFor(templateId: Identifier): Assertion =
      inside(msg) { case TransactionMsg(transaction) =>
        transaction.events.size shouldBe 1
        inside(transaction.events.head) {
          case Event(
                Created(
                  CreatedEvent(_, _, Some(`templateId`), _, Some(_), _, Seq(_), _, _, _, _, _)
                )
              ) =>
            succeed
        }
      }
  }
}
