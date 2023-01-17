// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import akka.stream.scaladsl.Flow
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.event.{Event => ApiEvent}
import com.daml.ledger.api.v1.transaction.{Transaction => ApiTransaction}
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.engine.trigger.Runner.TriggerContext
import com.daml.lf.engine.trigger.{
  InFlightCommandOverflowException,
  TriggerMsg,
  TriggerRunnerConfig,
}
import com.daml.platform.services.time.TimeProviderType
import com.daml.util.Ctx
import org.scalatest.{Inside, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

final class FailureTesting
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with SuiteResourceManagementAroundAll
    with TryValues {

  override protected def config: Config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        timeProviderType = TimeProviderType.Static
      )
    )
  )

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration
      .copy(inFlightCommandBackPressureCount = 20, inFlightCommandOverflowCount = 200)

  "Failure testing" should {
    // The following value should be kept in sync with the value of contractPairings in Cats.daml
    val contractPairings = 400

    s"with $contractPairings contract pairings and always failing submissions" should {
      def command(template: String, owner: String, i: Int): CreateCommand =
        CreateCommand(
          templateId = Some(LedgerApi.Identifier(packageId, "Cats", template)),
          createArguments = Some(
            LedgerApi.Record(fields =
              Seq(
                LedgerApi.RecordField("owner", Some(LedgerApi.Value().withParty(owner))),
                template match {
                  case "TestControl" =>
                    LedgerApi.RecordField("size", Some(LedgerApi.Value().withInt64(i.toLong)))
                  case _ =>
                    LedgerApi.RecordField("isin", Some(LedgerApi.Value().withInt64(i.toLong)))
                },
              )
            )
          ),
        )

      def cat(owner: String, i: Int): CreateCommand = command("Cat", owner, i)

      def food(owner: String, i: Int): CreateCommand = command("Food", owner, i)

      def notObserving(
          templateId: LedgerApi.Identifier
      ): TriggerContext[TriggerMsg] => Boolean = {
        case Ctx(
              _,
              TriggerMsg.Transaction(
                ApiTransaction(_, _, _, _, Seq(ApiEvent(Created(created))), _)
              ),
              _,
            ) if created.getTemplateId == templateId =>
          false

        case _ =>
          true
      }

      "Process all contract pairings successfully" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          _ <- Future.sequence(
            (0 until contractPairings).map { i =>
              create(client, party, cat(party, i))
            }
          )
          _ <- Future.sequence(
            (0 until contractPairings).map { i =>
              create(client, party, food(party, i))
            }
          )
          runner = getRunner(
            client,
            QualifiedName.assertFromString("Cats:feedingTrigger"),
            party,
          )
          (acs, offset) <- runner.queryACS()
          _ <- runner
            .runWithACS(
              acs,
              offset,
              msgFlow = Flow[TriggerContext[TriggerMsg]]
                // Allow flow to proceed until we observe a CatExample:TestComplete contract being created
                .takeWhile(
                  notObserving(LedgerApi.Identifier(packageId, "Cats", "TestComplete"))
                ),
            )
            ._2
        } yield {
          succeed
        }
      }
    }

    "Ledger completion and transaction delays" should {
      "Eventually cause a trigger overflow" in {
        recoverToSucceededIf[InFlightCommandOverflowException] {
          for {
            client <- ledgerClient()
            party <- allocateParty(client)
            runner = getRunner(
              client,
              QualifiedName.assertFromString("Cats:overflowTrigger"),
              party,
            )
            (acs, offset) <- runner.queryACS()
            _ <- runner
              .runWithACS(
                acs,
                offset,
                msgFlow = Flow[TriggerContext[TriggerMsg]]
                  // Filter out all completion and transaction events and so simulate ledger command completion delays
                  .filter {
                    case Ctx(_, TriggerMsg.Completion(_), _) =>
                      false
                    case Ctx(_, TriggerMsg.Transaction(_), _) =>
                      false
                    case _ =>
                      true
                  },
              )
              ._2
          } yield {
            fail("Cats:overflowTrigger failed to throw InFlightCommandOverflowException")
          }
        }
      }
    }
  }
}
