// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import org.apache.pekko.stream.scaladsl.Flow
import com.daml.ledger.api.refinements.ApiTypes.{Party => ApiParty}
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.event.{Event => ApiEvent}
import com.daml.ledger.api.v1.transaction.{Transaction => ApiTransaction}
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.engine.trigger.Runner.TriggerContext
import com.daml.lf.engine.trigger.{
  ACSOverflowException,
  InFlightCommandOverflowException,
  TriggerMsg,
  TriggerRuleEvaluationTimeout,
  TriggerRunnerConfig,
}
import com.daml.lf.language.LanguageMajorVersion
import com.daml.util.Ctx
import org.scalatest.{Inside, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import scala.concurrent.duration._

abstract class LoadTesting
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with TryValues {

  // The following value should be kept in sync with the value of contractPairings in Cats.daml
  val contractPairings: Int = 400
  // The following value should be kept in sync with the value of breedingRate in Cats.daml
  val breedingRate: Int = 100

  def command(template: String, owner: ApiParty, i: Int): CreateCommand =
    CreateCommand(
      templateId = Some(LedgerApi.Identifier(packageId, "Cats", template)),
      createArguments = Some(
        LedgerApi.Record(fields =
          Seq(
            LedgerApi.RecordField("owner", Some(LedgerApi.Value().withParty(owner.unwrap))),
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

  def cat(owner: ApiParty, i: Int): CreateCommand = command("Cat", owner, i)

  def food(owner: ApiParty, i: Int): CreateCommand = command("Food", owner, i)

  def notObserving(
      templateId: LedgerApi.Identifier
  ): TriggerContext[TriggerMsg] => Boolean = {
    case Ctx(
          _,
          TriggerMsg.Transaction(
            ApiTransaction(_, _, _, _, Seq(ApiEvent(Created(created))), _, _)
          ),
          _,
        ) if created.getTemplateId == templateId =>
      false

    case _ =>
      true
  }
}

class BaseLoadTestingV1 extends BaseLoadTesting(LanguageMajorVersion.V1)

// TODO(https://github.com/digital-asset/daml/issues/17812): re-enable this test and control its run
//  at the bazel target level.
class BaseLoadTestingV2 extends BaseLoadTesting(LanguageMajorVersion.V2)

class BaseLoadTesting(override val majorLanguageVersion: LanguageMajorVersion) extends LoadTesting {

  import AbstractTriggerTest._

  s"With $contractPairings contract pairings" should {
    "Contracts are already created" should {
      "Process all contract pairings successfully" in {
        for {
          client <- defaultLedgerClient()
          party <- allocateParty(client)
          _ <- create(client, party, (0 until contractPairings).map(i => cat(party, i)))
          _ <- create(client, party, (0 until contractPairings).map(i => food(party, i)))
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
                // Allow flow to proceed until we observe a Cats:TestComplete contract being created
                .takeWhile(
                  notObserving(LedgerApi.Identifier(packageId, "Cats", "TestComplete"))
                ),
            )
            ._2
        } yield {
          succeed
        }
      }

      "Contracts are created by the trigger" should {
        "Process all contract pairings successfully" in {
          for {
            client <- defaultLedgerClient()
            party <- allocateParty(client)
            runner = getRunner(
              client,
              QualifiedName.assertFromString("Cats:breedAndFeedTrigger"),
              party,
            )
            (acs, offset) <- runner.queryACS()
            _ <- runner
              .runWithACS(
                acs,
                offset,
                msgFlow = Flow[TriggerContext[TriggerMsg]]
                  // Allow flow to proceed until we observe a Cats:TestComplete contract being created
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
    }
  }
}

class InFlightLoadTestingV1 extends InFlightLoadTesting(LanguageMajorVersion.V1)

// TODO(https://github.com/digital-asset/daml/issues/17812): re-enable this test and control its run
//  at the bazel target level.
class InFlightLoadTestingV2 extends InFlightLoadTesting(LanguageMajorVersion.V2)

class InFlightLoadTesting(override val majorLanguageVersion: LanguageMajorVersion)
    extends LoadTesting {

  import AbstractTriggerTest._

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration
      .copy(
        inFlightCommandBackPressureCount = 20,
        hardLimit =
          super.triggerRunnerConfiguration.hardLimit.copy(inFlightCommandOverflowCount = 40),
      )

  "Ledger completion and transaction delays" should {
    "Eventually cause a trigger overflow" in {
      recoverToSucceededIf[InFlightCommandOverflowException] {
        for {
          client <- defaultLedgerClient()
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

class ACSLoadTestingV1 extends ACSLoadTesting(LanguageMajorVersion.V1)

// TODO(https://github.com/digital-asset/daml/issues/17812): re-enable this test and control its run
//  at the bazel target level.
class ACSLoadTestingV2 extends ACSLoadTesting(LanguageMajorVersion.V2)

class ACSLoadTesting(override val majorLanguageVersion: LanguageMajorVersion) extends LoadTesting {

  import AbstractTriggerTest._

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration.copy(hardLimit =
      super.triggerRunnerConfiguration.hardLimit.copy(maximumActiveContracts = 10)
    )

  "Large ACS on trigger startup" should {
    "Cause a trigger overflow" in {
      recoverToSucceededIf[ACSOverflowException] {
        for {
          client <- defaultLedgerClient()
          party <- allocateParty(client)
          _ <- create(client, party, (0 until breedingRate).map(i => cat(party, i)))
          _ <- create(client, party, (0 until breedingRate).map(i => food(party, i)))
          runner = getRunner(
            client,
            QualifiedName.assertFromString("Cats:feedingTrigger"),
            party,
          )
          (acs, offset) <- runner.queryACS()
          _ <- runner.runWithACS(acs, offset)._2
        } yield {
          fail("Cats:feedingTrigger failed to throw ACSOverflowException")
        }
      }
    }
  }

  "Creating large numbers of active contracts" should {
    "Cause a trigger overflow" in {
      recoverToSucceededIf[ACSOverflowException] {
        for {
          client <- defaultLedgerClient()
          party <- allocateParty(client)
          runner = getRunner(
            client,
            QualifiedName.assertFromString("Cats:breedingTrigger"),
            party,
          )
          (acs, offset) <- runner.queryACS()
          _ <- runner
            .runWithACS(
              acs,
              offset,
              msgFlow = Flow[TriggerContext[TriggerMsg]]
                // Allow flow to proceed until we observe a Cats:TestComplete contract being created
                .takeWhile(
                  notObserving(LedgerApi.Identifier(packageId, "Cats", "TestComplete"))
                ),
            )
            ._2
        } yield {
          fail("Cats:breedingTrigger failed to throw ACSOverflowException")
        }
      }
    }
  }
}

class TriggerRuleEvaluationTimeoutTestingV1
    extends TriggerRuleEvaluationTimeoutTesting(LanguageMajorVersion.V1)

// TODO(https://github.com/digital-asset/daml/issues/17812): re-enable this test and control its run
//  at the bazel target level.
class TriggerRuleEvaluationTimeoutTestingV2
    extends TriggerRuleEvaluationTimeoutTesting(LanguageMajorVersion.V2)

class TriggerRuleEvaluationTimeoutTesting(override val majorLanguageVersion: LanguageMajorVersion)
    extends LoadTesting {

  import AbstractTriggerTest._

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration.copy(hardLimit =
      super.triggerRunnerConfiguration.hardLimit
        .copy(allowTriggerTimeouts = true, ruleEvaluationTimeout = 1.second)
    )

  "Long running trigger initialization evaluation" should {
    "Cause a trigger timeout" in {
      recoverToSucceededIf[TriggerRuleEvaluationTimeout] {
        for {
          client <- defaultLedgerClient()
          party <- allocateParty(client)
          runner = getRunner(
            client,
            QualifiedName.assertFromString("Cats:earlyBreedingTrigger"),
            party,
          )
          (acs, offset) <- runner.queryACS()
          _ <- runner
            .runWithACS(
              acs,
              offset,
              msgFlow = Flow[TriggerContext[TriggerMsg]]
                // Allow flow to proceed until we observe a Cats:TestComplete contract being created
                .takeWhile(
                  notObserving(LedgerApi.Identifier(packageId, "Cats", "TestComplete"))
                ),
            )
            ._2
        } yield {
          fail("Cats:earlyBreedingTrigger failed to throw TriggerRuleEvaluationTimeout")
        }
      }
    }
  }

  "Long running trigger rule evaluation" should {
    "Cause a trigger timeout" in {
      recoverToSucceededIf[TriggerRuleEvaluationTimeout] {
        for {
          client <- defaultLedgerClient()
          party <- allocateParty(client)
          runner = getRunner(
            client,
            QualifiedName.assertFromString("Cats:lateBreedingTrigger"),
            party,
          )
          (acs, offset) <- runner.queryACS()
          _ <- runner
            .runWithACS(
              acs,
              offset,
              msgFlow = Flow[TriggerContext[TriggerMsg]]
                // Allow flow to proceed until we observe a Cats:TestComplete contract being created
                .takeWhile(
                  notObserving(LedgerApi.Identifier(packageId, "Cats", "TestComplete"))
                ),
            )
            ._2
        } yield {
          fail("Cats:lateBreedingTrigger failed to throw TriggerRuleEvaluationTimeout")
        }
      }
    }
  }
}
