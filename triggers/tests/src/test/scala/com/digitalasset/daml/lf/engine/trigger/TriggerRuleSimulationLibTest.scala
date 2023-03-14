// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, ParticipantId, singleParticipant}
import com.daml.lf.crypto
import com.daml.lf.data.{Bytes, Ref}
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.engine.trigger.Runner.{
  numberOfActiveContracts,
  numberOfInFlightCommands,
  numberOfInFlightCreateCommands,
  numberOfPendingContracts,
}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SValue.{SContractId, SInt64, SParty}
import com.daml.lf.speedy.{Command, SValue}
import com.daml.lf.value.Value.ContractId
import com.daml.platform.services.time.TimeProviderType
import com.daml.script.converter.Converter.record
import com.google.rpc.status.Status
import io.grpc.Status.Code
import io.grpc.Status.Code.OK
import org.scalacheck.Gen
import org.scalatest.{Assertion, Inside, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.Future

class TriggerRuleSimulationLibTest
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with SuiteResourceManagementAroundAll
    with TryValues
    with CatGenerators {

  import TriggerRuleSimulationLib._

  override protected def config: Config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        timeProviderType = TimeProviderType.Static
      )
    )
  )

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration.copy(hardLimit =
      super.triggerRunnerConfiguration.hardLimit
        .copy(allowTriggerTimeouts = true, allowInFlightCommandOverflows = true)
    )

  def forAll[T](gen: Gen[T], sampleSize: Int = 100, parallelism: Int = 1)(
      test: T => Future[Assertion]
  ): Future[Assertion] = {
    // TODO: ????: use results (e.g. submissions and ACS/inflight changes) of simulator runs to infer additional events
    Source(0 to sampleSize)
      .map(_ => gen.sample)
      .collect { case Some(data) => data }
      .mapAsync(parallelism) { data =>
        test(data)
      }
      .takeWhile(_ == succeed, inclusive = true)
      .runWith(Sink.last)
  }

  "Trigger rule simulation" should {
    "correctly log metrics for initState lambda" in {
      forAll(initState) { acs =>
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          (trigger, simulator) = getSimulator(
            client,
            QualifiedName.assertFromString("Cats:feedingTrigger"),
            packageId,
            applicationId,
            compiledPackages,
            config.participants(ParticipantId).apiServer.timeProviderType,
            triggerRunnerConfiguration,
            party,
          )
          (submissions, metrics, state) <- simulator.initialStateLambda(acs)
        } yield {
          withClue((acs, state, submissions, metrics)) {
            metrics.evaluation.submissions should be(submissions.size)
            metrics.submission.creates should be(submissions.map(numberOfCreateCommands).sum)
            metrics.submission.createAndExercises should be(
              submissions.map(numberOfCreateAndExerciseCommands).sum
            )
            metrics.submission.exercises should be(submissions.map(numberOfExerciseCommands).sum)
            metrics.submission.exerciseByKeys should be(
              submissions.map(numberOfExerciseByKeyCommands).sum
            )
            metrics.evaluation.steps should be(
              metrics.evaluation.getTimes + metrics.evaluation.submissions + 1
            )
            metrics.startState.acs.activeContracts should be(acs.size)
            metrics.startState.acs.pendingContracts should be(0)
            metrics.startState.inFlight.commands should be(0)
            Some(metrics.endState.acs.activeContracts) should be(
              numberOfActiveContracts(state, trigger.level, trigger.version)
            )
            Some(metrics.endState.acs.pendingContracts) should be(
              numberOfPendingContracts(state, trigger.level, trigger.version)
            )
            Some(metrics.endState.inFlight.commands) should be(
              numberOfInFlightCommands(state, trigger.level, trigger.version)
            )
          }
        }
      }
    }

    "correctly log metrics for updateState lambda" in {
      forAll(updateState) { case (acs, userState, inFlightCmds, msg) =>
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          (trigger, simulator) = getSimulator(
            client,
            QualifiedName.assertFromString("Cats:feedingTrigger"),
            packageId,
            applicationId,
            compiledPackages,
            config.participants(ParticipantId).apiServer.timeProviderType,
            triggerRunnerConfiguration,
            party,
          )
          converter = new Converter(compiledPackages, trigger)
          startState = converter
            .fromTriggerUpdateState(
              acs,
              userState,
              inFlightCmds,
              TriggerParties(Party(party), Set.empty),
              triggerRunnerConfiguration,
            )
          (submissions, metrics, endState) <- simulator.updateStateLambda(startState, msg)
        } yield {
          withClue((startState, msg, endState, submissions, metrics)) {
            metrics.evaluation.submissions should be(submissions.size)
            metrics.submission.creates should be(submissions.map(numberOfCreateCommands).sum)
            metrics.submission.createAndExercises should be(
              submissions.map(numberOfCreateAndExerciseCommands).sum
            )
            metrics.submission.exercises should be(submissions.map(numberOfExerciseCommands).sum)
            metrics.submission.exerciseByKeys should be(
              submissions.map(numberOfExerciseByKeyCommands).sum
            )
            metrics.evaluation.steps should be(
              metrics.evaluation.getTimes + metrics.evaluation.submissions + 1
            )
            Some(metrics.startState.acs.activeContracts) should be(
              numberOfActiveContracts(startState, trigger.level, trigger.version)
            )
            Some(metrics.startState.acs.pendingContracts) should be(
              numberOfPendingContracts(startState, trigger.level, trigger.version)
            )
            Some(metrics.startState.inFlight.commands) should be(
              numberOfInFlightCommands(startState, trigger.level, trigger.version)
            )
            Some(metrics.endState.acs.activeContracts) should be(
              numberOfActiveContracts(endState, trigger.level, trigger.version)
            )
            Some(metrics.endState.acs.pendingContracts) should be(
              numberOfPendingContracts(endState, trigger.level, trigger.version)
            )
            Some(metrics.endState.inFlight.commands) should be(
              numberOfInFlightCommands(endState, trigger.level, trigger.version)
            )
            val pendingContractSubmissions = submissions
              .map(request =>
                numberOfCreateCommands(request) + numberOfCreateAndExerciseCommands(
                  request
                ) + numberOfExerciseCommands(request) + numberOfExerciseByKeyCommands(request)
              )
              .sum
            msg match {
              case TriggerMsg.Completion(completion)
                  if completion.getStatus.code == 0 && completion.commandId.nonEmpty =>
                // Completion success for a request this trigger (should have) produced
                metrics.startState.acs.activeContracts should be(
                  metrics.endState.acs.activeContracts
                )
                metrics.endState.acs.pendingContracts should be(
                  metrics.startState.acs.pendingContracts + pendingContractSubmissions
                )
                metrics.endState.inFlight.commands should be(
                  metrics.startState.inFlight.commands + submissions.size
                )

              case TriggerMsg.Completion(completion)
                  if completion.getStatus.code != 0 && completion.commandId.nonEmpty =>
                // Completion failure for a request this trigger (should have) produced
                val completionFailureSubmissions = numberOfInFlightCreateCommands(
                  converter.fromCommandId(completion.commandId),
                  endState,
                  trigger.level,
                  trigger.version,
                )
                val submissionDelta = if (inFlightCmds.contains(completion.commandId)) 1 else 0
                metrics.startState.acs.activeContracts should be(
                  metrics.endState.acs.activeContracts
                )
                metrics.endState.acs.pendingContracts should be(
                  metrics.startState.acs.pendingContracts + pendingContractSubmissions - completionFailureSubmissions
                    .getOrElse(0)
                )
                metrics.endState.inFlight.commands should be(
                  metrics.startState.inFlight.commands + submissions.size - submissionDelta
                )

              case TriggerMsg.Transaction(transaction) if transaction.commandId.nonEmpty =>
                // Transaction events are related to a command this trigger (should have) produced
                val transactionCreates = transaction.events.count(_.event.isCreated)
                val transactionArchives = transaction.events.count(_.event.isArchived)
                val submissionDelta = if (inFlightCmds.contains(transaction.commandId)) 1 else 0
                metrics.endState.acs.activeContracts should be(
                  metrics.startState.acs.activeContracts + transactionCreates - transactionArchives
                )
                metrics.endState.acs.pendingContracts should be(
                  metrics.startState.acs.pendingContracts + pendingContractSubmissions // - (transactionCreates + transactionArchives)
                )
                metrics.endState.inFlight.commands should be(
                  metrics.startState.inFlight.commands + submissions.size - submissionDelta
                )

              case TriggerMsg.Transaction(transaction) =>
                // Transaction events are related to events this trigger subscribed to, but not to commands produced by this trigger
                val transactionCreates = transaction.events.count(_.event.isCreated)
                val transactionArchives = transaction.events.count(_.event.isArchived)
                metrics.endState.acs.activeContracts should be(
                  metrics.startState.acs.activeContracts + transactionCreates - transactionArchives
                )
                metrics.endState.acs.pendingContracts should be(
                  metrics.startState.acs.pendingContracts + pendingContractSubmissions
                )
                metrics.endState.inFlight.commands should be(
                  metrics.startState.inFlight.commands + submissions.size
                )

              case _ =>
                metrics.startState.acs.activeContracts should be(
                  metrics.endState.acs.activeContracts
                )
                metrics.endState.acs.pendingContracts should be(
                  metrics.startState.acs.pendingContracts + pendingContractSubmissions
                )
                metrics.endState.inFlight.commands should be(
                  metrics.startState.inFlight.commands + submissions.size
                )
            }
          }
        }
      }
    }
  }
}

trait CatGenerators {
  import TriggerRuleSimulationLib.{CommandsInFlight, TriggerExperiment}

  protected def packageId: PackageId

  def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))

  def create(template: String, owner: String, i: Long): CreatedEvent =
    CreatedEvent(
      contractId = toContractId(s"$template:$i").coid,
      templateId = Some(LedgerApi.Identifier(packageId, "Cats", template)),
      createArguments = Some(
        LedgerApi.Record(fields =
          Seq(
            LedgerApi.RecordField("owner", Some(LedgerApi.Value().withParty(owner))),
            template match {
              case "TestControl" =>
                LedgerApi.RecordField("size", Some(LedgerApi.Value().withInt64(i)))
              case _ =>
                LedgerApi.RecordField("isin", Some(LedgerApi.Value().withInt64(i)))
            },
          )
        )
      ),
    )

  def archive(template: String, owner: String, i: Long): ArchivedEvent =
    ArchivedEvent(
      contractId = toContractId(s"$template:$i").coid,
      templateId = Some(LedgerApi.Identifier(packageId, "Cats", template)),
      witnessParties = Seq(owner),
    )

  def createCat(owner: String, i: Long): CreatedEvent = create("Cat", owner, i)

  def archivedCat(owner: String, i: Long): ArchivedEvent = archive("Cat", owner, i)

  def createFood(owner: String, i: Long): CreatedEvent = create("Food", owner, i)

  def archivedFood(owner: String, i: Long): ArchivedEvent = archive("Food", owner, i)

  def createCommandGen: Gen[Command.Create] =
    for {
      party <- partyGen
      templateId <- Gen.frequency(
        1 -> Identifier(packageId, QualifiedName.assertFromString("Cats:Cat")),
        1 -> Identifier(packageId, QualifiedName.assertFromString("Cats:Food")),
      )
      n <- Gen.choose(0, maxNumOfCats)
      arguments = record(
        templateId,
        "owner" -> SParty(Ref.Party.assertFromString(party)),
        "isin" -> SInt64(n),
      )
    } yield Command.Create(templateId, arguments)

  def exerciseCommandGen: Gen[Command.ExerciseTemplate] =
    for {
      n <- Gen.choose(0L, maxNumOfCats)
      templateId = Identifier(packageId, QualifiedName.assertFromString("Cats:Cat"))
      contractId = SContractId(toContractId(s"Cat:$n"))
      foodCid = SContractId(toContractId(s"Food:$n"))
      choiceId <- Gen.const("Feed")
      argument = record(templateId, "foodCid" -> foodCid)
    } yield Command.ExerciseTemplate(
      templateId,
      contractId,
      Ref.ChoiceName.assertFromString(choiceId),
      argument,
    )

  def commandGen: Gen[Command] = Gen.frequency(
    1 -> createCommandGen,
    9 -> exerciseCommandGen,
  )

  def transactionGen(
      owner: String,
      numOfCats: Long = 0,
      amountOfFood: Long = 0,
      catsKilled: Long = 0,
      foodEaten: Long = 0,
      knownCmdId: String = UUID.randomUUID().toString,
  ): Gen[Transaction] =
    for {
      id <- Gen.numStr
      cmdId <- Gen.frequency(
        1 -> Gen.const(""),
        9 -> Gen.frequency(
          1 -> Gen.uuid.map(_.toString),
          9 -> Gen.const(knownCmdId),
        ),
      )
      cats = (0L to numOfCats)
        .map(i => createCat(owner, i))
        .map(event => Event(Event.Event.Created(event)))
      deadCats = (0L to catsKilled)
        .map(i => archivedCat(owner, i))
        .map(event => Event(Event.Event.Archived(event)))
      food = (0L to amountOfFood)
        .map(i => createFood(owner, i))
        .map(event => Event(Event.Event.Created(event)))
      eatenFood = (0L to foodEaten)
        .map(i => archivedFood(owner, i))
        .map(event => Event(Event.Event.Archived(event)))
    } yield Transaction(
      transactionId = id,
      commandId = cmdId,
      events = cats ++ food ++ deadCats ++ eatenFood,
    )

  def successfulCompletionGen(
      owner: String,
      knownCmdId: String = UUID.randomUUID().toString,
  ): Gen[Completion] =
    for {
      id <- Gen.numStr
      cmdId <- Gen.frequency(
        1 -> Gen.const(""),
        9 -> Gen.frequency(
          1 -> Gen.uuid.map(_.toString),
          9 -> Gen.const(knownCmdId),
        ),
      )
    } yield Completion(
      commandId = cmdId,
      status = Some(Status(OK.value(), "")),
      transactionId = id,
      actAs = Seq(owner),
    )

  def failingCompletionGen(
      owner: String,
      knownCmdId: String = UUID.randomUUID().toString,
  ): Gen[Completion] =
    for {
      id <- Gen.numStr
      cmdId <- Gen.frequency(
        1 -> Gen.const(""),
        9 -> Gen.frequency(
          1 -> Gen.uuid.map(_.toString),
          9 -> Gen.const(knownCmdId),
        ),
      )
      code <- Gen.choose(1, Code.values().length)
    } yield Completion(
      commandId = cmdId,
      status = Some(Status(code, "simulated-failure")),
      transactionId = id,
      actAs = Seq(owner),
    )

  private val maxNumOfCats = 10L

  private val partyGen: Gen[String] = Gen.const("alice")

  private val userStateGen: Gen[SValue] = Gen.choose(0L, maxNumOfCats).map(SValue.SInt64)

  private val basicTesting = TriggerExperiment(
    acsGen = Gen.const(Seq.empty),
    userStateGen = userStateGen,
    inFlightCmdGen = Gen.const(Map.empty),
    msgGen = Gen.const(TriggerMsg.Heartbeat),
  )

  private val activeContractTesting = TriggerExperiment(
    acsGen = for {
      party <- partyGen
      numOfCats <- Gen.choose(0, maxNumOfCats)
      amountOfFood <- Gen.choose(0, maxNumOfCats)
      cats = (0L to numOfCats).map(createCat(party, _))
      food = (0L to amountOfFood).map(createFood(party, _))
    } yield cats ++ food,
    userStateGen = userStateGen,
    inFlightCmdGen = Gen.const(Map.empty),
    msgGen = Gen.const(TriggerMsg.Heartbeat),
  )

  private val transactionTesting = TriggerExperiment(
    acsGen = Gen.const(Seq.empty),
    userStateGen = userStateGen,
    inFlightCmdGen = Gen.const(Map.empty),
    msgGen = for {
      party <- partyGen
      numOfCats <- Gen.choose(0, maxNumOfCats)
      amountOfFood <- Gen.choose(0, maxNumOfCats)
      transaction <- transactionGen(party, numOfCats = numOfCats, amountOfFood = amountOfFood)
    } yield TriggerMsg.Transaction(transaction),
  )

  private val transactionInFlightTesting = {
    val cmdId = UUID.randomUUID().toString
    val inFlightCmdGen = for {
      cmds <- Gen.listOfN(maxNumOfCats.toInt, commandGen)
    } yield (cmdId, cmds)

    TriggerExperiment(
      acsGen = Gen.const(Seq.empty),
      userStateGen = userStateGen,
      inFlightCmdGen = Gen.mapOf(inFlightCmdGen),
      msgGen = for {
        party <- partyGen
        numOfCats <- Gen.choose(0, maxNumOfCats)
        amountOfFood <- Gen.choose(0, maxNumOfCats)
        transaction <- transactionGen(
          party,
          numOfCats = numOfCats,
          amountOfFood = amountOfFood,
          knownCmdId = cmdId,
        )
      } yield TriggerMsg.Transaction(transaction),
    )
  }

  private val completionTesting = {
    val cmdId = UUID.randomUUID().toString

    TriggerExperiment(
      acsGen = Gen.const(Seq.empty),
      userStateGen = userStateGen,
      inFlightCmdGen = Gen.const(Map(cmdId -> Seq.empty)),
      msgGen = for {
        party <- partyGen
        completion <- Gen.frequency(
          9 -> Gen.frequency(
            1 -> successfulCompletionGen(party),
            9 -> successfulCompletionGen(party, cmdId),
          ),
          1 -> Gen.frequency(
            1 -> failingCompletionGen(party),
            9 -> failingCompletionGen(party, cmdId),
          ),
        )
      } yield TriggerMsg.Completion(completion),
    )
  }

  private val completionInFlightTesting = {
    val cmdId = UUID.randomUUID().toString
    val inFlightCmdGen = for {
      cmds <- Gen.listOfN(maxNumOfCats.toInt, commandGen)
    } yield (cmdId, cmds)

    TriggerExperiment(
      acsGen = Gen.const(Seq.empty),
      userStateGen = userStateGen,
      inFlightCmdGen = Gen.mapOf(inFlightCmdGen),
      msgGen = for {
        party <- partyGen
        completion <- Gen.frequency(
          9 -> Gen.frequency(
            1 -> successfulCompletionGen(party),
            9 -> successfulCompletionGen(party, cmdId),
          ),
          1 -> Gen.frequency(
            1 -> failingCompletionGen(party),
            9 -> failingCompletionGen(party, cmdId),
          ),
        )
      } yield TriggerMsg.Completion(completion),
    )
  }

  val initState: Gen[Seq[CreatedEvent]] = Gen.frequency(
    1 -> basicTesting.initState,
    1 -> activeContractTesting.initState,
    1 -> transactionTesting.initState,
    1 -> transactionInFlightTesting.initState,
    1 -> completionTesting.initState,
    1 -> completionInFlightTesting.initState,
  )

  val updateState: Gen[(Seq[CreatedEvent], SValue, CommandsInFlight, TriggerMsg)] = Gen.frequency(
    1 -> basicTesting.updateState,
    1 -> activeContractTesting.updateState,
    1 -> transactionTesting.updateState,
    1 -> transactionInFlightTesting.updateState,
    1 -> completionTesting.updateState,
    1 -> completionInFlightTesting.updateState,
  )
}
