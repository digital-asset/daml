// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Command.{Command => ApiCommand}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.lf.crypto
import com.daml.lf.data.{Bytes, Ref}
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.engine.trigger.TriggerMsg
import com.daml.lf.speedy.{Command, SValue}
import com.daml.lf.speedy.SValue.{SContractId, SInt64, SParty}
import com.daml.lf.value.Value.ContractId
import com.daml.script.converter.Converter.record
import com.google.rpc.status.Status
import io.grpc.Status.Code
import io.grpc.Status.Code.OK
import org.scalacheck.Gen

import java.util.UUID
import scala.concurrent.ExecutionContext

trait CatGenerators {

  protected def packageId: PackageId

  val partyGen: Gen[String] = Gen.const("alice")

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

  def createCommandGen(maxNumOfCats: Long): Gen[Command.Create] =
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

  def exerciseCommandGen(maxNumOfCats: Long): Gen[Command.ExerciseTemplate] =
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

  def commandGen(maxNumOfCats: Long): Gen[Command] = Gen.frequency(
    1 -> createCommandGen(maxNumOfCats),
    9 -> exerciseCommandGen(maxNumOfCats),
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
}

trait CatTriggerResourceUsageTestGenerators extends CatGenerators {

  def acsGen(owner: String, size: Long): Seq[CreatedEvent] =
    acsGen(owner, size, size)

  def acsGen(owner: String, numOfCats: Long, amountOfFood: Long): Seq[CreatedEvent] =
    (0L to numOfCats).map(i => createCat(owner, i)) ++ (0L to amountOfFood).map(i =>
      createFood(owner, i)
    )

  def monotonicACS(owner: String, sizeGen: Iterator[Long]): Iterator[Seq[CreatedEvent]] =
    sizeGen.map(acsGen(owner, _))

  def triggerIterator(simulator: TriggerRuleSimulationLib, startState: SValue)(implicit
      ec: ExecutionContext
  ): Source[(Seq[SubmitRequest], TriggerRuleMetrics.RuleMetrics, SValue), NotUsed] = {
    Source
      .repeat(TriggerMsg.Heartbeat)
      .scanAsync[(Seq[SubmitRequest], Option[TriggerRuleMetrics.RuleMetrics], SValue)](
        (Seq.empty, None, startState)
      ) { case ((_, _, state), msg) =>
        for {
          (submissions, metrics, nextState) <- simulator.updateStateLambda(state, msg)
        } yield (submissions, Some(metrics), nextState)
      }
      .collect { case (submissions, Some(metrics), state) => (submissions, metrics, state) }
  }

  implicit class TriggerRuleSimulationHelpers(
      source: Source[(Seq[SubmitRequest], TriggerRuleMetrics.RuleMetrics, SValue), NotUsed]
  ) {
    def findDuplicateCommandRequests: Source[Set[ApiCommand], NotUsed] = {
      source
        .scan[(Set[ApiCommand], Set[ApiCommand])]((Set.empty, Set.empty)) {
          case ((observations, _), (submissions, _, _)) =>
            val newObservations = submissions.flatMap(_.getCommands.commands.map(_.command)).toSet
            val newDuplicates = newObservations.intersect(observations)

            (observations ++ newObservations, newDuplicates)
        }
        .map(_._2)
    }
  }
}
