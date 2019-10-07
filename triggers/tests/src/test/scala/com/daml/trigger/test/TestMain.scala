// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.trigger.test

import java.io.File
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Sink, Flow}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}
import scalaz.syntax.tag._
import scalaz.syntax.traverse._

import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.ledger.api.v1.command_submission_service._
import com.digitalasset.ledger.api.v1.commands._
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.daml.lf.value.{Value => Lf}

import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.daml.lf.speedy.SExpr
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.platform.participant.util.LfEngineToApi.{toApiIdentifier}
import com.digitalasset.platform.server.api.validation.FieldValidations.{validateIdentifier}

import com.daml.trigger.{Runner, TriggerMsg}

case class Config(ledgerPort: Int, darPath: File)

// We do not use scalatest here since that doesn’t work nicely with
// the client_server_test macro.

case class NumMessages(num: Long)

object TestRunner {
  def assertEqual[A](actual: A, expected: A, note: String) = {
    if (actual == expected) {
      Right(())
    } else {
      Left(s"$note: Expected $expected but got $actual")
    }
  }
  def findStdlibPackageId(dar: Dar[(PackageId, Package)]) =
    dar.all
      .find {
        case (pkgId, pkg) =>
          pkg.modules.contains(DottedName.assertFromString("DA.Internal.Template"))
      }
      .get
      ._1
}

class TestRunner(ledgerPort: Int) {
  var partyCount = 0

  val applicationId = ApplicationId("Trigger Test Runner")

  val clientConfig = LedgerClientConfiguration(
    applicationId = applicationId.unwrap,
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = None
  )

  def getNewParty(): String = {
    partyCount = partyCount + 1
    s"Alice$partyCount"
  }

  def genericTest[A](
      // test name
      name: String,
      // The dar package the trigger is in
      dar: Dar[(PackageId, Package)],
      // Identifier of the trigger value
      triggerId: Identifier,
      // Commands to interact with the trigger
      commands: (LedgerClient, String) => ExecutionContext => ActorMaterializer => Future[A],
      // the number of messages that should be delivered to the trigger
      numMessages: NumMessages,
      // assertion on final state
      assertFinalState: (SExpr, A) => Either[String, Unit],
      // assertion on the final acs represented as a map from template_id to
      // (contract_id, create_arguments)
      assertFinalACS: (
          Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
          A) => Either[String, Unit]) = {

    println(s"---\n$name:")

    val system = ActorSystem("TriggerRunner")
    val sequencer = new AkkaExecutionSequencerPool("TriggerRunnerPool")(system)
    implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
    implicit val ec: ExecutionContext = system.dispatcher

    val party = getNewParty()
    val clientF = LedgerClient.singleHost("localhost", ledgerPort, clientConfig)(ec, sequencer)
    val triggerFlow: Future[SExpr] = for {
      client <- clientF
      finalState <- Runner.run(
        dar,
        triggerId,
        client,
        applicationId,
        party,
        msgFlow = Flow[TriggerMsg].take(numMessages.num)
      )
    } yield finalState
    val commandsFlow: Future[A] =
      clientF.flatMap(client => commands(client, party)(ec)(materializer))
    triggerFlow.failed.foreach(_ => system.terminate)
    commandsFlow.failed.foreach(_ => system.terminate)
    val filter = TransactionFilter(List((party, Filters.defaultInstance)).toMap)
    val testFlow: Future[Unit] = for {
      client <- clientF
      finalState <- triggerFlow
      commandsR <- commandsFlow
      _ <- assertFinalState(finalState, commandsR) match {
        case Left(err) =>
          Future.failed(new RuntimeException(s"Assertion on final state failed: $err"))
        case Right(()) => Future.unit
      }
      acsResponses <- client.activeContractSetClient
        .getActiveContracts(filter, verbose = true)
        .runWith(Sink.seq)
      acs = acsResponses
        .flatMap(x => x.activeContracts)
        .map(
          ev =>
            (
              validateIdentifier(ev.getTemplateId).fold(throw _, identity),
              (
                ev.contractId,
                ValueValidator.validateRecord(ev.getCreateArguments).fold(throw _, identity))))
        .groupBy(_._1)
        .mapValues(_.map(_._2))
      _ <- assertFinalACS(acs, commandsR) match {
        case Left(err) =>
          Future.failed(new RuntimeException(s"Assertion on final ACS failed: $err"))
        case Right(()) => Future.unit
      }
    } yield ()
    testFlow.onComplete({
      case Success(_) => {
        system.terminate
        println(s"Test $name succeeded")
      }
      case Failure(err) => {
        system.terminate
        println(s"Test $name failed: $err")
        sys.exit(1)
      }
    })
    Await.result(testFlow, Duration.Inf)
  }
}

case class AcsTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  case class ActiveAssetMirrors(num: Int)
  case class SuccessfulCompletions(num: Long)
  case class FailedCompletions(num: Long)

  val triggerId: Identifier =
    Identifier(dar.main._1, QualifiedName.assertFromString("ACS:test"))

  val assetId = value.Identifier(
    packageId = dar.main._1,
    moduleName = "ACS",
    entityName = "AssetUnit"
  )

  def test(
      name: String,
      numMessages: NumMessages,
      numSuccCompletions: SuccessfulCompletions,
      numFailedCompletions: FailedCompletions,
      commands: (LedgerClient, String) => ExecutionContext => ActorMaterializer => Future[
        (Set[String], ActiveAssetMirrors)]) = {
    def assertFinalState(finalState: SExpr, commandsR: (Set[String], ActiveAssetMirrors)) = {
      finalState match {
        case SEValue(SRecord(_, _, vals)) =>
          for {
            _ <- TestRunner.assertEqual(vals.size, 5, "number of record fields")
            activeAssets <- vals.get(0) match {
              case SMap(v) => Right(v.keySet)
              case _ => Left(s"Expected a map but got ${vals.get(0)}")
            }
            successfulCompletions <- vals.get(1) match {
              case SInt64(i) => Right(i)
              case _ => Left(s"Expected an Int64 but got ${vals.get(1)}")
            }
            failedCompletions <- vals.get(2) match {
              case SInt64(i) => Right(i)
              case _ => Left(s"Expected an Int64 but got ${vals.get(2)}")
            }
            _ <- TestRunner.assertEqual(activeAssets, commandsR._1, "activeAssets")
            _ <- TestRunner.assertEqual(
              successfulCompletions,
              numSuccCompletions.num,
              "successfulCompletions")
            _ <- TestRunner.assertEqual(
              failedCompletions,
              numFailedCompletions.num,
              "failedCompletions")
          } yield ()
        case _ => Left(s"Expected a map but got $finalState")
      }
    }
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: (Set[String], ActiveAssetMirrors)) = {
      val activeMirrorContracts = acs(
        Identifier(dar.main._1, QualifiedName.assertFromString("ACS:AssetMirror"))).size
      TestRunner.assertEqual(activeMirrorContracts, commandsR._2.num, "activeMirrorContracts")
    }
    runner.genericTest(
      name,
      dar,
      triggerId,
      commands,
      numMessages,
      assertFinalState,
      assertFinalACS)
  }

  // Create a contract and return the contract id.
  def create(client: LedgerClient, party: String, commandId: String)(
      implicit ec: ExecutionContext,
      materializer: ActorMaterializer): Future[String] = {
    val commands = Seq(
      Command().withCreate(CreateCommand(
        templateId = Some(assetId),
        createArguments = Some(
          value.Record(
            recordId = Some(assetId),
            fields = Seq(
              value.RecordField(
                "issuer",
                Some(value.Value().withParty(party))
              )
            )
          )),
      )))
    for {
      r <- client.commandClient.trackSingleCommand(
        SubmitRequest(
          commands = Some(Commands(
            ledgerId = client.ledgerId.unwrap,
            applicationId = runner.applicationId.unwrap,
            commandId = commandId,
            party = party,
            ledgerEffectiveTime = Some(fromInstant(Instant.EPOCH)),
            maximumRecordTime = Some(fromInstant(Instant.EPOCH.plusSeconds(5))),
            commands = commands
          ))))
      t <- client.transactionClient.getFlatTransactionById(r.transactionId, Seq(party))
    } yield t.transaction.get.events.head.getCreated.contractId
  }

  // Archive the contract with the given id.
  def archive(client: LedgerClient, party: String, commandId: String, contractId: String)(
      implicit ec: ExecutionContext,
      materializer: ActorMaterializer): Future[Unit] = {
    val archiveVal = Some(
      value
        .Value()
        .withRecord(
          value.Record(
            recordId = Some(
              value.Identifier(
                packageId = TestRunner.findStdlibPackageId(dar),
                moduleName = "DA.Internal.Template",
                entityName = "Archive")),
            fields = Seq()
          )))
    val commands = Seq(
      Command().withExercise(ExerciseCommand(
        templateId = Some(
          value.Identifier(
            packageId = dar.main._1,
            moduleName = "ACS",
            entityName = "AssetUnit",
          )),
        contractId = contractId,
        choice = "Archive",
        choiceArgument = archiveVal,
      )))
    for {
      comp <- client.commandClient.trackSingleCommand(
        SubmitRequest(
          commands = Some(Commands(
            ledgerId = client.ledgerId.unwrap,
            applicationId = runner.applicationId.unwrap,
            commandId = commandId,
            party = party,
            ledgerEffectiveTime = Some(fromInstant(Instant.EPOCH)),
            maximumRecordTime = Some(fromInstant(Instant.EPOCH.plusSeconds(5))),
            commands = commands
          ))))
      _ <- Future {
        if (comp.getStatus.code != 0) {
          throw new RuntimeException("archive failed")
        }
      }
    } yield ()
  }

  def runTests() = {
    test(
      "1 create",
      // 1 for the create from the test
      // 1 for the completion from the test
      // 1 for the create in the trigger
      // 1 for the exercise in the trigger
      // 2 completions for the trigger
      NumMessages(6),
      SuccessfulCompletions(3),
      FailedCompletions(0),
      (client, party) => { implicit ec: ExecutionContext => implicit mat: ActorMaterializer =>
        {
          for {
            contractId <- create(client, party, "1.0")
          } yield (Set(contractId), ActiveAssetMirrors(1))
        }
      }
    )

    test(
      "2 creates",
      // 2 for the creates from the test
      // 2 completions for the test
      // 2 for the creates in the trigger
      // 2 for the exercises in the trigger
      // 4 completions for the trigger
      NumMessages(12),
      SuccessfulCompletions(6),
      FailedCompletions(0),
      (client, party) => { implicit ec: ExecutionContext => implicit mat: ActorMaterializer =>
        {
          for {
            contractId1 <- create(client, party, "2.0")
            contractId2 <- create(client, party, "2.1")
          } yield (Set(contractId1, contractId2), ActiveAssetMirrors(2))
        }
      }
    )

    test(
      "2 creates and 2 archives",
      // 2 for the creates from the test
      // 2 for the archives from the test
      // 4 for the completions from the test
      // 2 for the creates in the trigger
      // 2 for the exercises in the trigger
      // 4 for the completions in the trigger
      NumMessages(16),
      SuccessfulCompletions(8),
      FailedCompletions(0),
      (client, party) => { implicit ec: ExecutionContext => implicit mat: ActorMaterializer =>
        {
          for {
            contractId1 <- create(client, party, "3.0")
            contractId2 <- create(client, party, "3.1")
            _ <- archive(client, party, "3.2", contractId1)
            _ <- archive(client, party, "3.3", contractId2)
          } yield (Set(), ActiveAssetMirrors(2))
        }
      }
    )
  }
}

case class CopyTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  val triggerId: Identifier =
    Identifier(dar.main._1, QualifiedName.assertFromString("Copy:copyTrigger"))

  val masterId = Identifier(dar.main._1, QualifiedName.assertFromString("Copy:Master"))

  val copyId = Identifier(dar.main._1, QualifiedName.assertFromString("Copy:Copy"))

  val subscriberId = Identifier(dar.main._1, QualifiedName.assertFromString("Copy:Subscriber"))

  def test(
      name: String,
      numMessages: NumMessages,
      numMasters: Int,
      numSubscribers: Int,
      numCopies: Int,
      commands: (LedgerClient, String) => ExecutionContext => ActorMaterializer => Future[Unit]) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = Right(())
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = {
      println(acs)
      for {
        _ <- TestRunner.assertEqual(
          acs.get(masterId).fold(0)(_.size),
          numMasters,
          "number of Master contracts")
        _ <- TestRunner.assertEqual(
          acs.get(subscriberId).fold(0)(_.size),
          numSubscribers,
          "number of Subscriber contracts")
        _ <- TestRunner.assertEqual(
          acs.get(copyId).fold(0)(_.size),
          numCopies,
          "number of Copies contracts")
      } yield ()
    }
    runner.genericTest(
      name,
      dar,
      triggerId,
      commands,
      numMessages,
      assertFinalState,
      assertFinalACS)
  }

  def createMaster(client: LedgerClient, owner: String, name: String, commandId: String)(
      implicit ec: ExecutionContext,
      mat: ActorMaterializer): Future[Unit] = {
    val commands = Seq(
      Command().withCreate(CreateCommand(
        templateId = Some(toApiIdentifier(masterId)),
        createArguments = Some(value.Record(
          recordId = Some(toApiIdentifier(masterId)),
          fields = Seq(
            value.RecordField("owner", Some(value.Value().withParty(owner))),
            value.RecordField("name", Some(value.Value().withText(name))),
            value.RecordField("info", Some(value.Value().withText("")))
          )
        ))
      )))

    for {
      _ <- client.commandClient.trackSingleCommand(
        SubmitRequest(
          commands = Some(Commands(
            ledgerId = client.ledgerId.unwrap,
            applicationId = runner.applicationId.unwrap,
            commandId = commandId,
            party = owner,
            ledgerEffectiveTime = Some(fromInstant(Instant.EPOCH)),
            maximumRecordTime = Some(fromInstant(Instant.EPOCH.plusSeconds(5))),
            commands = commands
          ))))
    } yield ()
  }

  def createSubscriber(client: LedgerClient, owner: String, obs: String, commandId: String)(
      implicit ec: ExecutionContext,
      mat: ActorMaterializer): Future[Unit] = {
    val commands = Seq(
      Command().withCreate(CreateCommand(
        templateId = Some(toApiIdentifier(subscriberId)),
        createArguments = Some(value.Record(
          recordId = Some(toApiIdentifier(subscriberId)),
          fields = Seq(
            value.RecordField("owner", Some(value.Value().withParty(owner))),
            value.RecordField("obs", Some(value.Value().withParty(obs))))
        ))
      )))

    for {
      _ <- client.commandClient.trackSingleCommand(
        SubmitRequest(
          commands = Some(Commands(
            ledgerId = client.ledgerId.unwrap,
            applicationId = runner.applicationId.unwrap,
            commandId = commandId,
            party = obs,
            ledgerEffectiveTime = Some(fromInstant(Instant.EPOCH)),
            maximumRecordTime = Some(fromInstant(Instant.EPOCH.plusSeconds(5))),
            commands = commands
          ))))
    } yield ()
  }

  def runTests() = {
    test(
      "1 master, 0 subscriber",
      // 1 for create of master
      // 1 for corresponding completion
      NumMessages(2),
      numMasters = 1,
      numSubscribers = 0,
      numCopies = 0,
      (client, party) => { implicit ec: ExecutionContext => implicit mat: ActorMaterializer =>
        {
          for {
            _ <- createMaster(client, owner = party, name = "master0", "0.0")
          } yield ()
        }
      }
    )
    test(
      "1 master, 1 subscriber",
      // 1 for create of master
      // 1 for create of subscriber
      // 2 for corresponding completions
      // 1 for create of copy
      // 1 for corresponding completion
      NumMessages(6),
      numMasters = 1,
      numSubscribers = 1,
      numCopies = 1,
      (client, party) => { implicit ec: ExecutionContext => implicit mat: ActorMaterializer =>
        {
          for {
            _ <- createMaster(client, owner = party, name = "master0", "1.0")
            _ <- createSubscriber(client, owner = party, obs = party, "1.1")
          } yield ()
        }
      }
    )
    test(
      "2 master, 1 subscriber",
      // 2 for create of master
      // 1 for create of subscriber
      // 3 for corresponding completions
      // 2 for create of copy
      // 2 for corresponding completion
      NumMessages(10),
      numMasters = 2,
      numSubscribers = 1,
      numCopies = 2,
      (client, party) => { implicit ec: ExecutionContext => implicit mat: ActorMaterializer =>
        {
          for {
            _ <- createMaster(client, owner = party, name = "master0", "2.0")
            _ <- createMaster(client, owner = party, name = "master1", "2.1")
            _ <- createSubscriber(client, owner = party, obs = party, "2.2")
          } yield ()
        }
      }
    )
  }
}

object TestMain {

  private val configParser = new scopt.OptionParser[Config]("acs_test") {
    head("acs_test")

    opt[Int]("target-port")
      .required()
      .action((p, c) => c.copy(ledgerPort = p))

    arg[File]("<dar>")
      .required()
      .action((d, c) => c.copy(darPath = d))
  }

  private val applicationId = ApplicationId("AscMain test")

  case class ActiveAssetMirrors(num: Int)
  case class NumMessages(num: Long)
  case class SuccessfulCompletions(num: Long)
  case class FailedCompletions(num: Long)

  def main(args: Array[String]): Unit = {
    configParser.parse(args, Config(0, null)) match {
      case None =>
        sys.exit(1)
      case Some(config) =>
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }
        val runner = new TestRunner(config.ledgerPort)
        // AcsTests(dar, runner).runTests()
        CopyTests(dar, runner).runTests()
    }
  }
}
