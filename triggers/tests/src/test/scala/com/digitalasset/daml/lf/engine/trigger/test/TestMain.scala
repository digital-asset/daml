// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import java.nio.file.{Path, Paths}
import java.io.File

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.api.v1.command_submission_service._
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.value
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.auth.TokenHolder
import com.daml.lf.PureCompiledPackages
import com.daml.lf.archive.DarReader
import com.daml.lf.archive.Dar
import com.daml.lf.language.Ast._
import com.daml.lf.archive.Decode
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Numeric
import com.daml.lf.data.Ref._
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.value.{Value => Lf}
import com.daml.grpc.adapter.AkkaExecutionSequencerPool
import com.daml.lf.speedy.Compiler
import com.daml.lf.speedy.SExpr
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.ledger.api.validation.ValueValidator
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import com.daml.platform.server.api.validation.FieldValidations.validateIdentifier
import com.daml.platform.services.time.TimeProviderType
import com.daml.lf.engine.trigger.{Runner, Trigger, TriggerMsg}

case class Config(
    ledgerPort: Int,
    darPath: Path,
    timeProviderType: TimeProviderType,
    accessTokenFile: Option[Path],
    cacrt: Option[File],
)

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

class TestRunner(val config: Config) extends StrictLogging {
  var partyCount = 0

  val applicationId = ApplicationId("Trigger Test Runner")
  val tokenHolder = config.accessTokenFile.map(new TokenHolder(_))

  val clientConfig = LedgerClientConfiguration(
    applicationId = applicationId.unwrap,
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = config.cacrt.flatMap(file =>
      TlsConfiguration.Empty.copy(trustCertCollectionFile = Some(file)).client),
    token = tokenHolder.flatMap(_.token)
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
      commands: (LedgerClient, String) => ExecutionContext => Materializer => Future[A],
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
    implicit val materializer: Materializer = Materializer(system)
    implicit val ec: ExecutionContext = system.dispatcher

    val party = getNewParty()
    val clientF =
      LedgerClient.singleHost("localhost", config.ledgerPort, clientConfig)(ec, sequencer)

    // We resolve this promise once we have queried the ACS to initialize the trigger.
    // This ensures that the commands will only be executed afterwards.
    val acsPromise = Promise[Unit]()

    val darMap = dar.all.toMap
    val compiler = Compiler(darMap)
    val compiledPackages =
      PureCompiledPackages(darMap, compiler.compilePackages(darMap.keys)).right.get
    val trigger = Trigger.fromIdentifier(compiledPackages, triggerId) match {
      case Left(err) => throw new RuntimeException(err)
      case Right(trigger) => trigger
    }

    val triggerFlow: Future[SExpr] = for {
      client <- clientF
      runner = new Runner(
        compiledPackages,
        trigger,
        client,
        config.timeProviderType,
        applicationId,
        party)
      (acs, offset) <- runner.queryACS()
      _ = acsPromise.success(())
      finalState <- runner
        .runWithACS(
          acs,
          offset,
          msgFlow = Flow[TriggerMsg].take(numMessages.num)
        )
        ._2
    } yield finalState
    val commandsFlow: Future[A] = for {
      _ <- acsPromise.future
      r <- clientF.flatMap(client => commands(client, party)(ec)(materializer))
    } yield r
    triggerFlow.failed.foreach(err => {
      logger.error("Trigger flow failed", err)
      system.terminate
    })
    commandsFlow.failed.foreach(err => {
      logger.error("Commands flow failed", err)
      system.terminate
    })
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
    entityName = "Asset"
  )

  def test(
      name: String,
      numMessages: NumMessages,
      numSuccCompletions: SuccessfulCompletions,
      numFailedCompletions: FailedCompletions,
      commands: (LedgerClient, String) => ExecutionContext => Materializer => Future[
        (Set[String], ActiveAssetMirrors)]) = {
    def assertFinalState(finalState: SExpr, commandsR: (Set[String], ActiveAssetMirrors)) = {
      finalState match {
        case SEValue(SRecord(_, _, vals)) =>
          for {
            _ <- TestRunner.assertEqual(vals.size, 5, "number of record fields")
            activeAssets <- vals.get(0) match {
              case SList(v) =>
                Right(
                  v.map(x =>
                      x.asInstanceOf[SContractId].value.asInstanceOf[Lf.AbsoluteContractId].coid)
                    .toSet)
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
      materializer: Materializer): Future[String] = {
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
      r <- client.commandClient
        .withTimeProvider(Some(Runner.getTimeProvider(runner.config.timeProviderType)))
        .trackSingleCommand(
          SubmitRequest(
            commands = Some(
              Commands(
                ledgerId = client.ledgerId.unwrap,
                applicationId = runner.applicationId.unwrap,
                commandId = commandId,
                party = party,
                commands = commands
              ))))
      t <- client.transactionClient.getFlatTransactionById(r.transactionId, Seq(party))
    } yield t.transaction.get.events.head.getCreated.contractId
  }

  // Archive the contract with the given id.
  def archive(client: LedgerClient, party: String, commandId: String, contractId: String)(
      implicit ec: ExecutionContext,
      materializer: Materializer): Future[Unit] = {
    val archiveVal = Some(
      value
        .Value()
        .withRecord(value.Record(
          recordId = Some(value.Identifier(
            packageId = PackageId.assertFromString(
              "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662"),
            moduleName = "DA.Internal.Template",
            entityName = "Archive"
          )),
          fields = Seq()
        )))
    val commands = Seq(
      Command().withExercise(ExerciseCommand(
        templateId = Some(
          value.Identifier(
            packageId = dar.main._1,
            moduleName = "ACS",
            entityName = "Asset",
          )),
        contractId = contractId,
        choice = "Archive",
        choiceArgument = archiveVal,
      )))
    for {
      comp <- client.commandClient
        .withTimeProvider(Some(Runner.getTimeProvider(runner.config.timeProviderType)))
        .trackSingleCommand(
          SubmitRequest(
            commands = Some(
              Commands(
                ledgerId = client.ledgerId.unwrap,
                applicationId = runner.applicationId.unwrap,
                commandId = commandId,
                party = party,
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
      SuccessfulCompletions(2),
      FailedCompletions(0),
      (client, party) => { implicit ec: ExecutionContext => implicit mat: Materializer =>
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
      SuccessfulCompletions(4),
      FailedCompletions(0),
      (client, party) => { implicit ec: ExecutionContext => implicit mat: Materializer =>
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
      SuccessfulCompletions(4),
      FailedCompletions(0),
      (client, party) => { implicit ec: ExecutionContext => implicit mat: Materializer =>
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
    Identifier(dar.main._1, QualifiedName.assertFromString("CopyTrigger:copyTrigger"))

  val originalId = Identifier(dar.main._1, QualifiedName.assertFromString("CopyTrigger:Original"))

  val copyId = Identifier(dar.main._1, QualifiedName.assertFromString("CopyTrigger:Copy"))

  val subscriberId =
    Identifier(dar.main._1, QualifiedName.assertFromString("CopyTrigger:Subscriber"))

  def test(
      name: String,
      numMessages: NumMessages,
      numOriginals: Int,
      numSubscribers: Int,
      numCopies: Int,
      commands: (LedgerClient, String) => ExecutionContext => Materializer => Future[Unit]) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = Right(())
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = {
      for {
        _ <- TestRunner.assertEqual(
          acs.get(originalId).fold(0)(_.size),
          numOriginals,
          "number of Original contracts")
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

  def createOriginal(client: LedgerClient, owner: String, name: String, commandId: String)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Unit] = {
    val commands = Seq(
      Command().withCreate(CreateCommand(
        templateId = Some(toApiIdentifier(originalId)),
        createArguments = Some(value.Record(
          recordId = Some(toApiIdentifier(originalId)),
          fields = Seq(
            value.RecordField("owner", Some(value.Value().withParty(owner))),
            value.RecordField("name", Some(value.Value().withText(name))),
            value.RecordField("textdata", Some(value.Value().withText("")))
          )
        ))
      )))

    for {
      _ <- client.commandClient
        .withTimeProvider(Some(Runner.getTimeProvider(runner.config.timeProviderType)))
        .trackSingleCommand(
          SubmitRequest(
            commands = Some(
              Commands(
                ledgerId = client.ledgerId.unwrap,
                applicationId = runner.applicationId.unwrap,
                commandId = commandId,
                party = owner,
                commands = commands
              ))))
    } yield ()
  }

  def createSubscriber(
      client: LedgerClient,
      subscriber: String,
      subscribedTo: String,
      commandId: String)(implicit ec: ExecutionContext, mat: Materializer): Future[Unit] = {
    val commands = Seq(
      Command().withCreate(CreateCommand(
        templateId = Some(toApiIdentifier(subscriberId)),
        createArguments = Some(value.Record(
          recordId = Some(toApiIdentifier(subscriberId)),
          fields = Seq(
            value.RecordField("subscriber", Some(value.Value().withParty(subscriber))),
            value.RecordField("subscribedTo", Some(value.Value().withParty(subscribedTo)))
          )
        ))
      )))

    for {
      _ <- client.commandClient
        .withTimeProvider(Some(Runner.getTimeProvider(runner.config.timeProviderType)))
        .trackSingleCommand(
          SubmitRequest(
            commands = Some(
              Commands(
                ledgerId = client.ledgerId.unwrap,
                applicationId = runner.applicationId.unwrap,
                commandId = commandId,
                party = subscriber,
                commands = commands
              ))))
    } yield ()
  }

  def runTests() = {
    test(
      "1 original, 0 subscriber",
      // 1 for create of original
      // 1 for corresponding completion
      NumMessages(2),
      numOriginals = 1,
      numSubscribers = 0,
      numCopies = 0,
      (client, party) => { implicit ec: ExecutionContext => implicit mat: Materializer =>
        {
          for {
            _ <- createOriginal(client, owner = party, name = "original0", "0.0")
          } yield ()
        }
      }
    )
    test(
      "1 original, 1 subscriber",
      // 1 for create of original
      // 1 for create of subscriber
      // 2 for corresponding completions
      // 1 for create of copy
      // 1 for corresponding completion
      NumMessages(6),
      numOriginals = 1,
      numSubscribers = 1,
      numCopies = 1,
      (client, party) => { implicit ec: ExecutionContext => implicit mat: Materializer =>
        {
          for {
            _ <- createOriginal(client, owner = party, name = "original0", "1.0")
            _ <- createSubscriber(client, subscriber = party, subscribedTo = party, "1.1")
          } yield ()
        }
      }
    )
    test(
      "2 original, 1 subscriber",
      // 2 for create of original
      // 1 for create of subscriber
      // 3 for corresponding completions
      // 2 for create of copy
      // 2 for corresponding completion
      NumMessages(10),
      numOriginals = 2,
      numSubscribers = 1,
      numCopies = 2,
      (client, party) => { implicit ec: ExecutionContext => implicit mat: Materializer =>
        {
          for {
            _ <- createOriginal(client, owner = party, name = "original0", "2.0")
            _ <- createOriginal(client, owner = party, name = "original1", "2.1")
            _ <- createSubscriber(client, subscriber = party, subscribedTo = party, "2.2")
          } yield ()
        }
      }
    )
  }
}

case class RetryTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  val triggerId: Identifier =
    Identifier(dar.main._1, QualifiedName.assertFromString("Retry:retryTrigger"))

  val tId = Identifier(dar.main._1, QualifiedName.assertFromString("Retry:T"))

  val doneId = Identifier(dar.main._1, QualifiedName.assertFromString("Retry:Done"))

  def test(name: String, numMessages: NumMessages, numT: Int, numDone: Int) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = Right(())
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = {
      for {
        _ <- TestRunner.assertEqual(acs.get(tId).fold(0)(_.size), numT, "number of T contracts")
        _ <- TestRunner.assertEqual(
          acs.get(doneId).fold(0)(_.size),
          numDone,
          "number of Done contracts")
      } yield ()
    }
    runner.genericTest(name, dar, triggerId, (_, _) => { implicit ec: ExecutionContext =>
      { implicit mat: Materializer =>
        Future {}
      }
    }, numMessages, assertFinalState, assertFinalACS)
  }

  def runTests() = {
    test(
      "3 retries",
      // 1 for create of T
      // 1 for completion
      // 3 failed completion for exercises
      // 1 for create of Done
      // 1 for corresponding completion
      NumMessages(7),
      numT = 1,
      numDone = 1,
    )
  }
}

case class ExerciseByKeyTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  val triggerId: Identifier =
    Identifier(dar.main._1, QualifiedName.assertFromString("ExerciseByKey:exerciseByKeyTrigger"))

  val tId = Identifier(dar.main._1, QualifiedName.assertFromString("ExerciseByKey:T"))

  val tPrimeId = Identifier(dar.main._1, QualifiedName.assertFromString("ExerciseByKey:T_"))

  def test(name: String, numMessages: NumMessages, numT: Int, numTPrime: Int) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = Right(())
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = {
      for {
        _ <- TestRunner.assertEqual(acs.get(tId).fold(0)(_.size), numT, "number of T contracts")
        _ <- TestRunner.assertEqual(
          acs.get(tPrimeId).fold(0)(_.size),
          numTPrime,
          "number of Done contracts")
      } yield ()
    }
    runner.genericTest(name, dar, triggerId, (_, _) => { implicit ec: ExecutionContext =>
      { implicit mat: Materializer =>
        Future {}
      }
    }, numMessages, assertFinalState, assertFinalACS)
  }

  def runTests() = {
    test(
      "1 exerciseByKey",
      // 1 for create of T
      // 1 for completion
      // 1 for exerciseByKey
      // 1 for corresponding completion
      NumMessages(4),
      numT = 1,
      numTPrime = 1,
    )
  }
}

case class NumericTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  val triggerId: Identifier =
    Identifier(dar.main._1, QualifiedName.assertFromString("Numeric:test"))

  val tId = Identifier(dar.main._1, QualifiedName.assertFromString("Numeric:T"))

  def test(name: String, numMessages: NumMessages, tValues: Set[Numeric]) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = Right(())
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = {
      val activeTs = acs(tId)
      val actualTValues =
        activeTs.map({ case (_, r) => r.fields(1)._2.asInstanceOf[Lf.ValueNumeric].value }).toSet
      TestRunner.assertEqual(actualTValues, tValues, "T values")
    }
    runner.genericTest(name, dar, triggerId, (_, _) => { implicit ec: ExecutionContext =>
      { implicit mat: Materializer =>
        Future {}
      }
    }, numMessages, assertFinalState, assertFinalACS)
  }

  def runTests() = {
    test(
      "numeric",
      // 1 for create of T
      // 1 for completion
      // 1 for exercise on T
      // 1 for completion
      NumMessages(4),
      // We don’t check that we get the right numeric scale here since we only call validateRecord in our
      // tests and that would only test the ledger API and not triggers.
      Set(Numeric.assertFromUnscaledBigDecimal(1.06), Numeric.assertFromUnscaledBigDecimal(2.06))
    )
  }
}

case class CommandIdTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  val triggerId: Identifier =
    Identifier(dar.main._1, QualifiedName.assertFromString("CommandId:test"))

  val tId = Identifier(dar.main._1, QualifiedName.assertFromString("CommandId:T"))

  def test(name: String, numMessages: NumMessages, cmdIds: List[String]) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = {
      val expected = SEValue(SList(FrontStack(cmdIds.map(SText))))
      TestRunner.assertEqual(finalState, expected, "list of command ids")
    }
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = Right(())
    runner.genericTest(name, dar, triggerId, (_, _) => { implicit ec: ExecutionContext =>
      { implicit mat: Materializer =>
        Future {}
      }
    }, numMessages, assertFinalState, assertFinalACS)
  }

  def runTests() = {
    test(
      "command-id",
      // 1 for create of T
      // 1 for completion
      // 1 for archive on T
      // 1 for completion
      NumMessages(4),
      List("myexerciseid", "mycreateid")
    )
  }
}

case class PendingTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  val triggerId: Identifier =
    Identifier(dar.main._1, QualifiedName.assertFromString("PendingSet:booTrigger"))

  val fooId = Identifier(dar.main._1, QualifiedName.assertFromString("PendingSet:Foo"))
  val booId = Identifier(dar.main._1, QualifiedName.assertFromString("PendingSet:Boo"))
  val doneId = Identifier(dar.main._1, QualifiedName.assertFromString("PendingSet:Done"))

  def test(name: String, numMessages: NumMessages, expectedNumFoo: Int, expectedNumBoo: Int) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = Right(())
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = {
      val numDone = acs.get(doneId).fold(0)(_.size)
      val numFoo = acs.get(fooId).fold(0)(_.size)
      val numBoo = acs.get(booId).fold(0)(_.size)
      TestRunner.assertEqual(numDone, 1, "active Done")
      TestRunner.assertEqual(numFoo, expectedNumFoo, "active Foo")
      TestRunner.assertEqual(numBoo, expectedNumBoo, "active Boo")
    }
    runner.genericTest(name, dar, triggerId, (_, _) => {
      implicit ec: ExecutionContext => implicit mat: Materializer =>
        Future.unit
    }, numMessages, assertFinalState, assertFinalACS)
  }

  def runTests() = {
    test(
      "pending set",
      // 1 for the creates at startup
      // 1 for the completion from startup
      // 1 for the exercise in the trigger
      // 1 for the completion in the trigger
      NumMessages(4),
      expectedNumFoo = 0,
      expectedNumBoo = 1
    )
  }
}

case class TemplateFilterTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  case class ActiveAssetMirrors(num: Int)
  case class SuccessfulCompletions(num: Long)
  case class FailedCompletions(num: Long)

  val oneId = value.Identifier(
    packageId = dar.main._1,
    moduleName = "TemplateIdFilter",
    entityName = "One"
  )
  val twoId = value.Identifier(
    packageId = dar.main._1,
    moduleName = "TemplateIdFilter",
    entityName = "Two"
  )

  def test(
      name: String,
      triggerName: String,
      numMessages: NumMessages,
      doneOnes: Int,
      doneTwos: Int) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = Right(())
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = {
      val activeDoneOne = acs
        .get(Identifier(dar.main._1, QualifiedName.assertFromString("TemplateIdFilter:DoneOne")))
        .fold(0)(_.size)
      val activeDoneTwo = acs
        .get(Identifier(dar.main._1, QualifiedName.assertFromString("TemplateIdFilter:DoneTwo")))
        .fold(0)(_.size)
      for {
        _ <- TestRunner.assertEqual(activeDoneOne, doneOnes, "DoneOne")
        _ <- TestRunner.assertEqual(activeDoneTwo, doneTwos, "DoneTwo")
      } yield ()
    }
    def cmds(client: LedgerClient, party: String) = { implicit ec: ExecutionContext =>
      { implicit mat: Materializer =>
        for {
          _ <- createOne(client, party, "createOne")
          _ <- createTwo(client, party, "createTwo")
        } yield ()
      }
    }
    val triggerId =
      Identifier(dar.main._1, QualifiedName.assertFromString(s"TemplateIdFilter:$triggerName"))
    runner.genericTest(name, dar, triggerId, cmds, numMessages, assertFinalState, assertFinalACS)
  }

  def create(client: LedgerClient, party: String, commandId: String, templateId: value.Identifier)(
      implicit ec: ExecutionContext,
      materializer: Materializer): Future[Unit] = {
    val commands = Seq(
      Command().withCreate(CreateCommand(
        templateId = Some(templateId),
        createArguments = Some(
          value.Record(
            recordId = Some(templateId),
            fields = Seq(
              value.RecordField(
                "p",
                Some(value.Value().withParty(party))
              )
            )
          )),
      )))
    for {
      r <- client.commandClient
        .withTimeProvider(Some(Runner.getTimeProvider(runner.config.timeProviderType)))
        .trackSingleCommand(
          SubmitRequest(
            commands = Some(
              Commands(
                ledgerId = client.ledgerId.unwrap,
                applicationId = runner.applicationId.unwrap,
                commandId = commandId,
                party = party,
                commands = commands
              ))))
    } yield ()
  }

  def createOne(client: LedgerClient, party: String, commandId: String)(
      implicit ec: ExecutionContext,
      materializer: Materializer): Future[Unit] =
    create(client, party, commandId, oneId)

  def createTwo(client: LedgerClient, party: String, commandId: String)(
      implicit ec: ExecutionContext,
      materializer: Materializer): Future[Unit] = create(client, party, commandId, twoId)

  def runTests() = {
    test(
      "Filter to One",
      "testOne",
      // 2 for the creates from the test
      // 2 for the completions from the test
      // 1 for the create in the trigger
      // 1 for the completion from the trigger
      NumMessages(6),
      doneOnes = 1,
      doneTwos = 0
    )
    test(
      "Filter to Two",
      "testTwo",
      // 2 for the creates from the test
      // 2 for the completions from the test
      // 1 for the create in the trigger
      // 1 for the completion from the trigger
      NumMessages(6),
      doneOnes = 0,
      doneTwos = 1
    )
  }
}

case class TimeTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  val triggerId: Identifier =
    Identifier(dar.main._1, QualifiedName.assertFromString("Time:test"))

  val tId = Identifier(dar.main._1, QualifiedName.assertFromString("Time:T"))

  def test(name: String, triggerName: String, numMessages: NumMessages) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = {
      finalState match {
        case SEValue(SRecord(_, _, values)) =>
          for {
            _ <- TestRunner.assertEqual(values.size, 2, "number of tuple elements")
            _ <- values.get(1) match {
              case SList(items) =>
                for {
                  _ <- TestRunner.assertEqual(items.length, 2, "number of time values")
                  times <- items.pop match {
                    case Some((STimestamp(timeA), tail)) =>
                      tail.pop match {
                        case Some((STimestamp(timeB), _)) => Right((timeA, timeB))
                        case _ => Left(s"Expected at least two timestamps")
                      }
                    case _ => Left(s"Expected at least one timestamp")
                  }
                  (timeA, timeB) = times
                  _ <- runner.config.timeProviderType match {
                    case TimeProviderType.Static =>
                      TestRunner.assertEqual(timeA, timeB, "static times")
                    case _ =>
                      // Given the limited resolution it can happen that timeA == timeB
                      if (!(timeA >= timeB)) {
                        Left(s"Second create at $timeA should have happened after first $timeB")
                      } else {
                        Right(())
                      }
                  }
                } yield ()
              case _ => Left(s"Expected a list but got ${values.get(1)}")
            }
          } yield ()
        case _ => Left(s"Expected a tuple but got $finalState")
      }
    }
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = {
      Right(())
    }
    def cmds(client: LedgerClient, party: String) = { implicit ec: ExecutionContext =>
      { implicit mat: Materializer =>
        Future {}
      }
    }
    val triggerId =
      Identifier(dar.main._1, QualifiedName.assertFromString(s"Time:$triggerName"))
    runner.genericTest(name, dar, triggerId, cmds, numMessages, assertFinalState, assertFinalACS)
  }

  def runTests() = {
    test(
      "Time",
      "test",
      // 2 creates and 2 completions
      NumMessages(4)
    )
  }
}

case class HeartbeatTests(dar: Dar[(PackageId, Package)], runner: TestRunner) {

  val triggerId: Identifier =
    Identifier(dar.main._1, QualifiedName.assertFromString("Heartbeat:test"))

  def test(name: String, triggerName: String, numMessages: NumMessages) = {
    def assertFinalState(finalState: SExpr, commandsR: Unit) = {
      finalState match {
        case SEValue(SInt64(count)) =>
          TestRunner.assertEqual(count, 2, "number of heartbeats")
        case _ => Left(s"Expected Int64 but got $finalState")
      }
    }
    def assertFinalACS(
        acs: Map[Identifier, Seq[(String, Lf.ValueRecord[Lf.AbsoluteContractId])]],
        commandsR: Unit) = {
      Right(())
    }
    def cmds(client: LedgerClient, party: String) = { implicit ec: ExecutionContext =>
      { implicit mat: Materializer =>
        Future {}
      }
    }
    runner.genericTest(name, dar, triggerId, cmds, numMessages, assertFinalState, assertFinalACS)
  }

  def runTests() = {
    test(
      "Heartbeat",
      "test",
      // 2 heartbeats
      NumMessages(2)
    )
  }
}

object TestMain {

  private val configParser = new scopt.OptionParser[Config]("acs_test") {
    head("acs_test")

    opt[Int]("target-port")
      .required()
      .action((p, c) => c.copy(ledgerPort = p))

    arg[String]("<dar>")
      .required()
      .action((d, c) => c.copy(darPath = Paths.get(d)))

    opt[Unit]('w', "wall-clock-time")
      .action { (t, c) =>
        c.copy(timeProviderType = TimeProviderType.WallClock)
      }
      .text("Use wall clock time (UTC). When not provided, static time is used.")
    opt[String]("access-token-file")
      .action { (f, c) =>
        c.copy(accessTokenFile = Some(Paths.get(f)))
      }

    opt[File]("cacrt")
      .optional()
      .action((d, c) => c.copy(cacrt = Some(d)))
  }

  private val applicationId = ApplicationId("AscMain test")

  case class ActiveAssetMirrors(num: Int)
  case class NumMessages(num: Long)
  case class SuccessfulCompletions(num: Long)
  case class FailedCompletions(num: Long)

  def main(args: Array[String]): Unit = {
    configParser.parse(args, Config(0, null, TimeProviderType.Static, None, None)) match {
      case None =>
        sys.exit(1)
      case Some(config) =>
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath.toFile).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }
        val runner = new TestRunner(config)
        AcsTests(dar, runner).runTests()
        CopyTests(dar, runner).runTests()
        RetryTests(dar, runner).runTests()
        ExerciseByKeyTests(dar, runner).runTests()
        HeartbeatTests(dar, runner).runTests()
        NumericTests(dar, runner).runTests()
        CommandIdTests(dar, runner).runTests()
        PendingTests(dar, runner).runTests()
        TemplateFilterTests(dar, runner).runTests()
        TimeTests(dar, runner).runTests()
    }
  }
}
