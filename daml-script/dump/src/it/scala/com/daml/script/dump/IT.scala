// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.io.IOException
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.language.Ast.Package
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.script.{GrpcLedgerClient, Participants, Runner, ScriptTimeMode}
import com.daml.ledger.api.domain
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundAll}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.{value => api}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.testing.utils.TransactionEq
import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.SdkVersion
import scalaz.syntax.tag._
import scalaz.std.scalaFuture._
import scalaz.std.list._
import scalaz.syntax.traverse._
import spray.json._

import scala.concurrent.Future
import scala.sys.process._
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

final class IT
    extends AsyncFreeSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with BeforeAndAfterEach
    with SuiteResourceManagementAroundAll
    with SandboxNextFixture
    with TestCommands {
  private val appId = domain.ApplicationId("script-dump")
  private val clientConfiguration = LedgerClientConfiguration(
    applicationId = appId.unwrap,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
    token = None,
  )
  val isWindows: Boolean = sys.props("os.name").toLowerCase.contains("windows")
  val exe = if (isWindows) { ".exe" }
  else ""
  private var tmpDir: Path = null
  private val damlc =
    BazelRunfiles.requiredResource(s"compiler/damlc/damlc$exe")
  private val damlScriptLib = BazelRunfiles.requiredResource("daml-script/daml/daml-script.dar")
  private def iouId(s: String) =
    api.Identifier(packageId, moduleName = "Iou", s)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    tmpDir = Files.createTempDirectory("script_dump")
  }
  override protected def afterEach(): Unit = {
    super.afterEach()
    deleteRecursively(tmpDir)
  }

  // TODO(MK) Put this somewhere in //libs-scala
  private def deleteRecursively(dir: Path): Unit = {
    Files.walkFileTree(
      dir,
      new SimpleFileVisitor[Path] {
        override def postVisitDirectory(dir: Path, exc: IOException) = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }

        override def visitFile(file: Path, attrs: BasicFileAttributes) = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }
      },
    )
    ()
  }

  private def submit(client: LedgerClient, p: Ref.Party, cmd: Command) =
    client.commandServiceClient.submitAndWaitForTransaction(
      SubmitAndWaitRequest(
        Some(
          Commands(
            ledgerId = client.ledgerId.unwrap,
            applicationId = appId.unwrap,
            commandId = UUID.randomUUID().toString(),
            party = p,
            commands = Seq(cmd),
          )
        )
      )
    )

  private def collectTrees(client: LedgerClient, parties: List[Ref.Party]) =
    client.transactionClient
      .getTransactionTrees(
        LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN),
        Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
        transactionFilter(parties: _*),
      )
      .runWith(Sink.seq)

  private def allocateParties(client: LedgerClient, numParties: Int): Future[List[Ref.Party]] =
    List
      .range(0, numParties)
      .traverse(_ => client.partyManagementClient.allocateParty(None, None).map(_.party))

  private val ledgerBegin = LedgerOffset(
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
  )
  private val ledgerEnd = LedgerOffset(
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END)
  )
  private def ledgerOffset(offset: String) = LedgerOffset(LedgerOffset.Value.Absolute(offset))

  private def createScriptDump(
      parties: List[Ref.Party],
      offset: LedgerOffset,
  ): Future[Dar[(PackageId, Package)]] = for {
    // build script dump
    _ <- Main.run(
      Config(
        ledgerHost = "localhost",
        ledgerPort = serverPort.value,
        parties = parties,
        start = offset,
        end = ledgerEnd,
        outputPath = tmpDir,
        damlScriptLib = damlScriptLib.toString,
        sdkVersion = SdkVersion.sdkVersion,
      )
    )
    // compile script dump
    _ = Seq[String](
      damlc.toString,
      "build",
      "--project-root",
      tmpDir.toString,
      "-o",
      tmpDir.resolve("dump.dar").toString,
    ).! shouldBe 0
    // load DAR
    encodedDar = DarReader().readArchiveFromFile(tmpDir.resolve("dump.dar").toFile).get
    dar = encodedDar.map { case (pkgId, pkgArchive) =>
      Decode.readArchivePayload(pkgId, pkgArchive)
    }
  } yield dar

  private def runScriptDump(
      client: LedgerClient,
      parties: List[Ref.Party],
      dar: Dar[(PackageId, Package)],
  ): Future[Unit] = for {
    _ <- Runner.run(
      dar,
      Ref.Identifier(dar.main._1, Ref.QualifiedName.assertFromString("Dump:dump")),
      inputValue = Some(JsArray(parties.map(JsString(_)).toVector)),
      timeMode = ScriptTimeMode.WallClock,
      initialClients = Participants(
        default_participant = Some(new GrpcLedgerClient(client, ApplicationId("script"))),
        participants = Map.empty,
        party_participants = Map.empty,
      ),
    )
  } yield ()

  private def testOffset(
      numParties: Int,
      skip: Int,
  )(f: (LedgerClient, Seq[Ref.Party]) => Future[Unit]): Future[Assertion] =
    for {
      client <- LedgerClient(channel, clientConfiguration)
      parties <- allocateParties(client, numParties)
      // setup
      _ <- f(client, parties)
      before <- collectTrees(client, parties)
      // Reproduce ACS up to offset and transaction trees after offset.
      offset =
        if (skip == 0) { ledgerBegin }
        else { ledgerOffset(before(skip - 1).offset) }
      beforeCmp = before.drop(skip)
      // reproduce from dump
      dar <- createScriptDump(parties, offset)
      newParties <- allocateParties(client, numParties)
      _ <- runScriptDump(client, newParties, dar)
      // check that the new transaction trees are the same
      after <- collectTrees(client, newParties)
      afterCmp = after.drop(after.length - beforeCmp.length)
    } yield {
      TransactionEq.equivalent(beforeCmp, afterCmp).fold(fail(_), _ => succeed)
    }

  private def testIou: (LedgerClient, Seq[Ref.Party]) => Future[Unit] = {
    case (client, Seq(p1, p2)) =>
      for {
        t0 <- submit(
          client,
          p1,
          Command().withCreate(
            CreateCommand(
              templateId = Some(iouId("Iou")),
              createArguments = Some(
                api.Record(
                  fields = Seq(
                    api.RecordField("issuer", Some(api.Value().withParty(p1))),
                    api.RecordField("owner", Some(api.Value().withParty(p1))),
                    api.RecordField("currency", Some(api.Value().withText("USD"))),
                    api.RecordField("amount", Some(api.Value().withNumeric("100"))),
                    api.RecordField("observers", Some(api.Value().withList(api.List()))),
                  )
                )
              ),
            )
          ),
        )
        cid0 = t0.getTransaction.events(0).getCreated.contractId
        t1 <- submit(
          client,
          p1,
          Command().withExercise(
            ExerciseCommand(
              templateId = Some(iouId("Iou")),
              choice = "Iou_Split",
              contractId = cid0,
              choiceArgument = Some(
                api
                  .Value()
                  .withRecord(
                    api.Record(fields =
                      Seq(api.RecordField(value = Some(api.Value().withNumeric("50"))))
                    )
                  )
              ),
            )
          ),
        )
        cid1 = t1.getTransaction.events(1).getCreated.contractId
        cid2 = t1.getTransaction.events(2).getCreated.contractId
        t2 <- submit(
          client,
          p1,
          Command().withExercise(
            ExerciseCommand(
              templateId = Some(iouId("Iou")),
              choice = "Iou_Transfer",
              contractId = cid2,
              choiceArgument = Some(
                api
                  .Value()
                  .withRecord(
                    api.Record(fields =
                      Seq(api.RecordField(value = Some(api.Value().withParty(p2))))
                    )
                  )
              ),
            )
          ),
        )
        cid3 = t2.getTransaction.events(1).getCreated.contractId
        _ <- submit(
          client,
          p2,
          Command().withExercise(
            ExerciseCommand(
              templateId = Some(iouId("IouTransfer")),
              choice = "IouTransfer_Accept",
              contractId = cid3,
              choiceArgument = Some(api.Value().withRecord(api.Record())),
            )
          ),
        )
      } yield ()
  }

  "Generated dump for IOU transfer compiles" - {
    "offset 0 - empty ACS" in { testOffset(2, 0)(testIou) }
    "offset 2 - skip split" in { testOffset(2, 2)(testIou) }
    "offset 4 - no trees" in { testOffset(2, 4)(testIou) }
  }

  private def transactionFilter(ps: Ref.Party*) =
    TransactionFilter(filtersByParty = ps.map(p => p -> Filters()).toMap)
}
