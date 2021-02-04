// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.io.IOException
import java.nio.file.{Files, FileVisitResult, Path, SimpleFileVisitor}
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
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundEach}
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
import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.sandboxnext.SandboxNextFixture
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import spray.json._

import scala.sys.process._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

final class IT
    extends AsyncFreeSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundEach
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
  private val tmpDir = Files.createTempDirectory("script_dump")
  private val damlc =
    BazelRunfiles.requiredResource(s"compiler/damlc/damlc$exe")
  private val damlScriptLib = BazelRunfiles.requiredResource("daml-script/daml/daml-script.dar")
  private def iouId(s: String) =
    api.Identifier(packageId, moduleName = "Iou", s)

  override protected def afterAll(): Unit = {
    super.afterAll()
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

  "Generated dump for IOU transfer compiles" in {
    for {
      client <- LedgerClient(channel, clientConfiguration)
      p1 <- client.partyManagementClient.allocateParty(None, None).map(_.party)
      p2 <- client.partyManagementClient.allocateParty(None, None).map(_.party)
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
                  api.Record(fields = Seq(api.RecordField(value = Some(api.Value().withParty(p2)))))
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
      _ <- Main.run(
        Config(
          ledgerHost = "localhost",
          ledgerPort = serverPort.value,
          parties = scala.List(p1, p2),
          outputPath = tmpDir,
          damlScriptLib = damlScriptLib.toString,
          sdkVersion = "0.0.0",
        )
      )

      _ = Seq[String](
        damlc.toString,
        "build",
        "--project-root",
        tmpDir.toString,
        "-o",
        tmpDir.resolve("dump.dar").toString,
      ).! shouldBe 0
      // Now run the DAML Script again
      p3 <- client.partyManagementClient.allocateParty(None, None).map(_.party)
      p4 <- client.partyManagementClient.allocateParty(None, None).map(_.party)
      encodedDar = DarReader().readArchiveFromFile(tmpDir.resolve("dump.dar").toFile).get
      dar: Dar[(PackageId, Package)] = encodedDar
        .map { case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive) }
      _ <- Runner.run(
        dar,
        Ref.Identifier(dar.main._1, Ref.QualifiedName.assertFromString("Dump:dump")),
        inputValue = Some(JsArray(JsString(p3), JsString(p4))),
        timeMode = ScriptTimeMode.WallClock,
        initialClients = Participants(
          default_participant = Some(new GrpcLedgerClient(client, ApplicationId("script"))),
          participants = Map.empty,
          party_participants = Map.empty,
        ),
      )
      transactions <- client.transactionClient
        .getTransactions(
          LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN),
          Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
          transactionFilter(p3),
        )
        .runWith(Sink.seq)
      _ = transactions should have length 4
      transactions <- client.transactionClient
        .getTransactions(
          LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN),
          Some(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
          transactionFilter(p4),
        )
        .runWith(Sink.seq)
      _ = transactions should have length 2
      acs <- client.activeContractSetClient
        .getActiveContracts(transactionFilter(p3))
        .runWith(Sink.seq)
      _ = acs.flatMap(_.activeContracts) should have size 2
      _ = println(acs)
      acs <- client.activeContractSetClient
        .getActiveContracts(transactionFilter(p4))
        .runWith(Sink.seq)
      _ = acs.flatMap(_.activeContracts) should have size 1
    } yield succeed
  }

  private def transactionFilter(p: Ref.Party) =
    TransactionFilter(filtersByParty = Seq(p -> Filters()).toMap)
}
