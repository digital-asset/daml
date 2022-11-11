// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.nio.file.{Files, Path}
import java.util.UUID
import akka.stream.scaladsl.Sink
import com.daml.SdkVersion
import com.daml.bazeltools.BazelRunfiles
import com.daml.fs.Utils.deleteRecursively
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundAll}
import com.daml.ledger.api.tls.TlsConfiguration
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
import com.daml.lf.archive.{Dar, DarDecoder}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.script.ledgerinteraction.{GrpcLedgerClient, ScriptTimeMode}
import com.daml.lf.engine.script.{Participants, Runner}
import com.daml.lf.language.Ast.Package
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.sandbox.services.TestCommands
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.syntax.tag._
import spray.json._

import scala.concurrent.Future
import scala.sys.process._

trait ReproducesTransactions
    extends AsyncFreeSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with BeforeAndAfterEach
    with SuiteResourceManagementAroundAll
    with SandboxFixture
    with SandboxBackend.Postgresql
    with StrictLogging
    with TestCommands {

  private val appId = Ref.ApplicationId.assertFromString("script-export")
  private val clientConfiguration = LedgerClientConfiguration(
    applicationId = appId,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
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
    tmpDir = Files.createTempDirectory("script_export")
  }
  override protected def afterEach(): Unit = {
    super.afterEach()
    deleteRecursively(tmpDir)
  }

  private def submit(client: LedgerClient, p: Ref.Party, cmd: Command) =
    client.commandServiceClient.submitAndWaitForTransaction(
      SubmitAndWaitRequest(
        Some(
          Commands(
            ledgerId = client.ledgerId.unwrap,
            applicationId = appId,
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
    for {
      _ <- Future { logger.debug("Allocating parties") }
      ps <- List
        .range(0, numParties)
        // Allocate parties sequentially to avoid timeouts on CI.
        .foldLeft[Future[List[Ref.Party]]](Future.successful(Nil)) { case (acc, _) =>
          for {
            ps <- acc
            p <- client.partyManagementClient.allocateParty(None, None).map(_.party)
          } yield ps :+ p
        }
      _ = logger.debug("Allocated parties")
    } yield ps

  private val ledgerBegin = LedgerOffset(
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
  )
  private val ledgerEnd = LedgerOffset(
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END)
  )
  private def ledgerOffset(offset: String) = LedgerOffset(LedgerOffset.Value.Absolute(offset))

  private def createScriptExport(
      parties: List[Ref.Party],
      offset: LedgerOffset,
  ): Future[Dar[(PackageId, Package)]] = for {
    // build script export
    _ <- Main.run(
      Config(
        ledgerHost = "localhost",
        ledgerPort = serverPort.value,
        tlsConfig = TlsConfiguration(false, None, None, None),
        accessToken = None,
        partyConfig = PartyConfig(
          parties = Party.subst(parties),
          allParties = false,
        ),
        start = offset,
        end = ledgerEnd,
        maxInboundMessageSize = Config.DefaultMaxInboundMessageSize,
        exportType = Some(
          ExportScript(
            outputPath = tmpDir,
            acsBatchSize = 2,
            setTime = true,
            damlScriptLib = damlScriptLib.toString,
            sdkVersion = SdkVersion.sdkVersion,
          )
        ),
      )
    )
    // compile script export
    _ = Seq[String](
      damlc.toString,
      "build",
      "--project-root",
      tmpDir.toString,
      "-o",
      tmpDir.resolve("export.dar").toString,
    ).! shouldBe 0
    // load DAR
    dar <- Future.fromTry(DarDecoder.readArchiveFromFile(tmpDir.resolve("export.dar").toFile).toTry)
  } yield dar

  private def runScriptExport(
      client: LedgerClient,
      partiesMap: Map[Ref.Party, Ref.Party],
      dar: Dar[(PackageId, Package)],
  ): Future[Unit] = for {
    _ <- Runner.run(
      dar,
      Ref.Identifier(dar.main._1, Ref.QualifiedName.assertFromString("Export:export")),
      inputValue = Some(
        JsObject(
          "parties" -> JsObject(partiesMap.map { case (oldP, newP) =>
            oldP -> JsString(newP)
          }.toSeq: _*),
          "contracts" -> JsObject(),
        )
      ),
      timeMode = ScriptTimeMode.Static,
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
      // reproduce from export
      dar <- createScriptExport(parties, offset)
      newParties <- allocateParties(client, numParties)
      partiesMap = parties.zip(newParties).toMap
      _ <- runScriptExport(client, partiesMap, dar)
      // check that the new transaction trees are the same
      after <- collectTrees(client, newParties)
      afterCmp = after.drop(after.length - beforeCmp.length)
    } yield {
      TransactionEq.equivalent(beforeCmp, afterCmp).fold(fail(_), _ => succeed)
    }

  @scala.annotation.nowarn("msg=match may not be exhaustive")
  private def testIou: (LedgerClient, Seq[Ref.Party]) => Future[Unit] = {
    case (client, Seq(p1, p2)) =>
      for {
        _ <- Future {
          logger.debug("Starting testIou")
        }
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

  def numParties: Int
  def skip: Int
  def description: String

  "Generated export for IOU transfer compiles" - {
    s"offset $skip - $description" in { testOffset(numParties, skip)(testIou) }
  }

  private def transactionFilter(ps: Ref.Party*) =
    TransactionFilter(filtersByParty = ps.map(p => p -> Filters()).toMap)
}
