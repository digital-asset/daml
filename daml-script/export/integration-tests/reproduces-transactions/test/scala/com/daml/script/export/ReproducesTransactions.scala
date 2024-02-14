// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import org.apache.pekko.stream.scaladsl.Sink
import com.daml.SdkVersion
import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonConfig.TimeProviderType
import com.daml.integrationtest.CantonFixture
import com.digitalasset.canton.ledger.api.refinements.ApiTypes
import com.digitalasset.canton.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v2.update_service.GetUpdateTreesResponse
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.transaction_filter.Filters
import com.daml.ledger.api.v1.{value => api}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.daml.ledger.testing.utils.TransactionEq
import com.daml.lf.archive.{Dar, DarDecoder}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.script.ledgerinteraction.GrpcLedgerClient
import com.daml.lf.engine.script.{Participants, Runner, ScriptTimeMode}
import com.daml.lf.language.Ast.Package
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

import scala.concurrent.Future
import scala.sys.process._

final class ReproducesTransactions
    extends AsyncFreeSpec
    with Matchers
    with BeforeAndAfterEach
    with CantonFixture {

  final override protected lazy val darFiles = List(darFile)
  final override protected lazy val timeProviderType = TimeProviderType.Static

  private val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
  val scriptPath = BazelRunfiles.rlocation("daml-script/runner/daml-script-binary" + exe)
  val damlScriptLib = BazelRunfiles.requiredResource("daml-script/daml3/daml3-script.dar")
  val darPath = BazelRunfiles.rlocation("daml-script/test/script-test.dar")

  val isWindows: Boolean = sys.props("os.name").toLowerCase.contains("windows")
  private val damlc =
    BazelRunfiles.requiredResource(s"compiler/damlc/damlc$exe")
  private val darFile = BazelRunfiles.rlocation(Paths.get(com.daml.ledger.test.ModelTestDar.path))
  private val mainPkg = DarDecoder.assertReadArchiveFromFile(darFile.toFile).main._1
  private def iouId(s: String) =
    api.Identifier(mainPkg, moduleName = "Iou", s)

  private def submit(client: LedgerClient, p: Ref.Party, cmd: Command) =
    client.v2.commandService.deprecatedSubmitAndWaitForTransactionForJsonApi(
      SubmitAndWaitRequest(
        Some(
          Commands(
            applicationId = applicationId.getOrElse(""),
            commandId = UUID.randomUUID().toString(),
            actAs = Seq(p),
            commands = Seq(cmd),
          )
        )
      )
    )

  private def collectTrees(client: LedgerClient, parties: List[Ref.Party]) =
    client.v2.updateService
      .getUpdateTreesSource(
        begin =
          ParticipantOffset().withBoundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_BEGIN),
        end = Some(
          ParticipantOffset().withBoundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_END)
        ),
        filter = transactionFilter(parties: _*),
      )
      .runWith(Sink.seq)
      .map(
        _.map(_.update)
          .collect { case GetUpdateTreesResponse.Update.TransactionTree(tree) =>
            tree
          }
      )

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

  private val participantBegin =
    ParticipantOffset().withBoundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_BEGIN)
  private val participantEnd =
    ParticipantOffset().withBoundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_END)
  private def participantOffset(offset: String) =
    ParticipantOffset.of(ParticipantOffset.Value.Absolute(offset))

  private def createScriptExport(
      parties: List[Ref.Party],
      offset: ParticipantOffset,
      dir: Path,
  ): Future[Dar[(PackageId, Package)]] = for {
    // build script export
    _ <- Main.run(
      Config(
        ledgerHost = "localhost",
        ledgerPort = ports.head.value,
        tlsConfig = TlsConfiguration(false, None, None, None),
        accessToken = None,
        partyConfig = PartyConfig(
          parties = ApiTypes.Party.subst(parties),
          allParties = false,
        ),
        start = offset,
        end = participantEnd,
        maxInboundMessageSize = Config.DefaultMaxInboundMessageSize,
        exportType = Some(
          ExportScript(
            outputPath = dir,
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
      dir.toString,
      "-o",
      dir.resolve("export.dar").toString,
    ).! shouldBe 0
    // load DAR
    dar <- Future.fromTry(DarDecoder.readArchiveFromFile(dir.resolve("export.dar").toFile).toTry)
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
        default_participant = Some(new GrpcLedgerClient(client, applicationId)),
        participants = Map.empty,
        party_participants = Map.empty,
      ),
    )
  } yield ()

  private def testOffset(
      numParties: Int,
      skip: Int,
  )(f: (LedgerClient, Seq[Ref.Party]) => Future[Unit]): Future[Assertion] = {
    val dir = Files.createTempDirectory("script_export")
    val future = for {
      client <- defaultLedgerClient()
      parties <- allocateParties(client, numParties)
      // setup
      _ <- f(client, parties)
      before <- collectTrees(client, parties)
      // Reproduce ACS up to offset and transaction trees after offset.
      offset =
        if (skip == 0) { participantBegin }
        else { participantOffset(before(skip - 1).offset) }
      beforeCmp = before.drop(skip)
      // reproduce from export
      dar <- createScriptExport(parties, offset, dir)
      newParties <- allocateParties(client, numParties)
      partiesMap = parties.zip(newParties).toMap
      _ <- runScriptExport(client, partiesMap, dar)
      // check that the new transaction trees are the same
      after <- collectTrees(client, newParties)
      afterCmp = after.drop(after.length - beforeCmp.length)
      _ = com.daml.fs.Utils.deleteRecursively(dir)
    } yield TransactionEq.equivalent(beforeCmp, afterCmp).fold(fail(_), _ => succeed)
    future.onComplete(_ => com.daml.fs.Utils.deleteRecursively(dir))
    future
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

  "Generated export for IOU transfer compiles" - {

    s"offset 0 - empty ACS" in { testOffset(2, 0)(testIou) }

    s"offset 2 - skip split" in { testOffset(2, 2)(testIou) }

    s"offset 4 - no trees" in { testOffset(2, 4)(testIou) }

  }

  private def transactionFilter(ps: Ref.Party*) =
    TransactionFilter(filtersByParty = ps.map(p => p -> Filters()).toMap)
}
