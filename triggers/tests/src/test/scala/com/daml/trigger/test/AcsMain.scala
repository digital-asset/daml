// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.trigger.test

import java.io.File
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Sink
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
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
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.daml.lf.speedy.SExpr
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SValue._

import com.daml.trigger.Runner

case class Config(ledgerPort: Int, darPath: File)

// This is a very rough test suite to make sure that we do not regress
// on what we already have for DAML triggers. This will likely change
// significantly in the future.

// We do not use scalatest here since that doesn’t work nicely with
// the client_server_test macro.

object AcsMain {

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
        val stdlibPackageId: PackageId = dar.all
          .find {
            case (pkgId, pkg) =>
              pkg.modules.contains(DottedName.assertFromString("DA.Internal.Template"))
          }
          .get
          ._1

        val triggerId: Identifier =
          Identifier(dar.main._1, QualifiedName.assertFromString("ACS:test"))
        val runner = Runner.fromDar(dar, triggerId)

        val system: ActorSystem = ActorSystem("TriggerRunner")
        implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
        val sequencer = new AkkaExecutionSequencerPool("TriggerRunnerPool")(system)
        implicit val ec: ExecutionContext = system.dispatcher

        val clientConfig = LedgerClientConfiguration(
          applicationId = applicationId.unwrap,
          ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
          commandClient = CommandClientConfiguration.default,
          sslContext = None
        )

        val assetId = value.Identifier(
          packageId = dar.main._1,
          moduleName = "ACS",
          entityName = "Asset"
        )

        // Create a contract and return the contract id.
        def create(client: LedgerClient, party: String, commandId: String): Future[String] = {
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
                  applicationId = applicationId.unwrap,
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
        def archive(
            client: LedgerClient,
            party: String,
            commandId: String,
            contractId: String): Future[Unit] = {
          val archiveVal = Some(
            value
              .Value()
              .withRecord(
                value.Record(
                  recordId = Some(
                    value.Identifier(
                      packageId = stdlibPackageId,
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
                  entityName = "Asset",
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
                  applicationId = applicationId.unwrap,
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

        var partyCount = 0
        def getNewParty(): String = {
          partyCount = partyCount + 1
          s"Alice$partyCount"
        }

        def test(transactions: Long, commands: (LedgerClient, String) => Future[Set[String]]) = {
          val party = getNewParty()
          val clientF =
            LedgerClient.singleHost("localhost", config.ledgerPort, clientConfig)(ec, sequencer)
          val filter = TransactionFilter(List((party, Filters.defaultInstance)).toMap)
          val triggerFlow: Future[SExpr] = for {
            client <- clientF
            acsResponses <- client.activeContractSetClient
              .getActiveContracts(filter, verbose = true)
              .runWith(Sink.seq)

            offset <- Future {
              Array(acsResponses: _*).lastOption
                .fold(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))(resp =>
                  LedgerOffset().withAbsolute(resp.offset))
            }
            finalState <- client.transactionClient
              .getTransactions(offset, None, filter)
              .take(transactions)
              .runWith(runner.getTriggerSink(acsResponses.flatMap(x => x.activeContracts)))
          } yield finalState
          val commandsFlow: Future[Set[String]] = for {
            client <- clientF
            activeContracts <- commands(client, party)
          } yield activeContracts

          // We want to error out if either of the futures fails so Future.sequence
          // does not do the trick and we have to hack around it using a Promise
          val p = Promise[(SExpr, Set[String])]()
          triggerFlow.onComplete(r =>
            r match {
              case Success(_) => ()
              case Failure(e) => p.tryFailure(e); ()
          })
          commandsFlow.onComplete(r =>
            r match {
              case Success(_) => ()
              case Failure(e) => p.tryFailure(e); ()
          })
          triggerFlow
            .zip(commandsFlow)
            .onComplete(r =>
              r match {
                case Success(v) => {
                  p.success(v); ()
                }
                case Failure(_) => ()
            })
          p.future.onComplete(r =>
            r match {
              case Success(_) => ()
              case Failure(_) => {
                Await.result(system.terminate(), Duration.Inf)
                sys.exit(1)
              }
          })
          val r = Await.result(p.future, Duration.Inf)

          r._1 match {
            case SEValue(SMap(v)) =>
              assert(
                v.keySet == r._2,
                "Expected " + r._2.toString + " but got " + v.keySet.toString)
            case _ => assert(false, "Expected a map but got " + r._1.toString)
          }
        }

        try {

          test(1, (client, party) => {
            for {
              contractId <- create(client, party, "1.0")
            } yield Set(contractId)
          })

          test(2, (client, party) => {
            for {
              contractId1 <- create(client, party, "2.0")
              contractId2 <- create(client, party, "2.1")
            } yield Set(contractId1, contractId2)
          })

          test(
            4,
            (client, party) => {
              for {
                contractId1 <- create(client, party, "3.0")
                contractId2 <- create(client, party, "3.1")
                _ <- archive(client, party, "3.2", contractId1)
                _ <- archive(client, party, "3.3", contractId2)
              } yield Set()
            }
          )

        } finally {
          val _ = Await.result(system.terminate(), Duration.Inf)
        }
    }
  }
}
