// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

import com.daml.extractor.config.{ExtractorConfig, SnapshotEndSetting}
import com.daml.extractor.ledger.types.TransactionTree
import com.daml.extractor.targets.TextPrintTarget
import com.daml.extractor.writers.Writer
import com.daml.grpc.GrpcException
import com.daml.ledger.api.auth.AuthServiceJWTPayload
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.services.commands.SynchronousCommandClient
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.lf.data.Ref.Party
import com.daml.platform.sandbox.SandboxRequiringAuthorization
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.timer.Delayed
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import scalaz.{OneAnd, \/}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

final class AuthSpec
    extends AsyncFlatSpec
    with SandboxNextFixture
    with SandboxRequiringAuthorization
    with SuiteResourceManagementAroundAll
    with Matchers
    with TestCommands {

  private def newSyncClient = new SynchronousCommandClient(CommandServiceGrpc.stub(channel))

  private lazy val dummyRequest = dummyCommands(wrappedLedgerId, "commandId1")

  private val operator = "OPERATOR"
  private val operatorPayload = AuthServiceJWTPayload(
    ledgerId = None,
    participantId = None,
    applicationId = None,
    exp = None,
    admin = true,
    actAs = List(operator),
    readAs = List(operator),
  )

  private val accessTokenFile = Files.createTempFile("Extractor", "AuthSpec")

  private def setToken(string: String): Unit = {
    val _ = Files.write(accessTokenFile, string.getBytes())
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      Files.delete(accessTokenFile)
    } catch {
      case NonFatal(e) =>
        LoggerFactory.getLogger(classOf[AuthSpec]).warn("Unable to delete temporary token file", e)
    }
  }

  private def extractor(config: ExtractorConfig) =
    new Extractor(config, TextPrintTarget)()

  private def noAuth =
    ExtractorConfig(
      "127.0.0.1",
      ledgerPort = serverPort,
      ledgerInboundMessageSizeMax = 50 * 1024 * 1024,
      LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
      SnapshotEndSetting.Head,
      OneAnd(Party.assertFromString(operator), List.empty),
      Set.empty,
      TlsConfiguration(
        enabled = false,
        None,
        None,
        None,
      ),
      None,
    )

  private def withAuth = noAuth.copy(accessTokenFile = Option(accessTokenFile))

  private def tailWithAuth = withAuth.copy(to = SnapshotEndSetting.Follow)

  behavior of "Extractor against a Ledger API protected by authentication"

  it should "fail immediately with a UNAUTHENTICATED if no token is provided" in {
    extractor(noAuth).run().failed.collect { case GrpcException.UNAUTHENTICATED() =>
      succeed
    }
  }

  it should "succeed if the proper token is provided" in {
    setToken(toHeader(operatorPayload))
    extractor(withAuth).run().map(_ => succeed)
  }

  it should "eventually succeed if an invalid token is replaced" in {
    val writtenTxs = ListBuffer.empty[String]
    val process =
      new Extractor(tailWithAuth, None)((_, _, _) =>
        new Writer {
          private val lastOffset = new AtomicReference[String]

          override def init(): Future[Unit] = Future.unit

          override def handlePackages(packageStore: PackageStore): Future[Unit] =
            Future.unit

          override def handleTransaction(
              transaction: TransactionTree
          ): Future[Writer.RefreshPackages \/ Unit] = {
            Future.successful {
              \/.right {
                lastOffset.set {
                  writtenTxs += transaction.transactionId
                  transaction.offset
                }
              }
            }
          }
          override def getLastOffset: Future[Option[String]] =
            Future.successful(Option(lastOffset.get()))
        }
      )
    setToken(toHeader(expiringIn(Duration.ofSeconds(5), operatorPayload)))
    val _ = process.run()
    val expectedTxs = ListBuffer.empty[String]
    Delayed.Future
      .by(10.seconds) {
        newSyncClient
          .submitAndWaitForTransactionId(
            SubmitAndWaitRequest(commands = dummyRequest.commands),
            Option(toHeader(operatorPayload)),
          )
          .map(_.transactionId)
      }
      .onComplete {
        case Success(tx) => val _ = expectedTxs += tx
        case Failure(_) => () // do nothing, the test will fail
      }
    Delayed.by(15.seconds)(setToken(toHeader(operatorPayload)))
    Delayed.Future
      .by(20.seconds) {
        newSyncClient
          .submitAndWaitForTransactionId(
            SubmitAndWaitRequest(commands = dummyRequest.commands),
            Option(toHeader(operatorPayload)),
          )
          .map(_.transactionId)
      }
      .onComplete {
        case Success(tx) => val _ = expectedTxs += tx
        case Failure(_) => () // do nothing, the test will fail
      }
    Delayed.Future.by(25.seconds)(process.shutdown().map { _ =>
      writtenTxs should contain theSameElementsAs expectedTxs
    })
  }

}
