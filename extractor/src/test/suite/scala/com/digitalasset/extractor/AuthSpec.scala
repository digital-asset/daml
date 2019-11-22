// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import java.nio.file.Files
import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.extractor.config.{ExtractorConfig, SnapshotEndSetting}
import com.digitalasset.extractor.ledger.types.TransactionTree
import com.digitalasset.extractor.targets.TextPrintTarget
import com.digitalasset.extractor.writers.Writer
import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import com.digitalasset.jwt.domain.DecodedJwt
import com.digitalasset.jwt.{HMAC256Verifier, JwtSigner}
import com.digitalasset.ledger.api.auth.{AuthServiceJWT, AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.client.services.commands.SynchronousCommandClient
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.{SandboxFixture, TestCommands}
import com.digitalasset.timer.Delayed
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.{AsyncFlatSpec, Matchers}
import org.slf4j.LoggerFactory
import scalaz.{OneAnd, \/}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

final class AuthSpec
    extends AsyncFlatSpec
    with SandboxFixture
    with SuiteResourceManagementAroundAll
    with Matchers
    with TestCommands {

  private val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
  private val jwtSecret = "com.digitalasset.extractor.AuthSpec"

  private def newSyncClient = new SynchronousCommandClient(CommandServiceGrpc.stub(channel))

  lazy val dummyRequest = {
    // we need to adjust the time of the request because we pass 10
    // days in the test scenario.
    val letInstant = Instant.EPOCH.plus(10, ChronoUnit.DAYS)
    val let = Timestamp(letInstant.getEpochSecond, letInstant.getNano)
    val mrt = Timestamp(let.seconds + 30L, let.nanos)
    dummyCommands(ledgerId, "commandId1").update(
      _.commands.ledgerEffectiveTime := let,
      _.commands.maximumRecordTime := mrt
    )
  }

  implicit class AuthServiceJWTPayloadExtensions(payload: AuthServiceJWTPayload) {
    def expiresIn(t: java.time.Duration): AuthServiceJWTPayload =
      payload.copy(exp = Some(Instant.now.plus(t)))
    def expiresInFiveSeconds: AuthServiceJWTPayload = expiresIn(Duration.ofSeconds(5))
    def expiresTomorrow: AuthServiceJWTPayload = expiresIn(Duration.ofDays(1))
    def expired: AuthServiceJWTPayload = expiresIn(Duration.ofDays(-1))

    def signed(secret: String): String =
      JwtSigner.HMAC256
        .sign(DecodedJwt(jwtHeader, AuthServiceJWTCodec.compactPrint(payload)), secret)
        .getOrElse(sys.error("Failed to generate token"))
        .value

    def asHeader(secret: String = jwtSecret) = s"Bearer ${signed(secret)}"
  }

  override protected def config: SandboxConfig =
    super.config.copy(
      authService = Some(
        AuthServiceJWT(
          HMAC256Verifier(jwtSecret).getOrElse(sys.error("Failed to create HMAC256 verifier")))))

  private val operator = "OPERATOR"
  private val operatorPayload = AuthServiceJWTPayload(
    ledgerId = None,
    participantId = None,
    applicationId = None,
    exp = None,
    admin = true,
    actAs = List(operator),
    readAs = List(operator)
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
      ledgerPort = getSandboxPort,
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

  it should "fail immediately with a PERMISSION_DENIED if no token is provided" in {
    extractor(noAuth).run().failed.collect {
      case GrpcException(GrpcStatus.PERMISSION_DENIED(), _) => succeed
    }
  }

  it should "succeed if the proper token is provided" in {
    setToken(operatorPayload.asHeader())
    extractor(withAuth).run().map(_ => succeed)
  }

  it should "eventually succeed if an invalid token is replaced" in {
    val writtenTxs = ListBuffer.empty[String]
    val process =
      new Extractor(tailWithAuth, None)(
        (_, _, _) =>
          new Writer {
            private var lastOffset = new AtomicReference[String]
            override def init(): Future[Unit] = Future.successful(())
            override def handlePackages(packageStore: PackageStore): Future[Unit] =
              Future.successful(())
            override def handleTransaction(
                transaction: TransactionTree): Future[Writer.RefreshPackages \/ Unit] = {
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
    setToken(operatorPayload.expiresInFiveSeconds.asHeader())
    val _ = process.run()
    val expectedTxs = ListBuffer.empty[String]
    Delayed.Future
      .by(10.seconds) {
        newSyncClient
          .submitAndWaitForTransactionId(
            SubmitAndWaitRequest(commands = dummyRequest.commands),
            Option(operatorPayload.asHeader()))
          .map(_.transactionId)
      }
      .onComplete {
        case Success(tx) => val _ = expectedTxs += tx
        case Failure(NonFatal(_)) => () // do nothing, the test will fail
      }
    Delayed.by(15.seconds)(setToken(operatorPayload.asHeader()))
    Delayed.Future
      .by(20.seconds) {
        newSyncClient
          .submitAndWaitForTransactionId(
            SubmitAndWaitRequest(commands = dummyRequest.commands),
            Option(operatorPayload.asHeader()))
          .map(_.transactionId)
      }
      .onComplete {
        case Success(tx) => val _ = expectedTxs += tx
        case Failure(NonFatal(_)) => () // do nothing, the test will fail
      }
    Delayed.Future.by(25.seconds)(process.shutdown().map { _ =>
      writtenTxs should contain theSameElementsAs expectedTxs
    })
  }

}
