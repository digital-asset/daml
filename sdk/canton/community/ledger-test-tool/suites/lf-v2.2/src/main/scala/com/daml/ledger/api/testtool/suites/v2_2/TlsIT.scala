// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.{NoParties, allocate}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{Endpoint, LedgerTestSuite}
import com.daml.ledger.api.v2.version_service.VersionServiceGrpc.VersionServiceBlockingStub
import com.daml.ledger.api.v2.version_service.{GetLedgerApiVersionRequest, VersionServiceGrpc}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.tls.TlsVersion
import com.daml.tls.TlsVersion.TlsVersion
import com.digitalasset.canton.config.TlsClientConfig
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import io.grpc.StatusRuntimeException
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

/** Verifies that a participant server correctly handles TLSv1.3 only mode, i.e.:
  *   - accepts TLSv1.3 connections,
  *   - rejects TLSv1.2 (or lower) connections.
  */
final class TLSOnePointThreeIT(
    clientTlsConfiguration: Option[TlsClientConfig]
) extends TlsIT(
      shortIdentifierPrefix = "ServerOnTLSv13ConnectionFromClientOn",
      clientTlsConfiguration,
    ) {
  testTlsConnection(
    clientTlsVersions = Seq[TlsVersion](TlsVersion.V1_2, TlsVersion.V1_3),
    assertConnectionOk = true,
  )
  testTlsConnection(clientTlsVersion = TlsVersion.V1_3, assertConnectionOk = true)
  testTlsConnection(clientTlsVersion = TlsVersion.V1_2, assertConnectionOk = false)
  testTlsConnection(clientTlsVersion = TlsVersion.V1_1, assertConnectionOk = false)
  testTlsConnection(clientTlsVersion = TlsVersion.V1, assertConnectionOk = false)
}

/** Verifies that a participant server disallows TLSv1.1 or older, i.e.:
  *   - accepts either TLSv1.2 or TLSv1.3 connections,
  *   - rejects TLSv1.1 (or lower) connections.
  */
final class TLSAtLeastOnePointTwoIT(
    clientTlsConfiguration: Option[TlsClientConfig]
) extends TlsIT(
      shortIdentifierPrefix = "ServerOnTLSConnectionFromClientOn",
      clientTlsConfiguration,
    ) {
  testTlsConnection(
    clientTlsVersions = Seq[TlsVersion](TlsVersion.V1_2, TlsVersion.V1_3),
    assertConnectionOk = true,
  )
  testTlsConnection(clientTlsVersion = TlsVersion.V1_3, assertConnectionOk = true)
  testTlsConnection(clientTlsVersion = TlsVersion.V1_2, assertConnectionOk = true)
  testTlsConnection(clientTlsVersion = TlsVersion.V1_1, assertConnectionOk = false)
  testTlsConnection(clientTlsVersion = TlsVersion.V1, assertConnectionOk = false)
}

/** Verifies that the given participant server correctly handles client connections over selected
  * TLS versions.
  *
  * It works by creating and exercising a series of client service stubs, each over different TLS
  * version.
  */
abstract class TlsIT(
    shortIdentifierPrefix: String,
    clientTlsConfiguration: Option[TlsClientConfig],
) extends LedgerTestSuite {

  def testTlsConnection(clientTlsVersion: TlsVersion, assertConnectionOk: Boolean): Unit =
    testTlsConnection(
      clientTlsVersions = Seq(clientTlsVersion),
      assertConnectionOk = assertConnectionOk,
    )

  def testTlsConnection(clientTlsVersions: Seq[TlsVersion], assertConnectionOk: Boolean): Unit = {

    val (what, assertionOnServerResponse) =
      if (assertConnectionOk)
        ("accept", assertSuccessfulConnection)
      else
        ("reject", assertFailedConnection)

    val clientTlsVersionsText = clientTlsVersions
      .map(_.version.replace(".", ""))
      .mkString("and")

    testGivenAllParticipants(
      s"$shortIdentifierPrefix$clientTlsVersionsText",
      s"A ledger API server should $what a $clientTlsVersions connection",
      allocate(NoParties),
    ) { implicit ec => (testContexts: Seq[ParticipantTestContext]) =>
      { case _ =>
        // preconditions
        assume(testContexts.nonEmpty, "Missing an expected participant test context!")
        val firstTextContext = testContexts.head
        assume(
          clientTlsConfiguration.isDefined,
          "Missing required TLS configuration!",
        )
        val tlsConfiguration = clientTlsConfiguration.get
        val Endpoint.Remote(ledgerHostname, ledgerPort) =
          firstTextContext.ledgerEndpoint
            .getOrElse(
              throw new UnsupportedOperationException("This test works only for gRPC connections")
            )
            .endpoint
            .asInstanceOf[Endpoint.Remote]

        // given
        val sslContext =
          ClientChannelBuilder.sslContext(tlsConfiguration, enabledProtocols = clientTlsVersions)
        val serviceStubOwner: ResourceOwner[VersionServiceBlockingStub] = for {
          channel <- ResourceOwner.forChannel(
            builder = NettyChannelBuilder
              .forAddress(ledgerHostname, ledgerPort)
              .useTransportSecurity()
              .sslContext(sslContext),
            shutdownTimeout = 2.seconds,
          )
        } yield VersionServiceGrpc.blockingStub(channel)

        // when
        val response: Future[String] = serviceStubOwner.use { versionService =>
          val response = versionService.getLedgerApiVersion(new GetLedgerApiVersionRequest())
          Future.successful(response.version)
        }(ResourceContext(ec))

        // then
        response.transform[Unit] {
          assertionOnServerResponse
        }
      }
    }
  }

  private lazy val assertSuccessfulConnection: Try[String] => Try[Unit] = {
    case Success(version) =>
      Try[Unit] {
        assert(
          assertion = version ne null,
          message = s"Expected a not null version!",
        )
      }
    case Failure(exception) =>
      throw new AssertionError(s"Failed to receive a successful server response!", exception)
  }

  private lazy val assertFailedConnection: Try[String] => Try[Unit] = {
    case Success(version) =>
      Try[Unit] {
        assert(
          assertion = false,
          message =
            s"Connection succeeded and returned version: $version but expected connection failure!",
        )
      }
    case Failure(_: StatusRuntimeException) => Success[Unit](())
    case Failure(other) =>
      Try[Unit] {
        assert(
          assertion = false,
          message = s"Unexpected failure: $other",
        )
      }
  }

}
