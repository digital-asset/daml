// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation.{NoParties, allocate}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{Endpoint, LedgerTestSuite}
import com.daml.ledger.api.tls.{TlsConfiguration, TlsVersion}
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityServiceBlockingStub
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import io.grpc.StatusRuntimeException
import io.grpc.netty.NettyChannelBuilder

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Verifies that a participant server correctly handles TLSv1.3 only mode, i.e.:
  * - accepts TLSv1.3 connections,
  * - rejects TLSv1.2 (or lower) connections.
  */
final class TLSOnePointThreeIT(
    clientTlsConfiguration: Option[TlsConfiguration]
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
  * - accepts either TLSv1.2 or TLSv1.3 connections,
  * - rejects TLSv1.1 (or lower) connections.
  */
final class TLSAtLeastOnePointTwoIT(
    clientTlsConfiguration: Option[TlsConfiguration]
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

/** Verifies that the given participant server correctly handles client connections over selected TLS versions.
  *
  * It works by creating and exercising a series of client service stubs, each over different TLS version.
  */
abstract class TlsIT(
    shortIdentifierPrefix: String,
    clientTlsConfiguration: Option[TlsConfiguration],
) extends LedgerTestSuite {

  def testTlsConnection(clientTlsVersion: TlsVersion, assertConnectionOk: Boolean): Unit = {
    testTlsConnection(
      clientTlsVersions = Seq(clientTlsVersion),
      assertConnectionOk = assertConnectionOk,
    )
  }

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
        assume(
          tlsConfiguration.enabled,
          "TLS configuration is disabled but expected to be enabled!",
        )
        assume(
          firstTextContext.ledgerEndpoint.isInstanceOf[Endpoint.Remote],
          "Expected a remote (i.e. with a hostname and port) ledger endpoint!",
        )
        val Endpoint.Remote(ledgerHostname, ledgerPort) =
          firstTextContext.ledgerEndpoint.asInstanceOf[Endpoint.Remote]

        // given
        val sslContext = tlsConfiguration
          .client(enabledProtocols = clientTlsVersions)
          .getOrElse(throw new IllegalStateException("Missing SslContext!"))
        val serviceStubOwner: ResourceOwner[LedgerIdentityServiceBlockingStub] = for {
          channel <- ResourceOwner.forChannel(
            builder = NettyChannelBuilder
              .forAddress(ledgerHostname, ledgerPort)
              .useTransportSecurity()
              .sslContext(sslContext),
            shutdownTimeout = 2.seconds,
          )
        } yield LedgerIdentityServiceGrpc.blockingStub(channel): @nowarn(
          "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
        )

        // when
        val response: Future[String] = serviceStubOwner.use { identityService =>
          val response = identityService.getLedgerIdentity(new GetLedgerIdentityRequest())
          Future.successful(response.ledgerId)
        }(ResourceContext(ec)): @nowarn(
          "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
        )

        // then
        response.transform[Unit] {
          assertionOnServerResponse
        }
      }
    }
  }

  private lazy val assertSuccessfulConnection: Try[String] => Try[Unit] = {
    case Success(ledgerId) =>
      Try[Unit] {
        assert(
          assertion = ledgerId ne null,
          message = s"Expected a not null ledger id!",
        )
      }
    case Failure(exception) =>
      throw new AssertionError(s"Failed to receive a successful server response!", exception)
  }

  private lazy val assertFailedConnection: Try[String] => Try[Unit] = {
    case Success(ledgerId) =>
      Try[Unit] {
        assert(
          assertion = false,
          message =
            s"Connection succeeded and returned ledgerId: $ledgerId but expected connection failure!",
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
