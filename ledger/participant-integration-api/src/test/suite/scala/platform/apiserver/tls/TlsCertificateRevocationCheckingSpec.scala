// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.tls

import java.io.File

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.sampleservice.implementations.ReferenceImplementation
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.platform.apiserver.{ApiServer, ApiServices, LedgerApiServer}
import io.grpc.{BindableService, ManagedChannel}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.ledger.client.GrpcChannel
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.metrics.Metrics
import com.daml.ports.Port
import org.mockito.MockitoSugar
import com.daml.logging.LoggingContext
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}

import scala.collection.immutable
import scala.concurrent.Future

final class TlsCertificateRevocationCheckingSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with AkkaBeforeAndAfterAll
    with TestResourceContext
    with OcspResponderFixture {
  import TlsCertificateRevocationCheckingSpec.{TlsFixture, resource}

  val serverCrt = resource("server.crt")
  val serverKey = resource("server.pem")
  val caCrt = resource("ca.crt")
  val clientCrt = resource("client.crt")
  val clientKey = resource("client.pem")
  val clientRevokedCrt = resource("client-revoked.crt")
  val clientRevokedKey = resource("client-revoked.pem")
  val ocspCrt = resource("ocsp.crt")
  val ocspKey = resource("ocsp.key.pem")
  val index = resource("index.txt")

  override protected def indexPath: String = index.getAbsolutePath
  override protected def caCertPath: String = caCrt.getAbsolutePath
  override protected def ocspKeyPath: String = ocspKey.getAbsolutePath
  override protected def ocspCertPath: String = ocspCrt.getAbsolutePath
  override protected def ocspTestCertificate: String = clientCrt.getAbsolutePath

  classOf[LedgerApiServer].getSimpleName when {
    "certificate revocation checking is enabled" should {
      "allow TLS connections with valid certificates" in {
        TlsFixture(tlsEnabled = true, serverCrt, serverKey, caCrt, clientCrt, clientKey)
          .makeARequest()
          .map(_ => succeed)
      }

      "block TLS connections with revoked certificates" in {
        TlsFixture(
          tlsEnabled = true,
          serverCrt,
          serverKey,
          caCrt,
          clientRevokedCrt,
          clientRevokedKey)
          .makeARequest()
          .failed
          .collect {
            case com.daml.grpc.GrpcException.UNAVAILABLE() =>
              succeed
            case ex =>
              fail(s"Invalid exception: ${ex.getClass.getCanonicalName}: ${ex.getMessage}")
          }
      }
    }

    "certificate revocation checking is not enabled" should {
      "allow TLS connections with valid certificates" in {
        TlsFixture(tlsEnabled = false, serverCrt, serverKey, caCrt, clientCrt, clientKey)
          .makeARequest()
          .map(_ => succeed)
      }

      "allow TLS connections with revoked certificates" in {
        TlsFixture(
          tlsEnabled = false,
          serverCrt,
          serverKey,
          caCrt,
          clientRevokedCrt,
          clientRevokedKey)
          .makeARequest()
          .map(_ => succeed)
      }
    }
  }
}

object TlsCertificateRevocationCheckingSpec {

  protected final case class TlsFixture(
      tlsEnabled: Boolean,
      serverCrt: File,
      serverKey: File,
      caCrt: File,
      clientCrt: File,
      clientKey: File,
  )(implicit rc: ResourceContext, actorSystem: ActorSystem) {

    def makeARequest(): Future[HelloResponse] =
      resources().use { channel =>
        val testRequest = HelloRequest(1)
        HelloServiceGrpc
          .stub(channel)
          .single(testRequest)
      }

    private val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024 // taken from the Sandbox config

    private final class MockApiServices(apiServices: ApiServices)
        extends ResourceOwner[ApiServices] {
      override def acquire()(implicit context: ResourceContext): Resource[ApiServices] = {
        Resource(Future.successful(apiServices))(_ => Future.successful(()))(context)
      }
    }

    private final class EmptyApiServices extends ApiServices {
      override val services: Iterable[BindableService] = List(new ReferenceImplementation)
      override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices = this
    }

    private val serverTlsConfiguration = TlsConfiguration(
      enabled = tlsEnabled,
      keyCertChainFile = Some(serverCrt),
      keyFile = Some(serverKey),
      trustCertCollectionFile = Some(caCrt),
      enableCertRevocationChecking = true
    )

    private def apiServerOwner(): ResourceOwner[ApiServer] = {
      val apiServices = new EmptyApiServices
      val owner = new MockApiServices(apiServices)

      LoggingContext.newLoggingContext { implicit loggingContext =>
        new LedgerApiServer(
          apiServicesOwner = owner,
          desiredPort = Port.Dynamic,
          maxInboundMessageSize = DefaultMaxInboundMessageSize,
          address = None,
          tlsConfiguration = Some(serverTlsConfiguration),
          metrics = new Metrics(new MetricRegistry),
        )
      }
    }

    private val clientTlsConfiguration =
      TlsConfiguration(
        enabled = tlsEnabled,
        keyCertChainFile = Some(clientCrt),
        keyFile = Some(clientKey),
        trustCertCollectionFile = Some(caCrt)
      )

    private val ledgerClientConfiguration = LedgerClientConfiguration(
      applicationId = s"TlsCertificates-app",
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = clientTlsConfiguration.client,
      token = None
    )

    private def resources(): ResourceOwner[ManagedChannel] =
      for {
        apiServer <- apiServerOwner()
        channel <- new GrpcChannel.Owner(apiServer.port, ledgerClientConfiguration)
      } yield channel

  }

  protected def resource(src: String) =
    new File(rlocation("ledger/test-common/test-certificates/" + src))
}
