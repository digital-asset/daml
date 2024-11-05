// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.tls

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, Port}
import com.digitalasset.canton.config.{
  ServerAuthRequirementConfig,
  TlsClientCertificate,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.grpc.sampleservice.HelloServiceReferenceImplementation
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.platform.apiserver.{ApiService, ApiServices, LedgerApiService}
import io.grpc.{BindableService, ManagedChannel}
import io.netty.handler.ssl.ClientAuth

import java.io.File
import java.util.concurrent.Executors
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

final case class TlsFixture(
    loggerFactory: NamedLoggerFactory,
    tlsEnabled: Boolean,
    serverCrt: File,
    serverKey: File,
    caCrt: File,
    clientCrt: Option[File],
    clientKey: Option[File],
    clientAuth: ClientAuth = ClientAuth.REQUIRE,
    certRevocationChecking: Boolean = false,
)(implicit rc: ResourceContext, ec: ExecutionContext) {

  def makeARequest(): Future[v0.Hello.Response] =
    resources().use { channel =>
      val testRequest = v0.Hello.Request("foo")
      v0.HelloServiceGrpc
        .stub(channel)
        .hello(testRequest)
    }

  private val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024 // taken from the Sandbox config

  private final class MockApiServices(apiServices: ApiServices) extends ResourceOwner[ApiServices] {
    override def acquire()(implicit context: ResourceContext): Resource[ApiServices] =
      Resource(Future.successful(apiServices))(_ => Future.successful(()))(context)
  }

  private final class EmptyApiServices extends ApiServices {
    override val services: Iterable[BindableService] = List(
      new HelloServiceReferenceImplementation
    )
    override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices = this
  }

  private val serverTlsConfiguration = Option.when(tlsEnabled)(
    TlsServerConfig(
      certChainFile = ExistingFile.tryCreate(serverCrt),
      privateKeyFile = ExistingFile.tryCreate(serverKey),
      trustCollectionFile = Some(ExistingFile.tryCreate(caCrt)),
      clientAuth = clientAuth match {
        case ClientAuth.NONE => ServerAuthRequirementConfig.None
        case ClientAuth.OPTIONAL => ServerAuthRequirementConfig.Optional
        case ClientAuth.REQUIRE =>
          ServerAuthRequirementConfig.Require(
            TlsClientCertificate(
              certChainFile = clientCrt.getOrElse(new File("unused.txt")),
              privateKeyFile = clientKey.getOrElse(new File("unused.txt")),
            )
          )
      },
      enableCertRevocationChecking = certRevocationChecking,
    )
  )

  private def apiServerOwner(): ResourceOwner[ApiService] = {
    val apiServices = new EmptyApiServices
    val owner = new MockApiServices(apiServices)

    ResourceOwner
      .forExecutorService(() => Executors.newCachedThreadPool())
      .flatMap(servicesExecutor =>
        new LedgerApiService(
          apiServicesOwner = owner,
          desiredPort = Port.Dynamic,
          maxInboundMessageSize = DefaultMaxInboundMessageSize,
          address = None,
          tlsConfiguration = serverTlsConfiguration,
          servicesExecutor = servicesExecutor,
          metrics = LedgerApiServerMetrics.ForTesting,
          keepAlive = None,
          loggerFactory = loggerFactory,
        )
      )
  }

  private val clientTlsConfiguration =
    Option.when(tlsEnabled)(
      TlsClientConfig(
        trustCollectionFile = Some(ExistingFile.tryCreate(caCrt)),
        clientCert = (clientCrt, clientKey) match {
          case (Some(crt), Some(key)) =>
            Some(TlsClientCertificate(certChainFile = crt, privateKeyFile = key))
          case _ => None
        },
      )
    )

  private val ledgerClientChannelConfiguration = LedgerClientChannelConfiguration(
    sslContext = clientTlsConfiguration.map(ClientChannelBuilder.sslContext(_))
  )

  private def resources(): ResourceOwner[ManagedChannel] =
    for {
      apiServer <- apiServerOwner()
      channel <- new GrpcChannel.Owner(apiServer.port.unwrap, ledgerClientChannelConfiguration)
    } yield channel

}
