// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.tls

import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, Port}
import com.digitalasset.canton.config.{
  PemFile,
  ServerAuthRequirementConfig,
  TlsClientCertificate,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.grpc.sampleservice.HelloServiceReferenceImplementation
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.platform.apiserver.{ApiService, ApiServices, LedgerApiService}
import com.digitalasset.canton.protobuf
import com.digitalasset.canton.util.JarResourceUtils
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

  def makeARequest(): Future[protobuf.Hello.Response] =
    resources().use { channel =>
      val testRequest = protobuf.Hello.Request("foo")
      protobuf.HelloServiceGrpc
        .stub(channel)
        .hello(testRequest)
    }

  private val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024 // taken from the Sandbox config

  private final class EmptyApiServices extends ApiServices {
    override val services: Iterable[BindableService] = List(
      new HelloServiceReferenceImplementation
    )
    override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices = this

    override def close(): Unit = ()
  }

  private val serverTlsConfiguration = Option.when(tlsEnabled)(
    TlsServerConfig(
      certChainFile = PemFile(ExistingFile.tryCreate(serverCrt)),
      privateKeyFile = PemFile(ExistingFile.tryCreate(serverKey)),
      trustCollectionFile = Some(PemFile(ExistingFile.tryCreate(caCrt))),
      clientAuth = clientAuth match {
        case ClientAuth.NONE => ServerAuthRequirementConfig.None
        case ClientAuth.OPTIONAL => ServerAuthRequirementConfig.Optional
        case ClientAuth.REQUIRE =>
          ServerAuthRequirementConfig.Require(
            TlsClientCertificate(
              certChainFile = PemFile(
                ExistingFile.tryCreate(clientCrt.getOrElse(TlsFixture.resource("index.txt")))
              ), // NB: the file is not used
              privateKeyFile = PemFile(
                ExistingFile.tryCreate(clientKey.getOrElse(TlsFixture.resource("index.txt")))
              ), // NB: the file is not used
            )
          )
      },
      enableCertRevocationChecking = certRevocationChecking,
    )
  )

  private def apiServerOwner(): ResourceOwner[ApiService] = {
    val apiServices = new EmptyApiServices

    ResourceOwner
      .forExecutorService(() => Executors.newCachedThreadPool())
      .flatMap(servicesExecutor =>
        LedgerApiService(
          apiServices = apiServices,
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
        trustCollectionFile = Some(PemFile(ExistingFile.tryCreate(caCrt))),
        clientCert = (clientCrt, clientKey) match {
          case (Some(crt), Some(key)) =>
            Some(
              TlsClientCertificate(
                certChainFile = PemFile(ExistingFile.tryCreate(crt)),
                privateKeyFile = PemFile(ExistingFile.tryCreate(key)),
              )
            )
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

object TlsFixture {
  protected def resource(src: String) =
    JarResourceUtils.resourceFile("test-certificates/" + src)
}
