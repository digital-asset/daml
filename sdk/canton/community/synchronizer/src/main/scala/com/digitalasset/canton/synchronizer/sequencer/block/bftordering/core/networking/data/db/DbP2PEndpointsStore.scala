// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.db

import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, Port}
import com.digitalasset.canton.config.{
  PemFile,
  PemString,
  ProcessingTimeout,
  TlsClientCertificate,
  TlsClientConfig,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.P2PEndpointConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.ExecutionContext

final class DbP2PEndpointsStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends P2PEndpointsStore[PekkoEnv]
    with DbStore {

  import storage.api.*
  import storage.converters.*

  private implicit val getConnectionConfigRowResult: GetResult[P2PEndpoint] =
    GetResult { r =>
      val address = r.nextString()
      val port = Port.tryCreate(r.nextInt())
      val transportSecurity = r.nextBoolean()
      val customServerClientCertificates =
        r.nextBytesOption().map(ByteString.copyFrom).map(PemString(_))
      val clientCertificateChain =
        r.nextBytesOption().map(ByteString.copyFrom).map(PemString(_))
      val clientPrivateKeyFile = r.nextStringOption().map(ExistingFile.tryCreate)

      val clientCertificateInfo = (clientCertificateChain, clientPrivateKeyFile) match {
        case (Some(chain), Some(key)) =>
          Some(TlsClientCertificate(chain, PemFile(key)))
        case (None, None) =>
          None
        case _ =>
          throw new IllegalStateException(
            "Either both or none of client certificate chain and private key file must be stored"
          )
      }

      if (transportSecurity) {
        GrpcNetworking.TlsP2PEndpoint(
          P2PEndpointConfig(
            address,
            port,
            Some(
              TlsClientConfig(
                trustCollectionFile = customServerClientCertificates,
                clientCert = clientCertificateInfo,
                enabled = transportSecurity,
              )
            ),
          )
        )
      } else {
        GrpcNetworking.PlainTextP2PEndpoint(address, port)
      }
    }

  private implicit val setOptionalBlobParameter: SetParameter[Option[ByteString]] =
    (maybeByteString, positionedParameter) =>
      positionedParameter >> maybeByteString.map(_.toByteArray)

  private val profile = storage.profile

  override def listEndpoints(implicit
      traceContext: TraceContext
  ): PekkoEnv#FutureUnlessShutdownT[Seq[P2PEndpoint]] =
    queryUnlessShutdown(
      selectEndpoints,
      listEndpointsActionName,
    )

  override def addEndpoint(endpoint: P2PEndpoint)(implicit
      traceContext: TraceContext
  ): PekkoEnv#FutureUnlessShutdownT[Boolean] =
    updateUnlessShutdown(insertEndpoint(endpoint), addEndpointActionName(endpoint)).map {
      logAndCheckChangeCount
    }

  override def removeEndpoint(endpointId: P2PEndpoint.Id)(implicit
      traceContext: TraceContext
  ): PekkoEnv#FutureUnlessShutdownT[Boolean] =
    updateUnlessShutdown(
      deleteEndpoint(endpointId),
      removeEndpointActionName(endpointId),
    ).map(
      logAndCheckChangeCount
    )

  override def clearAllEndpoints()(implicit
      traceContext: TraceContext
  ): PekkoEnv#FutureUnlessShutdownT[Unit] =
    updateUnlessShutdown_(clearEndpoints, clearAllEndpointsActionName)

  private def logAndCheckChangeCount(changeCount: Int)(implicit
      traceContext: TraceContext
  ): Boolean = {
    logger.debug(s"Updated $changeCount rows")
    changeCount > 0
  }

  private def selectEndpoints: DbAction.ReadOnly[Seq[P2PEndpoint]] =
    sql"""select
            address, port, transport_security, custom_server_trust_certificates,
            client_certificate_chain, client_private_key_file
          from ord_p2p_endpoints"""
      .as[P2PEndpoint]

  private def insertEndpoint(endpoint: P2PEndpoint): DbAction.WriteOnly[Int] = {
    val address =
      String256M.tryCreate(endpoint.address) // URL host names are limited to 253 characters anyway
    val port = endpoint.port.unwrap
    val (customServerTrustCertificates, clientCertificateChain, clientPrivateKeyFile) =
      endpoint match {
        case GrpcNetworking.PlainTextP2PEndpoint(_, _) => (None, None, None)
        case GrpcNetworking.TlsP2PEndpoint(clientConfig) =>
          (
            clientConfig.tlsConfig.flatMap(_.trustCollectionFile).map(_.pemBytes),
            clientConfig.tlsConfig.flatMap(_.clientCert).map(_.certChainFile).map(_.pemBytes),
            clientConfig.tlsConfig
              .flatMap(_.clientCert)
              .map(_.privateKeyFile)
              .map(_.pemFile.unwrap.getAbsolutePath),
          )
      }
    profile match {
      case _: Postgres =>
        sqlu"""insert into ord_p2p_endpoints(
                 address, port, transport_security, custom_server_trust_certificates,
                 client_certificate_chain, client_private_key_file
               )
               values (
                 $address, $port, ${endpoint.transportSecurity}, $customServerTrustCertificates,
                 $clientCertificateChain, $clientPrivateKeyFile
               )
               on conflict (address, port, transport_security) do nothing"""
      case _: H2 =>
        sqlu"""merge into ord_p2p_endpoints
                 using dual on (
                   ord_p2p_endpoints.address = $address
                   and ord_p2p_endpoints.port = $port
                   and ord_p2p_endpoints.transport_security = ${endpoint.transportSecurity}
                 )
                 when not matched then
                   insert (
                     address, port, transport_security, custom_server_trust_certificates,
                     client_certificate_chain, client_private_key_file
                   )
                   values (
                     $address, $port, ${endpoint.transportSecurity}, $customServerTrustCertificates,
                     $clientCertificateChain, $clientPrivateKeyFile
                   )"""
    }
  }

  private def deleteEndpoint(endpointId: P2PEndpoint.Id): DbAction.WriteOnly[Int] = {
    val address256M = String256M.tryCreate(endpointId.address)
    sqlu"""delete from ord_p2p_endpoints
           where address = $address256M and port = ${endpointId.port.unwrap} and transport_security = ${endpointId.transportSecurity}"""
  }

  private def clearEndpoints: DbAction.WriteOnly[Int] = sqlu"truncate table ord_p2p_endpoints"

  private def queryUnlessShutdown[X](
      action: DBIOAction[X, NoStream, Effect.Read],
      actionName: String,
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[X] =
    PekkoFutureUnlessShutdown(actionName, () => storage.query(action, actionName))

  private def updateUnlessShutdown[X](
      action: DBIOAction[X, NoStream, Effect.Write & Effect.Transactional],
      actionName: String,
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[X] =
    PekkoFutureUnlessShutdown(actionName, () => storage.update(action, actionName))

  private def updateUnlessShutdown_(
      action: DBIOAction[?, NoStream, Effect.Write & Effect.Transactional],
      actionName: String,
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    updateUnlessShutdown(action, actionName).map(_ => ())
}
