// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.domain.v30 as domainV30
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.RegisterDomainRequest.DomainConnection
import com.digitalasset.canton.admin.participant.v30.{
  DisconnectAllDomainsRequest,
  DisconnectAllDomainsResponse,
}
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.domain.{
  DomainAliasManager,
  DomainConnectionConfig,
  DomainRegistryError,
  DomainRegistryHelpers,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.sync.CantonSyncService.ConnectDomain
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInternalError.DomainIsMissingInternally
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceUnknownDomain
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{DomainAlias, ProtoDeserializationError}
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcDomainConnectivityService(
    sync: CantonSyncService,
    aliasManager: DomainAliasManager,
    timeouts: ProcessingTimeout,
    sequencerInfoLoader: SequencerInfoLoader,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends v30.DomainConnectivityServiceGrpc.DomainConnectivityService
    with NamedLogging {

  private def waitUntilActiveIfSuccess(success: Boolean, domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, BaseCantonError, Unit] =
    if (success) waitUntilActive(domain) else EitherTUtil.unitUS[BaseCantonError]

  private def waitUntilActive(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, BaseCantonError, Unit] =
    for {
      domainId <- EitherT.fromOption[FutureUnlessShutdown](
        aliasManager
          .domainIdForAlias(domain),
        DomainIsMissingInternally(domain, "aliasManager"),
      )
      client <- EitherT.fromOption[FutureUnlessShutdown](
        sync.syncCrypto.ips
          .forDomain(domainId),
        DomainIsMissingInternally(domain, "ips"),
      )
      active <- EitherT
        .right(client.await(_.isParticipantActive(sync.participantId), timeouts.network.unwrap))
      _ <-
        EitherT
          .cond[FutureUnlessShutdown](
            active,
            (),
            DomainRegistryError.ConnectionErrors.ParticipantIsNotActive.Error(
              s"While domain $domain promised, participant ${sync.participantId} never became active within `timeouts.network` (${timeouts.network})."
            ): BaseCantonError,
          )
    } yield ()

  private def parseDomainAlias(
      domainAliasProto: String
  ): EitherT[FutureUnlessShutdown, BaseCantonError, DomainAlias] =
    EitherT.fromEither[FutureUnlessShutdown](
      DomainAlias
        .create(domainAliasProto)
        .leftMap(ProtoDeserializationFailure.WrapNoLoggingStr.apply)
    )

  private def parseDomainConnectionConfig(
      proto: Option[v30.DomainConnectionConfig],
      name: String,
  ): Either[BaseCantonError, DomainConnectionConfig] =
    ProtoConverter
      .parseRequired(DomainConnectionConfig.fromProtoV30, name, proto)
      .leftMap(ProtoDeserializationFailure.WrapNoLogging.apply)

  private def parseSequencerConnectionValidation(
      proto: domainV30.SequencerConnectionValidation
  ): Either[BaseCantonError, SequencerConnectionValidation] =
    SequencerConnectionValidation
      .fromProtoV30(proto)
      .leftMap(ProtoDeserializationFailure.WrapNoLogging.apply)

  /** @param domainConnectionP Protobuf data
    * @return True if handshake should be done, false otherwise
    */
  private def parseDomainConnection(
      domainConnectionP: v30.RegisterDomainRequest.DomainConnection
  ): Either[ProtoDeserializationFailure.WrapNoLogging, Boolean] = (domainConnectionP match {
    case DomainConnection.DOMAIN_CONNECTION_MISSING =>
      ProtoDeserializationError.FieldNotSet("domain_connection").asLeft
    case DomainConnection.DOMAIN_CONNECTION_NONE => Right(false)
    case DomainConnection.DOMAIN_CONNECTION_HANDSHAKE => Right(true)
    case DomainConnection.Unrecognized(unrecognizedValue) =>
      ProtoDeserializationError.UnrecognizedEnum("domain_connection", unrecognizedValue).asLeft
  }).leftMap(ProtoDeserializationFailure.WrapNoLogging.apply)

  override def reconnectDomain(
      request: v30.ReconnectDomainRequest
  ): Future[v30.ReconnectDomainResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ReconnectDomainRequest(domainAlias, keepRetrying) = request
    val ret = for {
      alias <- parseDomainAlias(domainAlias)
      success <- sync.connectDomain(alias, keepRetrying, ConnectDomain.Connect)
      _ <- waitUntilActiveIfSuccess(success, alias)
    } yield v30.ReconnectDomainResponse(connectedSuccessfully = success)
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def disconnectDomain(
      request: v30.DisconnectDomainRequest
  ): Future[v30.DisconnectDomainResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.DisconnectDomainRequest(domainAlias) = request
    val ret = for {
      alias <- parseDomainAlias(domainAlias)
      _ <- sync.disconnectDomain(alias).leftWiden[BaseCantonError]
    } yield v30.DisconnectDomainResponse()
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def disconnectAllDomains(
      request: DisconnectAllDomainsRequest
  ): Future[DisconnectAllDomainsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    CantonGrpcUtil.mapErrNewEUS(sync.disconnectDomains()).map(_ => DisconnectAllDomainsResponse())
  }

  override def logout(request: v30.LogoutRequest): Future[v30.LogoutResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.LogoutRequest(domainAliasP) = request

    val ret = for {
      domainAlias <- EitherT
        .fromEither[Future](DomainAlias.create(domainAliasP))
        .leftMap(err =>
          Status.INVALID_ARGUMENT
            .withDescription(s"Failed to parse domain alias: $err")
            .asRuntimeException()
        )
      _ <- sync
        .logout(domainAlias)
        .leftMap(err => err.asRuntimeException())
        .onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError))
    } yield v30.LogoutResponse()

    EitherTUtil.toFuture(ret)
  }

  override def listConnectedDomains(
      request: v30.ListConnectedDomainsRequest
  ): Future[v30.ListConnectedDomainsResponse] =
    Future.successful(v30.ListConnectedDomainsResponse(sync.readyDomains.map {
      case (alias, (domainId, healthy)) =>
        new v30.ListConnectedDomainsResponse.Result(
          domainAlias = alias.unwrap,
          domainId = domainId.toProtoPrimitive,
          healthy = healthy.unwrap,
        )
    }.toSeq))

  override def listRegisteredDomains(
      request: v30.ListRegisteredDomainsRequest
  ): Future[v30.ListRegisteredDomainsResponse] = {
    val connected = sync.readyDomains
    val registeredDomains = sync.registeredDomains

    Future.successful(
      v30.ListRegisteredDomainsResponse(
        results = registeredDomains
          .filter(_.status.isActive)
          .map(_.config)
          .map(cnf =>
            new v30.ListRegisteredDomainsResponse.Result(
              config = Some(cnf.toProtoV30),
              connected = connected.contains(cnf.domain),
            )
          )
      )
    )
  }

  override def connectDomain(
      request: v30.ConnectDomainRequest
  ): Future[v30.ConnectDomainResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ConnectDomainRequest(configPO, sequencerConnectionValidationPO) =
      request

    val ret: EitherT[FutureUnlessShutdown, BaseCantonError, v30.ConnectDomainResponse] = for {
      config <- EitherT.fromEither[FutureUnlessShutdown](
        parseDomainConnectionConfig(configPO, "config")
      )
      validation <- EitherT.fromEither[FutureUnlessShutdown](
        parseSequencerConnectionValidation(sequencerConnectionValidationPO)
      )
      _ = logger.info(show"Registering new domain $config")
      _ <- sync.addDomain(config, validation)

      _ = logger.info(s"Connecting to domain $config")
      success <- sync.connectDomain(
        domainAlias = config.domain,
        keepRetrying = false,
        connectDomain = ConnectDomain.Connect,
      )
      _ <- waitUntilActiveIfSuccess(success, config.domain)

    } yield v30.ConnectDomainResponse(success)

    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def registerDomain(
      request: v30.RegisterDomainRequest
  ): Future[v30.RegisterDomainResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.RegisterDomainRequest(configPO, domainConnectionP, sequencerConnectionValidationPO) =
      request

    val ret: EitherT[FutureUnlessShutdown, BaseCantonError, v30.RegisterDomainResponse] = for {
      performHandshake <- EitherT.fromEither[FutureUnlessShutdown](
        parseDomainConnection(domainConnectionP)
      )

      config <- EitherT.fromEither[FutureUnlessShutdown](
        parseDomainConnectionConfig(configPO, "config")
      )
      validation <- EitherT.fromEither[FutureUnlessShutdown](
        parseSequencerConnectionValidation(sequencerConnectionValidationPO)
      )
      _ = logger.info(show"Registering new domain $config")
      _ <- sync.addDomain(config, validation)
      _ <-
        if (performHandshake) {
          logger.info(s"Performing handshake to domain $config")
          for {
            success <-
              sync.connectDomain(
                domainAlias = config.domain,
                keepRetrying = false,
                connectDomain = ConnectDomain.HandshakeOnly,
              )
            _ <- waitUntilActiveIfSuccess(success, config.domain)
          } yield ()
        } else EitherT.rightT[FutureUnlessShutdown, BaseCantonError](())
    } yield v30.RegisterDomainResponse()

    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  /** reconfigure a domain connection
    */
  override def modifyDomain(request: v30.ModifyDomainRequest): Future[v30.ModifyDomainResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ModifyDomainRequest(newConfigPO, sequencerConnectionValidationPO) = request
    val ret = for {
      config <- EitherT.fromEither[FutureUnlessShutdown](
        parseDomainConnectionConfig(newConfigPO, "new_config")
      )
      validation <- EitherT.fromEither[FutureUnlessShutdown](
        parseSequencerConnectionValidation(sequencerConnectionValidationPO)
      )
      _ <- sync.modifyDomain(config, validation).leftWiden[BaseCantonError]
    } yield v30.ModifyDomainResponse()
    mapErrNewEUS(ret)
  }

  /** reconnect to domains
    */
  override def reconnectDomains(
      request: v30.ReconnectDomainsRequest
  ): Future[v30.ReconnectDomainsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    import cats.syntax.parallel.*
    val v30.ReconnectDomainsRequest(ignoreFailures) = request
    val ret = for {
      aliases <- sync.reconnectDomains(ignoreFailures = ignoreFailures)
      _ <- aliases.parTraverse(waitUntilActive)
    } yield v30.ReconnectDomainsResponse()
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  /** Get the domain id of the given domain alias
    */
  override def getDomainId(request: v30.GetDomainIdRequest): Future[v30.GetDomainIdResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.GetDomainIdRequest(domainAlias) = request
    val ret = for {
      alias <- parseDomainAlias(domainAlias)
      connectionConfig <-
        sync
          .domainConnectionConfigByAlias(alias)
          .leftMap(_ => SyncServiceUnknownDomain.Error(alias))
          .map(_.config)
          .mapK(FutureUnlessShutdown.outcomeK)
      result <-
        sequencerInfoLoader
          .loadAndAggregateSequencerEndpoints(
            connectionConfig.domain,
            None,
            connectionConfig.sequencerConnections,
            SequencerConnectionValidation.Active,
          )(
            traceContext,
            CloseContext(sync),
          )
          .leftMap[BaseCantonError](err => DomainRegistryError.fromSequencerInfoLoaderError(err))
      _ <- aliasManager
        .processHandshake(connectionConfig.domain, result.domainId)
        .leftMap(DomainRegistryHelpers.fromDomainAliasManagerError)
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftWiden[BaseCantonError]
    } yield v30.GetDomainIdResponse(domainId = result.domainId.toProtoPrimitive)
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

}
