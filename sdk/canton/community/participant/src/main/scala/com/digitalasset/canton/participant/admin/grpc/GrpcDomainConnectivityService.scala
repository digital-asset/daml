// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.domain.v30 as domainV30
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.participant.domain.{
  DomainAliasManager,
  DomainConnectionConfig,
  DomainRegistryError,
  DomainRegistryHelpers,
}
import com.digitalasset.canton.participant.sync.CantonSyncService.ConnectDomain
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInternalError.DomainIsMissingInternally
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceUnknownDomain
import com.digitalasset.canton.participant.sync.{CantonSyncService, SyncServiceError}
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ShowUtil.*

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
    if (success) waitUntilActive(domain) else EitherT.rightT(())

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
    EitherT
      .fromEither[FutureUnlessShutdown](DomainAlias.create(domainAliasProto))
      .leftMap(err => ProtoDeserializationFailure.WrapNoLoggingStr(err))

  private def parseDomainConnectionConfig(
      proto: Option[v30.DomainConnectionConfig],
      name: String,
  ) =
    EitherT
      .fromEither[FutureUnlessShutdown](
        ProtoConverter.parseRequired(DomainConnectionConfig.fromProtoV30, name, proto)
      )
      .leftMap(err => ProtoDeserializationFailure.WrapNoLogging(err))

  private def parseSequencerConnectionValidation(
      proto: domainV30.SequencerConnectionValidation
  ) =
    EitherT
      .fromEither[FutureUnlessShutdown](
        SequencerConnectionValidation.fromProtoV30(proto)
      )
      .leftMap(err => ProtoDeserializationFailure.WrapNoLogging(err))

  override def connectDomain(
      request: v30.ConnectDomainRequest
  ): Future[v30.ConnectDomainResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ConnectDomainRequest(domainAlias, keepRetrying) = request
    val ret = for {
      alias <- parseDomainAlias(domainAlias)
      success <- sync.connectDomain(alias, keepRetrying, ConnectDomain.Connect)
      _ <- waitUntilActiveIfSuccess(success, alias)
    } yield v30.ConnectDomainResponse(connectedSuccessfully = success)
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

  override def listConnectedDomains(
      request: v30.ListConnectedDomainsRequest
  ): Future[v30.ListConnectedDomainsResponse] =
    Future.successful(v30.ListConnectedDomainsResponse(sync.readyDomains.map {
      case (alias, (domainId, healthy)) =>
        new v30.ListConnectedDomainsResponse.Result(
          domainAlias = alias.unwrap,
          domainId = domainId.toProtoPrimitive,
          healthy = healthy,
        )
    }.toSeq))

  override def listConfiguredDomains(
      request: v30.ListConfiguredDomainsRequest
  ): Future[v30.ListConfiguredDomainsResponse] = {
    val connected = sync.readyDomains
    val configuredDomains = sync.configuredDomains
    Future.successful(
      v30.ListConfiguredDomainsResponse(
        results = configuredDomains
          .filter(_.status.isActive)
          .map(_.config)
          .map(cnf =>
            new v30.ListConfiguredDomainsResponse.Result(
              config = Some(cnf.toProtoV30),
              connected = connected.contains(cnf.domain),
            )
          )
      )
    )
  }

  override def registerDomain(
      request: v30.RegisterDomainRequest
  ): Future[v30.RegisterDomainResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.RegisterDomainRequest(addPO, handshakeOnly, sequencerConnectionValidationPO) = request
    val connectDomain = if (handshakeOnly) ConnectDomain.HandshakeOnly else ConnectDomain.Register
    val ret: EitherT[FutureUnlessShutdown, BaseCantonError, v30.RegisterDomainResponse] = for {
      config <- parseDomainConnectionConfig(addPO, "add")
      validation <- parseSequencerConnectionValidation(sequencerConnectionValidationPO)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        !(config.manualConnect && handshakeOnly),
        SyncServiceError.InvalidArgument
          .Error("For handshakeOnly to be useful, manualConnect should be set to false"),
      )
      _ = logger.info(show"Registering new domain $config")
      _ <- sync.addDomain(config, validation).mapK(FutureUnlessShutdown.outcomeK)
      _ <-
        if (!config.manualConnect) for {
          success <-
            sync.connectDomain(
              config.domain,
              keepRetrying = false,
              connectDomain = connectDomain,
            )
          _ <- waitUntilActiveIfSuccess(success, config.domain)
        } yield ()
        else EitherT.rightT[FutureUnlessShutdown, BaseCantonError](())
    } yield v30.RegisterDomainResponse()
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  /** reconfigure a domain connection
    */
  override def modifyDomain(request: v30.ModifyDomainRequest): Future[v30.ModifyDomainResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ModifyDomainRequest(config, sequencerConnectionValidationPO) = request
    val ret = for {
      config <- parseDomainConnectionConfig(config, "modify")
      validation <- parseSequencerConnectionValidation(sequencerConnectionValidationPO)
      _ <- sync
        .modifyDomain(config, validation)
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftWiden[BaseCantonError]
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
            connectionConfig.sequencerConnections,
            SequencerConnectionValidation.Active,
          )(
            traceContext,
            CloseContext(sync),
          )
          .leftMap(err => DomainRegistryError.fromSequencerInfoLoaderError(err))
          .mapK(FutureUnlessShutdown.outcomeK)
          .leftWiden[BaseCantonError]
      _ <- aliasManager
        .processHandshake(connectionConfig.domain, result.domainId)
        .leftMap(DomainRegistryHelpers.fromDomainAliasManagerError)
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftWiden[BaseCantonError]
    } yield v30.GetDomainIdResponse(domainId = result.domainId.toProtoPrimitive)
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

}
