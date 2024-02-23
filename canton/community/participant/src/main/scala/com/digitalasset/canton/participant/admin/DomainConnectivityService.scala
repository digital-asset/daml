// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader.SequencerAggregatedInfo
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.*
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.sync.CantonSyncService.ConnectDomain
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInternalError.DomainIsMissingInternally
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceUnknownDomain
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import io.grpc.StatusRuntimeException

import scala.concurrent.{ExecutionContext, Future}

class DomainConnectivityService(
    sync: CantonSyncService,
    aliasManager: DomainAliasManager,
    timeouts: ProcessingTimeout,
    sequencerInfoLoader: SequencerInfoLoader,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*

  private def waitUntilActiveIfSuccess(success: Boolean, domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[Future, StatusRuntimeException, Unit] =
    if (success) waitUntilActive(domain) else EitherT.rightT(())

  private def waitUntilActive(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): EitherT[Future, StatusRuntimeException, Unit] = {
    val clientE = for {
      domainId <- aliasManager
        .domainIdForAlias(domain)
        .toRight(DomainIsMissingInternally(domain, "aliasManager"))
      client <- sync.syncCrypto.ips
        .forDomain(domainId)
        .toRight(DomainIsMissingInternally(domain, "ips"))
    } yield client
    for {
      client <- mapErrNew(clientE)
      active <- EitherT
        .right(client.await(_.isParticipantActive(sync.participantId), timeouts.network.unwrap))
        .onShutdown(Right(true)) // don't emit ugly warnings on shutdown
      _ <- mapErrNew(
        Either
          .cond(
            active,
            (),
            DomainRegistryError.ConnectionErrors.ParticipantIsNotActive.Error(
              s"While domain $domain promised, participant ${sync.participantId} never became active within a reasonable timeframe."
            ),
          )
      )
    } yield ()

  }

  def connectDomain(domainAlias: String, keepRetrying: Boolean)(implicit
      traceContext: TraceContext
  ): Future[v30.ConnectDomainResponse] =
    for {
      alias <- Future(
        DomainAlias
          .create(domainAlias)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLoggingStr(err).asGrpcError)
      )
      success <- mapErrNewETUS(
        sync.connectDomain(alias, keepRetrying, ConnectDomain.Connect)
      )
        .valueOr(throw _)
      _ <- waitUntilActiveIfSuccess(success, alias).valueOr(throw _)
    } yield v30.ConnectDomainResponse(connectedSuccessfully = success)

  def disconnectDomain(
      domainAlias: String
  )(implicit traceContext: TraceContext): Future[v30.DisconnectDomainResponse] =
    for {
      alias <- Future(
        DomainAlias
          .create(domainAlias)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLoggingStr(err).asGrpcError)
      )
      _ <- mapErrNewETUS(sync.disconnectDomain(alias)).valueOr(throw _)
    } yield v30.DisconnectDomainResponse()

  def listConnectedDomains(): Future[v30.ListConnectedDomainsResponse] =
    Future.successful(v30.ListConnectedDomainsResponse(sync.readyDomains.map {
      case (alias, (domainId, healthy)) =>
        new v30.ListConnectedDomainsResponse.Result(
          domainAlias = alias.unwrap,
          domainId = domainId.toProtoPrimitive,
          healthy = healthy,
        )
    }.toSeq))

  def listConfiguredDomains(): Future[v30.ListConfiguredDomainsResponse] = {
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

  def registerDomain(
      request: v30.DomainConnectionConfig,
      handshakeOnly: Boolean,
  )(implicit traceContext: TraceContext): Future[v30.RegisterDomainResponse] = {

    val connectDomain = if (handshakeOnly) ConnectDomain.HandshakeOnly else ConnectDomain.Register

    for {
      conf <- Future(
        DomainConnectionConfig
          .fromProtoV30(request)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )

      _ = logger.info(show"Registering ${request.domainAlias} with ${conf}")
      _ <- mapErrNewET(sync.addDomain(conf)).valueOr(throw _)

      _ <-
        if (!conf.manualConnect) for {
          success <- mapErrNewETUS(
            sync.connectDomain(
              conf.domain,
              keepRetrying = false,
              connectDomain = connectDomain,
            )
          )
            .valueOr(throw _)
          _ <- waitUntilActiveIfSuccess(success, conf.domain).valueOr(throw _)
        } yield ()
        else Future.unit
    } yield v30.RegisterDomainResponse()
  }

  def modifyDomain(
      request: v30.DomainConnectionConfig
  )(implicit traceContext: TraceContext): Future[v30.ModifyDomainResponse] =
    for {
      conf <- Future(
        DomainConnectionConfig
          .fromProtoV30(request)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      _ <- mapErrNewET(sync.modifyDomain(conf)).valueOr(throw _)
    } yield v30.ModifyDomainResponse()

  private def getSequencerAggregatedInfo(domainAlias: String)(implicit
      traceContext: TraceContext
  ): Future[SequencerAggregatedInfo] = {
    for {
      alias <- Future(
        DomainAlias
          .create(domainAlias)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLoggingStr(err).asGrpcError)
      )
      connectionConfig <- mapErrNewET(
        sync
          .domainConnectionConfigByAlias(alias)
          .leftMap(_ => SyncServiceUnknownDomain.Error(alias))
          .map(_.config)
      ).valueOr(throw _)
      result <-
        sequencerInfoLoader
          .loadSequencerEndpoints(connectionConfig.domain, connectionConfig.sequencerConnections)(
            traceContext,
            CloseContext(sync),
          )
          .valueOr(err => throw DomainRegistryError.fromSequencerInfoLoaderError(err).asGrpcError)
      _ <- aliasManager
        .processHandshake(connectionConfig.domain, result.domainId)
        .leftMap(DomainRegistryHelpers.fromDomainAliasManagerError)
        .valueOr(err => throw err.asGrpcError)
    } yield result
  }

  def reconnectDomains(ignoreFailures: Boolean): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      aliases <- mapErrNewETUS(sync.reconnectDomains(ignoreFailures = ignoreFailures))
      _ <- aliases.parTraverse(waitUntilActive)
    } yield ()
    EitherTUtil.toFuture(ret)
  }

  def getDomainId(domainAlias: String)(implicit traceContext: TraceContext): Future[DomainId] =
    getSequencerAggregatedInfo(domainAlias).map(_.domainId)

}
