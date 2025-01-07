// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.RegisterDomainRequest.DomainConnection
import com.digitalasset.canton.admin.participant.v30.{
  DisconnectAllDomainsRequest,
  DisconnectAllDomainsResponse,
}
import com.digitalasset.canton.admin.sequencer.v30 as sequencerV30
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.sync.CantonSyncService.ConnectDomain
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInternalError.DomainIsMissingInternally
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceUnknownDomain
import com.digitalasset.canton.participant.synchronizer.{
  DomainConnectionConfig,
  DomainRegistryHelpers,
  SynchronizerAliasManager,
  SynchronizerRegistryError,
}
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{ProtoDeserializationError, SynchronizerAlias}
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcDomainConnectivityService(
    sync: CantonSyncService,
    aliasManager: SynchronizerAliasManager,
    timeouts: ProcessingTimeout,
    sequencerInfoLoader: SequencerInfoLoader,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends v30.DomainConnectivityServiceGrpc.DomainConnectivityService
    with NamedLogging {

  private def waitUntilActiveIfSuccess(success: Boolean, synchronizerAlias: SynchronizerAlias)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, BaseCantonError, Unit] =
    if (success) waitUntilActive(synchronizerAlias) else EitherTUtil.unitUS[BaseCantonError]

  private def waitUntilActive(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, BaseCantonError, Unit] =
    for {
      synchronizerId <- EitherT.fromOption[FutureUnlessShutdown](
        aliasManager
          .synchronizerIdForAlias(synchronizerAlias),
        DomainIsMissingInternally(synchronizerAlias, "aliasManager"),
      )
      client <- EitherT.fromOption[FutureUnlessShutdown](
        sync.syncCrypto.ips
          .forSynchronizer(synchronizerId),
        DomainIsMissingInternally(synchronizerAlias, "ips"),
      )
      active <- EitherT
        .right(client.awaitUS(_.isParticipantActive(sync.participantId), timeouts.network.unwrap))
      _ <-
        EitherT
          .cond[FutureUnlessShutdown](
            active,
            (),
            SynchronizerRegistryError.ConnectionErrors.ParticipantIsNotActive.Error(
              s"While synchronizerAlias $synchronizerAlias promised, participant ${sync.participantId} never became active within `timeouts.network` (${timeouts.network})."
            ): BaseCantonError,
          )
    } yield ()

  private def parseSynchronizerAlias(
      synchronizerAliasP: String
  ): EitherT[FutureUnlessShutdown, BaseCantonError, SynchronizerAlias] =
    EitherT.fromEither[FutureUnlessShutdown](
      SynchronizerAlias
        .create(synchronizerAliasP)
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
      proto: sequencerV30.SequencerConnectionValidation
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
    val v30.ReconnectDomainRequest(synchronizerAlias, keepRetrying) = request
    val ret = for {
      alias <- parseSynchronizerAlias(synchronizerAlias)
      success <- sync.connectDomain(alias, keepRetrying, ConnectDomain.Connect)
      _ <- waitUntilActiveIfSuccess(success, alias)
    } yield v30.ReconnectDomainResponse(connectedSuccessfully = success)
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def disconnectDomain(
      request: v30.DisconnectDomainRequest
  ): Future[v30.DisconnectDomainResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.DisconnectDomainRequest(synchronizerAlias) = request
    val ret = for {
      alias <- parseSynchronizerAlias(synchronizerAlias)
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
    val v30.LogoutRequest(synchronizerAliasP) = request

    val ret = for {
      synchronizerAlias <- EitherT
        .fromEither[Future](SynchronizerAlias.create(synchronizerAliasP))
        .leftMap(err =>
          Status.INVALID_ARGUMENT
            .withDescription(s"Failed to parse synchronizer alias: $err")
            .asRuntimeException()
        )
      _ <- sync
        .logout(synchronizerAlias)
        .leftMap(err => err.asRuntimeException())
        .onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError))
    } yield v30.LogoutResponse()

    EitherTUtil.toFuture(ret)
  }

  override def listConnectedDomains(
      request: v30.ListConnectedDomainsRequest
  ): Future[v30.ListConnectedDomainsResponse] =
    Future.successful(v30.ListConnectedDomainsResponse(sync.readyDomains.map {
      case (alias, (synchronizerId, healthy)) =>
        new v30.ListConnectedDomainsResponse.Result(
          synchronizerAlias = alias.unwrap,
          synchronizerId = synchronizerId.toProtoPrimitive,
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
              connected = connected.contains(cnf.synchronizerAlias),
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
        synchronizerAlias = config.synchronizerAlias,
        keepRetrying = false,
        connectDomain = ConnectDomain.Connect,
      )
      _ <- waitUntilActiveIfSuccess(success, config.synchronizerAlias)

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
                synchronizerAlias = config.synchronizerAlias,
                keepRetrying = false,
                connectDomain = ConnectDomain.HandshakeOnly,
              )
            _ <- waitUntilActiveIfSuccess(success, config.synchronizerAlias)
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

  /** Get the synchronizer id of the given synchronizer alias
    */
  override def getSynchronizerId(
      request: v30.GetSynchronizerIdRequest
  ): Future[v30.GetSynchronizerIdResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.GetSynchronizerIdRequest(synchronizerAlias) = request
    val ret = for {
      alias <- parseSynchronizerAlias(synchronizerAlias)
      connectionConfig <-
        sync
          .domainConnectionConfigByAlias(alias)
          .leftMap(_ => SyncServiceUnknownDomain.Error(alias))
          .map(_.config)
          .mapK(FutureUnlessShutdown.outcomeK)
      result <-
        sequencerInfoLoader
          .loadAndAggregateSequencerEndpoints(
            connectionConfig.synchronizerAlias,
            None,
            connectionConfig.sequencerConnections,
            SequencerConnectionValidation.Active,
          )(
            traceContext,
            CloseContext(sync),
          )
          .leftMap[BaseCantonError](err =>
            SynchronizerRegistryError.fromSequencerInfoLoaderError(err)
          )
      _ <- aliasManager
        .processHandshake(connectionConfig.synchronizerAlias, result.synchronizerId)
        .leftMap(DomainRegistryHelpers.fromSynchronizerAliasManagerError)
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftWiden[BaseCantonError]
    } yield v30.GetSynchronizerIdResponse(synchronizerId = result.synchronizerId.toProtoPrimitive)
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

}
