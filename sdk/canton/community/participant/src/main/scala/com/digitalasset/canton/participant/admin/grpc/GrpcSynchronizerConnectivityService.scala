// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.RegisterSynchronizerRequest.SynchronizerConnection
import com.digitalasset.canton.admin.participant.v30.{
  DisconnectAllSynchronizersRequest,
  DisconnectAllSynchronizersResponse,
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
import com.digitalasset.canton.participant.sync.CantonSyncService.ConnectSynchronizer
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInternalError.DomainIsMissingInternally
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceUnknownSynchronizer
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasManager,
  SynchronizerConnectionConfig,
  SynchronizerRegistryError,
  SynchronizerRegistryHelpers,
}
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{ProtoDeserializationError, SynchronizerAlias}
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcSynchronizerConnectivityService(
    sync: CantonSyncService,
    aliasManager: SynchronizerAliasManager,
    timeouts: ProcessingTimeout,
    sequencerInfoLoader: SequencerInfoLoader,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends v30.SynchronizerConnectivityServiceGrpc.SynchronizerConnectivityService
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

  private def parseSynchronizerConnectionConfig(
      proto: Option[v30.SynchronizerConnectionConfig],
      name: String,
  ): Either[BaseCantonError, SynchronizerConnectionConfig] =
    ProtoConverter
      .parseRequired(SynchronizerConnectionConfig.fromProtoV30, name, proto)
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
  private def parseSynchronizerConnection(
      domainConnectionP: v30.RegisterSynchronizerRequest.SynchronizerConnection
  ): Either[ProtoDeserializationFailure.WrapNoLogging, Boolean] = (domainConnectionP match {
    case SynchronizerConnection.SYNCHRONIZER_CONNECTION_UNSPECIFIED =>
      ProtoDeserializationError.FieldNotSet("synchronizer_connection").asLeft
    case SynchronizerConnection.SYNCHRONIZER_CONNECTION_NONE => Right(false)
    case SynchronizerConnection.SYNCHRONIZER_CONNECTION_HANDSHAKE => Right(true)
    case SynchronizerConnection.Unrecognized(unrecognizedValue) =>
      ProtoDeserializationError
        .UnrecognizedEnum("synchronizer_connection", unrecognizedValue)
        .asLeft
  }).leftMap(ProtoDeserializationFailure.WrapNoLogging.apply)

  override def reconnectSynchronizer(
      request: v30.ReconnectSynchronizerRequest
  ): Future[v30.ReconnectSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ReconnectSynchronizerRequest(synchronizerAlias, keepRetrying) = request
    val ret = for {
      alias <- parseSynchronizerAlias(synchronizerAlias)
      success <- sync.connectSynchronizer(alias, keepRetrying, ConnectSynchronizer.Connect)
      _ <- waitUntilActiveIfSuccess(success, alias)
    } yield v30.ReconnectSynchronizerResponse(connectedSuccessfully = success)
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def disconnectSynchronizer(
      request: v30.DisconnectSynchronizerRequest
  ): Future[v30.DisconnectSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.DisconnectSynchronizerRequest(synchronizerAlias) = request
    val ret = for {
      alias <- parseSynchronizerAlias(synchronizerAlias)
      _ <- sync.disconnectSynchronizer(alias).leftWiden[BaseCantonError]
    } yield v30.DisconnectSynchronizerResponse()
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def disconnectAllSynchronizers(
      request: DisconnectAllSynchronizersRequest
  ): Future[DisconnectAllSynchronizersResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    CantonGrpcUtil
      .mapErrNewEUS(sync.disconnectSynchronizers())
      .map(_ => DisconnectAllSynchronizersResponse())
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

  override def listConnectedSynchronizers(
      request: v30.ListConnectedSynchronizersRequest
  ): Future[v30.ListConnectedSynchronizersResponse] =
    Future.successful(v30.ListConnectedSynchronizersResponse(sync.readySynchronizers.map {
      case (alias, (synchronizerId, healthy)) =>
        new v30.ListConnectedSynchronizersResponse.Result(
          synchronizerAlias = alias.unwrap,
          synchronizerId = synchronizerId.toProtoPrimitive,
          healthy = healthy.unwrap,
        )
    }.toSeq))

  override def listRegisteredSynchronizers(
      request: v30.ListRegisteredSynchronizersRequest
  ): Future[v30.ListRegisteredSynchronizersResponse] = {
    val connected = sync.readySynchronizers
    val registeredSynchronizers = sync.registeredSynchronizers

    Future.successful(
      v30.ListRegisteredSynchronizersResponse(
        results = registeredSynchronizers
          .filter(_.status.isActive)
          .map(_.config)
          .map(cnf =>
            new v30.ListRegisteredSynchronizersResponse.Result(
              config = Some(cnf.toProtoV30),
              connected = connected.contains(cnf.synchronizerAlias),
            )
          )
      )
    )
  }

  override def connectSynchronizer(
      request: v30.ConnectSynchronizerRequest
  ): Future[v30.ConnectSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ConnectSynchronizerRequest(configPO, sequencerConnectionValidationPO) =
      request

    val ret: EitherT[FutureUnlessShutdown, BaseCantonError, v30.ConnectSynchronizerResponse] = for {
      config <- EitherT.fromEither[FutureUnlessShutdown](
        parseSynchronizerConnectionConfig(configPO, "config")
      )
      validation <- EitherT.fromEither[FutureUnlessShutdown](
        parseSequencerConnectionValidation(sequencerConnectionValidationPO)
      )
      _ = logger.info(show"Registering new synchronizer $config")
      _ <- sync.addSynchronizer(config, validation)

      _ = logger.info(s"Connecting to synchronizer $config")
      success <- sync.connectSynchronizer(
        synchronizerAlias = config.synchronizerAlias,
        keepRetrying = false,
        connectSynchronizer = ConnectSynchronizer.Connect,
      )
      _ <- waitUntilActiveIfSuccess(success, config.synchronizerAlias)

    } yield v30.ConnectSynchronizerResponse(success)

    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def registerSynchronizer(
      request: v30.RegisterSynchronizerRequest
  ): Future[v30.RegisterSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.RegisterSynchronizerRequest(
      configPO,
      domainConnectionP,
      sequencerConnectionValidationPO,
    ) =
      request

    val ret: EitherT[FutureUnlessShutdown, BaseCantonError, v30.RegisterSynchronizerResponse] =
      for {
        performHandshake <- EitherT.fromEither[FutureUnlessShutdown](
          parseSynchronizerConnection(domainConnectionP)
        )

        config <- EitherT.fromEither[FutureUnlessShutdown](
          parseSynchronizerConnectionConfig(configPO, "config")
        )
        validation <- EitherT.fromEither[FutureUnlessShutdown](
          parseSequencerConnectionValidation(sequencerConnectionValidationPO)
        )
        _ = logger.info(show"Registering new synchronizer $config")
        _ <- sync.addSynchronizer(config, validation)
        _ <-
          if (performHandshake) {
            logger.info(s"Performing handshake to synchronizer $config")
            for {
              success <-
                sync.connectSynchronizer(
                  synchronizerAlias = config.synchronizerAlias,
                  keepRetrying = false,
                  connectSynchronizer = ConnectSynchronizer.HandshakeOnly,
                )
              _ <- waitUntilActiveIfSuccess(success, config.synchronizerAlias)
            } yield ()
          } else EitherT.rightT[FutureUnlessShutdown, BaseCantonError](())
      } yield v30.RegisterSynchronizerResponse()

    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  /** reconfigure a synchronizer connection
    */
  override def modifySynchronizer(
      request: v30.ModifySynchronizerRequest
  ): Future[v30.ModifySynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ModifySynchronizerRequest(newConfigPO, sequencerConnectionValidationPO) = request
    val ret = for {
      config <- EitherT.fromEither[FutureUnlessShutdown](
        parseSynchronizerConnectionConfig(newConfigPO, "new_config")
      )
      validation <- EitherT.fromEither[FutureUnlessShutdown](
        parseSequencerConnectionValidation(sequencerConnectionValidationPO)
      )
      _ <- sync.modifySynchronizer(config, validation).leftWiden[BaseCantonError]
    } yield v30.ModifySynchronizerResponse()
    mapErrNewEUS(ret)
  }

  /** reconnect to domains
    */
  override def reconnectSynchronizers(
      request: v30.ReconnectSynchronizersRequest
  ): Future[v30.ReconnectSynchronizersResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    import cats.syntax.parallel.*
    val v30.ReconnectSynchronizersRequest(ignoreFailures) = request
    val ret = for {
      aliases <- sync.reconnectSynchronizers(ignoreFailures = ignoreFailures)
      _ <- aliases.parTraverse(waitUntilActive)
    } yield v30.ReconnectSynchronizersResponse()
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
          .synchronizerConnectionConfigByAlias(alias)
          .leftMap(_ => SyncServiceUnknownSynchronizer.Error(alias))
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
        .leftMap(SynchronizerRegistryHelpers.fromSynchronizerAliasManagerError)
        .leftWiden[BaseCantonError]
    } yield v30.GetSynchronizerIdResponse(synchronizerId = result.synchronizerId.toProtoPrimitive)
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

}
