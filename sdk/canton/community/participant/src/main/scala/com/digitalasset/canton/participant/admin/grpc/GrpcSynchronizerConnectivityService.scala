// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.traverse.*
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
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.sync.CantonSyncService.ConnectSynchronizer
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInternalError.SynchronizerIsMissingInternally
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceUnknownSynchronizer
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasManager,
  SynchronizerConnectionConfig,
  SynchronizerRegistryError,
  SynchronizerRegistryHelpers,
}
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.PhysicalSynchronizerId
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

  private def _mapErrNewEUS[C](res: EitherT[FutureUnlessShutdown, CantonBaseError, C])(implicit
      ec: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[C] = CantonGrpcUtil.mapErrNewEUS(res.leftMap(_.toCantonRpcError))

  private def waitUntilActiveIfSuccess(success: Boolean, synchronizerAlias: SynchronizerAlias)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] =
    if (success) waitUntilActive(synchronizerAlias) else EitherTUtil.unitUS[CantonBaseError]

  private def waitUntilActive(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] =
    for {
      synchronizerId <- EitherT.fromOption[FutureUnlessShutdown](
        aliasManager
          .synchronizerIdForAlias(synchronizerAlias),
        SynchronizerIsMissingInternally(synchronizerAlias, "aliasManager"),
      )
      client <- EitherT.fromOption[FutureUnlessShutdown](
        sync.syncCrypto.ips
          .forSynchronizer(synchronizerId),
        SynchronizerIsMissingInternally(synchronizerAlias, "ips"),
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
            ): CantonBaseError,
          )
    } yield ()

  private def parseSynchronizerAlias(
      synchronizerAliasP: String
  ): EitherT[FutureUnlessShutdown, CantonBaseError, SynchronizerAlias] =
    EitherT.fromEither[FutureUnlessShutdown](
      SynchronizerAlias
        .create(synchronizerAliasP)
        .leftMap(ProtoDeserializationFailure.WrapNoLoggingStr.apply)
    )

  private def parseSynchronizerConnectionConfig(
      proto: Option[v30.SynchronizerConnectionConfig],
      name: String,
  ): Either[CantonBaseError, SynchronizerConnectionConfig] =
    ProtoConverter
      .parseRequired(SynchronizerConnectionConfig.fromProtoV30, name, proto)
      .leftMap(ProtoDeserializationFailure.WrapNoLogging.apply)

  private def parseSequencerConnectionValidation(
      proto: sequencerV30.SequencerConnectionValidation
  ): Either[CantonBaseError, SequencerConnectionValidation] =
    SequencerConnectionValidation
      .fromProtoV30(proto)
      .leftMap(ProtoDeserializationFailure.WrapNoLogging.apply)

  /** @param synchronizerConnectionP
    *   Protobuf data
    * @return
    *   True if handshake should be done, false otherwise
    */
  private def parseSynchronizerConnection(
      synchronizerConnectionP: v30.RegisterSynchronizerRequest.SynchronizerConnection
  ): Either[ProtoDeserializationFailure.WrapNoLogging, Boolean] = (synchronizerConnectionP match {
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
    _mapErrNewEUS(ret)
  }

  override def disconnectSynchronizer(
      request: v30.DisconnectSynchronizerRequest
  ): Future[v30.DisconnectSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.DisconnectSynchronizerRequest(synchronizerAlias) = request
    val ret = for {
      alias <- parseSynchronizerAlias(synchronizerAlias)
      _ <- sync.disconnectSynchronizer(alias).leftWiden[CantonBaseError]
    } yield v30.DisconnectSynchronizerResponse()
    _mapErrNewEUS(ret)
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
          physicalSynchronizerId = synchronizerId.toProtoPrimitive,
          synchronizerId = synchronizerId.logical.toProtoPrimitive,
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
          .map(cnf =>
            new v30.ListRegisteredSynchronizersResponse.Result(
              config = Some(cnf.config.toProtoV30),
              connected = connected.contains(cnf.config.synchronizerAlias),
              physicalSynchronizerId = cnf.configuredPSId.toOption.map(_.toProtoPrimitive),
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

    val ret: EitherT[FutureUnlessShutdown, CantonBaseError, v30.ConnectSynchronizerResponse] = for {
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

    _mapErrNewEUS(ret)
  }

  override def registerSynchronizer(
      request: v30.RegisterSynchronizerRequest
  ): Future[v30.RegisterSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.RegisterSynchronizerRequest(
      configPO,
      synchronizerConnectionP,
      sequencerConnectionValidationPO,
    ) =
      request

    val ret: EitherT[FutureUnlessShutdown, CantonBaseError, v30.RegisterSynchronizerResponse] =
      for {
        performHandshake <- EitherT.fromEither[FutureUnlessShutdown](
          parseSynchronizerConnection(synchronizerConnectionP)
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
            // Since we don't retry, any error is returned as left and therefore will be returned to the caller.
            // If the connectSynchronizer is successful, it means that the topology has been successfully initalized.
            sync
              .connectSynchronizer(
                synchronizerAlias = config.synchronizerAlias,
                keepRetrying = false,
                connectSynchronizer = ConnectSynchronizer.HandshakeOnly,
              )
              .leftWiden[CantonBaseError]
          } else EitherT.rightT[FutureUnlessShutdown, CantonBaseError](())
      } yield v30.RegisterSynchronizerResponse()

    _mapErrNewEUS(ret)
  }

  /** reconfigure a synchronizer connection
    */
  override def modifySynchronizer(
      request: v30.ModifySynchronizerRequest
  ): Future[v30.ModifySynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ModifySynchronizerRequest(psidPO, newConfigPO, sequencerConnectionValidationPO) =
      request

    val ret = for {
      psidO <- EitherT
        .fromEither[FutureUnlessShutdown](
          psidPO.traverse(PhysicalSynchronizerId.fromProtoPrimitive(_, "physical_synchronizer_id"))
        )
        .leftMap(err => ProtoDeserializationFailure.WrapNoLoggingStr(err.message))
      config <- EitherT.fromEither[FutureUnlessShutdown](
        parseSynchronizerConnectionConfig(newConfigPO, "new_config")
      )
      validation <- EitherT.fromEither[FutureUnlessShutdown](
        parseSequencerConnectionValidation(sequencerConnectionValidationPO)
      )
      _ <- sync.modifySynchronizer(psidO, config, validation).leftWiden[CantonBaseError]
    } yield v30.ModifySynchronizerResponse()
    _mapErrNewEUS(ret)
  }

  /** reconnect to synchronizers
    */
  override def reconnectSynchronizers(
      request: v30.ReconnectSynchronizersRequest
  ): Future[v30.ReconnectSynchronizersResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    import cats.syntax.parallel.*
    val v30.ReconnectSynchronizersRequest(ignoreFailures) = request
    val ret = for {
      aliases <- sync.reconnectSynchronizers(
        ignoreFailures = ignoreFailures,
        mustBeActive = true,
        isTriggeredManually = true,
      )
      _ <- aliases.parTraverse(waitUntilActive)
    } yield v30.ReconnectSynchronizersResponse()
    _mapErrNewEUS(ret)
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
        EitherT
          .fromEither[FutureUnlessShutdown](
            sync.getSynchronizerConnectionConfigForAlias(alias, onlyActive = true)
          )
          .leftMap(_ => SyncServiceUnknownSynchronizer.Error(alias))
          .map(_.config)
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
          .leftMap[CantonBaseError](err =>
            SynchronizerRegistryError.fromSequencerInfoLoaderError(err)
          )
      _ <- aliasManager
        .processHandshake(connectionConfig.synchronizerAlias, result.synchronizerId)
        .leftMap(SynchronizerRegistryHelpers.fromSynchronizerAliasManagerError)
        .leftWiden[CantonBaseError]
    } yield v30.GetSynchronizerIdResponse(
      physicalSynchronizerId = result.synchronizerId.toProtoPrimitive,
      synchronizerId = result.synchronizerId.logical.toProtoPrimitive,
    )
    _mapErrNewEUS(ret)
  }

}
