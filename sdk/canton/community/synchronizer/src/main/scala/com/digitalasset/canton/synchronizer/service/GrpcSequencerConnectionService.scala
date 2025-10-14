// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.service

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader.SequencerAggregatedInfo
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.mediator.admin.v30.SequencerConnectionServiceGrpc.SequencerConnectionService
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonMutableHandlerRegistry}
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.SequencerConnectionXPoolError
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.{
  RequestSigner,
  RichSequencerClient,
  SequencerClient,
  SequencerClientTransportFactory,
}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnectionValidation,
  SequencerConnectionXPool,
  SequencerConnectionXPoolFactory,
  SequencerConnections,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc, TracingConfig}
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, retry}
import io.grpc.{Status, StatusException}
import monocle.Lens
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class GrpcSequencerConnectionService(
    fetchConnection: () => FutureUnlessShutdown[Option[SequencerConnections]],
    setConnection: (SequencerConnectionValidation, SequencerConnections) => EitherT[
      FutureUnlessShutdown,
      String,
      Unit,
    ],
    logout: () => EitherT[FutureUnlessShutdown, Status, Unit],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends v30.SequencerConnectionServiceGrpc.SequencerConnectionService
    with NamedLogging {
  override def getConnection(
      request: v30.GetConnectionRequest
  ): Future[v30.GetConnectionResponse] =
    TraceContextGrpc.withGrpcTraceContext { implicit traceContext =>
      CantonGrpcUtil.shutdownAsGrpcError(
        fetchConnection().map { sequencerConnectionsO =>
          v30.GetConnectionResponse(sequencerConnectionsO.map(_.toProtoV30))
        }
      )
    }

  override def setConnection(request: v30.SetConnectionRequest): Future[v30.SetConnectionResponse] =
    TraceContextGrpc.withGrpcTraceContext { implicit traceContext =>
      EitherTUtil.toFuture(CantonGrpcUtil.shutdownAsGrpcErrorE(for {
        existing <- EitherT.right(getConnection)
        requestedReplacement <- EitherT.fromEither[FutureUnlessShutdown](parseConnection(request))
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          validateReplacement(
            existing,
            requestedReplacement,
          )
        )
        validation <- EitherT.fromEither[FutureUnlessShutdown](
          SequencerConnectionValidation
            .fromProtoV30(request.sequencerConnectionValidation)
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.message).asException())
        )
        _ <- setConnection(validation, requestedReplacement)
          .leftMap(error => Status.FAILED_PRECONDITION.withDescription(error).asException())
      } yield v30.SetConnectionResponse()))
    }

  private def getConnection: FutureUnlessShutdown[SequencerConnections] =
    fetchConnection().map(
      _.getOrElse(
        throw Status.FAILED_PRECONDITION
          .withDescription("Initialize node before attempting to change sequencer connection")
          .asException()
      )
    )

  private def parseConnection(
      request: v30.SetConnectionRequest
  ): Either[StatusException, SequencerConnections] = {
    val v30.SetConnectionRequest(sequencerConnectionsPO, validation) = request
    ProtoConverter
      .parseRequired(
        SequencerConnections.fromProtoV30,
        "sequencerConnections",
        sequencerConnectionsPO,
      )
      .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.message).asException())
  }

  private def validateReplacement(
      existing: SequencerConnections,
      requestedReplacement: SequencerConnections,
  ): Either[StatusException, Unit] =
    (existing.default, requestedReplacement.default) match {
      // TODO(i12076): How should we be checking connections here? what should be validated?
      case (_: GrpcSequencerConnection, _: GrpcSequencerConnection) =>
        Either.unit
      case _ =>
        Left(
          Status.INVALID_ARGUMENT
            .withDescription(
              "requested replacement connection info is not of the same type as the existing"
            )
            .asException()
        )
    }

  /** Revoke the authentication tokens on a sequencer and disconnect the sequencer client
    */
  override def logout(
      request: v30.LogoutRequest
  ): Future[v30.LogoutResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val ret = for {
      _ <- logout()
        .leftMap(err => err.asRuntimeException())
        .onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError))
    } yield v30.LogoutResponse()

    EitherTUtil.toFuture(ret)
  }
}

object GrpcSequencerConnectionService extends HasLoggerName {

  trait UpdateSequencerClient {
    def set(client: RichSequencerClient): Unit
  }

  def setup[C](member: Member, useNewConnectionPool: Boolean)(
      registry: CantonMutableHandlerRegistry,
      fetchConfig: () => FutureUnlessShutdown[Option[C]],
      saveConfig: C => FutureUnlessShutdown[Unit],
      sequencerConnectionLens: Lens[C, SequencerConnections],
      requestSigner: RequestSigner,
      transportFactory: SequencerClientTransportFactory,
      sequencerInfoLoader: SequencerInfoLoader,
      connectionPoolFactory: SequencerConnectionXPoolFactory,
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: PhysicalSynchronizerId,
      sequencerClient: SequencerClient,
      tracingConfig: TracingConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionServiceFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): UpdateSequencerClient = {
    val clientO = new AtomicReference[Option[RichSequencerClient]](None)
    implicit val namedLoggingContext: NamedLoggingContext =
      NamedLoggingContext(loggerFactory, traceContext)
    registry.addServiceU(
      SequencerConnectionService.bindService(
        new GrpcSequencerConnectionService(
          fetchConnection = () => fetchConfig().map(_.map(sequencerConnectionLens.get)),
          setConnection = (sequencerConnectionValidation, newSequencerConnection) =>
            for {
              currentConfig <- OptionT(fetchConfig()).toRight(
                "Can't update config when none has yet been set. Please initialize node."
              )
              newConfig = sequencerConnectionLens.replace(newSequencerConnection)(currentConfig)

              // load and potentially validate the new connection
              //
              // Retries are not strictly necessary for the "change sequencer connection" scenario because the API is
              // idempotent and retries on the client-side are cheap. So by not retrying internally, the application
              // gets some error feedback more quickly and can in theory react to it. Whether callers will reasonably
              // inspect the returned error is debatable though.
              // In principle, mediator node start-up could also fail without retrying and rely on the container
              // framework to restart the pod. But that's a much more expensive operation, so it kinda makes sense to
              // retry there (see `waitUntilSequencerConnectionIsValidWithPool`).
              newEndpointsInfoAndPoolConfigO <-
                if (useNewConnectionPool) for {
                  // The following implementation strives to keep the same behavior as with the transport mechanisms,
                  // which is ensure the new config is valid before replacing the old config.
                  // The transport mechanism supports a variety of validation modes, whereas here we support only the
                  // equivalent to `THRESHOLD_ACTIVE`, i.e. the config is considered valid if at least trust-threshold-many
                  // connections are successful.
                  //
                  // Performing this validation here protects the node operator from typos in the connection config that
                  // would render their node dysfunctional because it cannot connect to the sequencer.
                  // On the other hand, the operator should be able to set a new configuration in case of a substantial
                  // change to sequencer endpoints and that should be doable concurrently to those sequencer endpoint
                  // changes taking place, so one could argue that we should not validate the config here and rely on the
                  // pool to report through health status.
                  //
                  // As we cannot satisfy both needs here, this will likely be discussed and revisited.
                  connectionPoolAndInfo <- validateConfig(
                    connectionPoolFactory = connectionPoolFactory,
                    sequencerConnections = newSequencerConnection,
                    poolName = "temp",
                    tracingConfig = tracingConfig,
                  )
                } yield {
                  val (pool, info) = connectionPoolAndInfo
                  pool.close()
                  (info, Some(pool.config))
                }
                else
                  sequencerInfoLoader
                    .loadAndAggregateSequencerEndpoints(
                      synchronizerAlias,
                      Some(synchronizerId),
                      newSequencerConnection,
                      sequencerConnectionValidation,
                    )
                    .leftMap(_.cause)
                    .map((_, None))
              (newEndpointsInfo, newPoolConfigO) = newEndpointsInfoAndPoolConfigO

              sequencerTransportsMapO = Option.when(!useNewConnectionPool)(
                transportFactory
                  .makeTransport(
                    newEndpointsInfo.sequencerConnections,
                    member,
                    requestSigner,
                    // We are not interested in replay for the connection service.
                    allowReplay = false,
                  )
              )

              sequencerTransports <- EitherT.fromEither[FutureUnlessShutdown](
                SequencerTransports.from(
                  sequencerTransportsMapO,
                  newEndpointsInfo.expectedSequencersO,
                  newEndpointsInfo.sequencerConnections.sequencerTrustThreshold,
                  newEndpointsInfo.sequencerConnections.sequencerLivenessMargin,
                  newEndpointsInfo.sequencerConnections.submissionRequestAmplification,
                  newEndpointsInfo.sequencerConnections.sequencerConnectionPoolDelays,
                )
              )

              // important to only save the config and change the transport after the `makeTransport` has run and done the handshake
              _ <- clientO.get.fold {
                // need to close here
                sequencerTransportsMapO.foreach(_.values.foreach(_.close()))
                EitherT.pure[FutureUnlessShutdown, String](())
              }(
                _.changeTransport(sequencerTransports, newPoolConfigO)
              )
              _ <- EitherT.right(saveConfig(newConfig))
            } yield (),
          sequencerClient.logout _,
          loggerFactory,
        ),
        executionContext,
      )
    )
    new UpdateSequencerClient {
      override def set(client: RichSequencerClient): Unit = clientO.set(Some(client))
    }
  }

  def waitUntilSequencerConnectionIsValid(
      sequencerInfoLoader: SequencerInfoLoader,
      flagCloseable: FlagCloseable,
      loadConfig: => FutureUnlessShutdown[Option[SequencerConnections]],
  )(implicit
      namedLoggingContext: NamedLoggingContext,
      executionContext: ExecutionContextExecutor,
  ): EitherT[FutureUnlessShutdown, String, SequencerAggregatedInfo] = {
    implicit val traceContext: TraceContext = namedLoggingContext.traceContext

    implicit val closeContext = CloseContext(flagCloseable)
    val alias = SynchronizerAlias.tryCreate("synchronizer")

    def tryNewConfig: EitherT[FutureUnlessShutdown, String, SequencerAggregatedInfo] =
      OptionT(loadConfig)
        .toRight("No sequencer connection config")
        .flatMap { settings =>
          sequencerInfoLoader
            .loadAndAggregateSequencerEndpoints(
              synchronizerAlias = alias,
              expectedSynchronizerId = None,
              sequencerConnections = settings,
              sequencerConnectionValidation = SequencerConnectionValidation.Active,
            )
            .leftMap { e =>
              namedLoggingContext.warn(s"Waiting for valid sequencer connection $e")
              e.toString
            }
        }
    import scala.concurrent.duration.*
    EitherT(
      retry
        .Pause(
          namedLoggingContext.tracedLogger,
          flagCloseable,
          maxRetries = Int.MaxValue,
          delay = 50.millis,
          operationName = "wait-for-valid-sequencer-connection",
        )
        .unlessShutdown(
          tryNewConfig.value,
          NoExceptionRetryPolicy,
        )
    )
  }

  def waitUntilSequencerConnectionIsValidWithPool(
      connectionPoolFactory: SequencerConnectionXPoolFactory,
      tracingConfig: TracingConfig,
      flagCloseable: FlagCloseable,
      loadConfig: => FutureUnlessShutdown[Option[SequencerConnections]],
  )(implicit
      namedLoggingContext: NamedLoggingContext,
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      actorSystem: ActorSystem,
  ): EitherT[FutureUnlessShutdown, String, (SequencerConnectionXPool, SequencerAggregatedInfo)] = {
    implicit val traceContext: TraceContext = namedLoggingContext.traceContext

    def tryNewConfig: EitherT[
      FutureUnlessShutdown,
      String,
      (SequencerConnectionXPool, SequencerAggregatedInfo),
    ] =
      for {
        sequencerConnections <- OptionT(loadConfig).toRight("No sequencer connection config")
        connectionPoolAndInfo <- validateConfig(
          connectionPoolFactory = connectionPoolFactory,
          sequencerConnections = sequencerConnections,
          poolName = "main",
          tracingConfig = tracingConfig,
          logErrorFn =
            error => namedLoggingContext.warn(s"Waiting for valid sequencer connection: $error"),
        )
      } yield connectionPoolAndInfo

    import scala.concurrent.duration.*
    EitherT(
      retry
        .Pause(
          namedLoggingContext.tracedLogger,
          flagCloseable,
          maxRetries = retry.Forever,
          delay = 50.millis,
          operationName = "wait-for-valid-sequencer-connection",
        )
        .unlessShutdown(
          tryNewConfig.value,
          NoExceptionRetryPolicy,
        )
    )
  }

  private def validateConfig(
      connectionPoolFactory: SequencerConnectionXPoolFactory,
      sequencerConnections: SequencerConnections,
      poolName: String,
      tracingConfig: TracingConfig,
      logErrorFn: SequencerConnectionXPoolError => Unit = _ => (),
  )(implicit
      namedLoggingContext: NamedLoggingContext,
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
  ): EitherT[FutureUnlessShutdown, String, (SequencerConnectionXPool, SequencerAggregatedInfo)] = {
    implicit val traceContext: TraceContext = namedLoggingContext.traceContext

    for {
      connectionPool <- EitherT.fromEither[FutureUnlessShutdown](
        connectionPoolFactory
          .createFromOldConfig(
            sequencerConnections,
            expectedPSIdO = None,
            tracingConfig = tracingConfig,
            name = poolName,
          )
          .leftMap { error =>
            logErrorFn(error)
            error.toString
          }
      )
      _ <- connectionPool.start().leftMap { error =>
        logErrorFn(error)
        error.toString
      }
    } yield {
      val psid = connectionPool.physicalSynchronizerIdO.getOrElse(
        ErrorUtil.invalidState(
          "a successfully started connection pool must have the synchronizer ID defined"
        )
      )
      val staticParameters = connectionPool.staticSynchronizerParametersO.getOrElse(
        ErrorUtil.invalidState(
          "a successfully started connection pool must have the static parameters defined"
        )
      )

      // The `sequencerConnections.aliasToConnections` field that we place into `SequencerAggregatedInfo` is different
      // with the connection pool compared to what is produced with the transport mechanism with the `SequencerInfoLoader`:
      // with the connection pool, we provide the original configuration, whereas `SequencerInfoLoader.loadAndAggregateSequencerEndpoints`
      // produces a map that depends on the validation mode (all, active only, etc.).
      // It seems this parameter is however only used later on for building the transports, so it does not matter
      // when using the connection pool (since they are not used).
      val info = SequencerAggregatedInfo(
        psid = psid,
        staticSynchronizerParameters = staticParameters,
        expectedSequencersO = None,
        sequencerConnections = sequencerConnections,
      )

      (connectionPool, info)
    }
  }

}
