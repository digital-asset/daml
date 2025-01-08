// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.service

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader.SequencerAggregatedInfo
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
}
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
  SequencerConnections,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.util.{EitherTUtil, retry}
import io.grpc.{Status, StatusException}
import monocle.Lens
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

  def setup[C](member: Member)(
      registry: CantonMutableHandlerRegistry,
      fetchConfig: () => FutureUnlessShutdown[Option[C]],
      saveConfig: C => FutureUnlessShutdown[Unit],
      sequencerConnectionLens: Lens[C, SequencerConnections],
      requestSigner: RequestSigner,
      transportFactory: SequencerClientTransportFactory,
      sequencerInfoLoader: SequencerInfoLoader,
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      sequencerClient: SequencerClient,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionServiceFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): UpdateSequencerClient = {
    val clientO = new AtomicReference[Option[RichSequencerClient]](None)
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
              newEndpointsInfo <- sequencerInfoLoader
                .loadAndAggregateSequencerEndpoints(
                  synchronizerAlias,
                  Some(synchronizerId),
                  newSequencerConnection,
                  sequencerConnectionValidation,
                )
                .leftMap(_.cause)

              sequencerTransportsMap = transportFactory
                .makeTransport(
                  newEndpointsInfo.sequencerConnections,
                  member,
                  requestSigner,
                )

              sequencerTransports <- EitherT.fromEither[FutureUnlessShutdown](
                SequencerTransports.from(
                  sequencerTransportsMap,
                  newEndpointsInfo.expectedSequencers,
                  newEndpointsInfo.sequencerConnections.sequencerTrustThreshold,
                  newEndpointsInfo.sequencerConnections.submissionRequestAmplification,
                )
              )

              // important to only save the config and change the transport after the `makeTransport` has run and done the handshake
              _ <- EitherT.right(saveConfig(newConfig))
              _ <- EitherT
                .right(
                  clientO
                    .get()
                    .fold {
                      // need to close here
                      sequencerTransportsMap.values.foreach(_.close())
                      FutureUnlessShutdown.unit
                    }(_.changeTransport(sequencerTransports))
                )
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
      futureSupervisor: FutureSupervisor,
      loadConfig: => FutureUnlessShutdown[Option[SequencerConnections]],
  )(implicit
      namedLoggingContext: NamedLoggingContext,
      executionContext: ExecutionContextExecutor,
  ): EitherT[FutureUnlessShutdown, String, SequencerAggregatedInfo] = {
    implicit val traceContext: TraceContext = namedLoggingContext.traceContext
    val promise = new PromiseUnlessShutdown[Either[String, SequencerAggregatedInfo]](
      "wait-for-valid-connection",
      futureSupervisor,
    )
    flagCloseable.runOnShutdown_(promise)

    implicit val closeContext = CloseContext(flagCloseable)
    val alias = SynchronizerAlias.tryCreate("domain")

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

}
