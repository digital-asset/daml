// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service.channel

import cats.syntax.either.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.synchronizer.sequencing.service.AuthenticationCheck
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{Status, StatusException}
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

/** Sequencer channel service supporting the creation of bidirectional, GRPC-based sequencer channels.
  */
private[channel] final class GrpcSequencerChannelService(
    authenticationCheck: AuthenticationCheck,
    channelPool: GrpcSequencerChannelPool,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
) extends v30.SequencerChannelServiceGrpc.SequencerChannelService
    with NamedLogging
    with FlagCloseable {

  /** Create a new, uninitialized sequencer channel whose initialization occurs via the first
    * entry of the request stream observer returned by this call. Throws a GRPC StatusException
    * if shutting down.
    *
    * @param responseObserver GRPC response observer for messages to the client
    */
  override def connectToSequencerChannel(
      responseObserver: StreamObserver[v30.ConnectToSequencerChannelResponse]
  ): StreamObserver[v30.ConnectToSequencerChannelRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    tryWithServerCallStreamObserver(responseObserver) { observer =>
      logger.debug("Received channel request - awaiting channel metadata.")
      for {
        _ <- Either.cond(
          !isClosing,
          (),
          Status.UNAVAILABLE.withDescription("Sequencer is being shut down.").asException(),
        )
        authenticatedMemberO = authenticationCheck.lookupCurrentMember()
        authenticationTokenO = IdentityContextHelper.getCurrentStoredAuthenticationToken
        requestObserver <- channelPool
          .createUninitializedChannel(
            observer,
            authenticationTokenO.map(_.expireAt), // close channel when auth token expires
            authenticationCheck.authenticate(_, authenticatedMemberO),
            timeouts,
            loggerFactory,
          )
          .leftMap(Status.UNAVAILABLE.withDescription(_).asException())
      } yield requestObserver
    }
  }

  override def ping(request: v30.PingRequest): Future[v30.PingResponse] =
    Future.successful(v30.PingResponse())

  def disconnectMember(member: Member)(implicit traceContext: TraceContext): Unit =
    channelPool.closeChannels(Some(member))

  def disconnectAllMembers()(implicit traceContext: TraceContext): Unit =
    channelPool.closeChannels()

  /** Ensure response observer provided by GRPC is a ServerCallStreamObserver
    * and that we are not shutting down.
    *
    * @param observer response observer
    * @param handler  handler requiring a ServerCallStreamObserver
    */
  private def tryWithServerCallStreamObserver[Response, Request](
      observer: StreamObserver[Response]
  )(
      handler: ServerCallStreamObserver[Response] => Either[StatusException, StreamObserver[
        Request
      ]]
  )(implicit traceContext: TraceContext): StreamObserver[Request] =
    (observer match {
      case serverCallStreamObserver: ServerCallStreamObserver[Response] =>
        handler(serverCallStreamObserver)
      case _ =>
        Left(
          Status.INTERNAL
            .withDescription("Unexpected non-server stream observer type")
            .asException()
        )
    }).valueOr { statusException =>
      LoggerUtil.logThrowableAtLevel(
        Level.WARN,
        s"Failed to create sequencer channel: ${statusException.getMessage}",
        statusException,
      )
      // Notify client of error and throw instead of returning a dummy StreamObserver
      // as GRPC would otherwise expect us to produce a (dummy) request handler.
      observer.onError(statusException)
      throw statusException
    }

  override def onClosed(): Unit =
    channelPool.close()
}

object GrpcSequencerChannelService {

  def apply(
      authenticationCheck: AuthenticationCheck,
      clock: Clock,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): GrpcSequencerChannelService =
    new GrpcSequencerChannelService(
      authenticationCheck,
      new GrpcSequencerChannelPool(clock, protocolVersion, timeouts, loggerFactory),
      timeouts,
      loggerFactory,
    )
}
