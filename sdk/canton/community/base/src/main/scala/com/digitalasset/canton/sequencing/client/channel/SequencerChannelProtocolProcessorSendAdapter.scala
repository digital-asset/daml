// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext

/** The send adapter allows the SequencerChannelProtocolProcessor to send messages to the channel
  * and wraps the outbound GRPC StreamObserver interface.
  */
private[channel] final class SequencerChannelProtocolProcessorSendAdapter(
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {
  private val requestObserver =
    new SingleUseCell[StreamObserver[v30.ConnectToSequencerChannelRequest]]

  private val ableToSendPayload = new AtomicBoolean(false)

  private[channel] def trySetRequestObserver(
      observer: StreamObserver[v30.ConnectToSequencerChannelRequest]
  ): Unit =
    requestObserver
      .putIfAbsent(observer)
      .foreach(observerAlreadySet =>
        if (observerAlreadySet != observer) {
          throw new IllegalStateException(
            "Request observer already set to a different observer - coding bug"
          )
        }
      )

  private[channel] def notifyAbleToSendPayload(): Unit = ableToSendPayload.set(true)

  /** Sends payloads to channel */
  private[channel] def sendPayload(operation: String, payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    performUnlessClosingEitherUSF(operation) {
      requestObserver.get match {
        case None =>
          val err = s"Attempt to send $operation before request observer set"
          logger.warn(err)
          EitherT.leftT[FutureUnlessShutdown, Unit](err)
        case Some(_) if !ableToSendPayload.get() =>
          val err = s"Attempt to send $operation before channel is ready to send payloads"
          logger.warn(err)
          EitherT.leftT[FutureUnlessShutdown, Unit](err)
        case Some(observer) =>
          logger.debug(s"About to send $operation")
          val request = v30.ConnectToSequencerChannelRequest(
            payload,
            Some(SerializableTraceContext(traceContext).toProtoV30),
          )
          EitherT.pure[FutureUnlessShutdown, String](observer.onNext(request))
      }
    }

  /** Sends channel completion */
  private[channel] def sendCompleted(status: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    performUnlessClosingEitherUSF(s"complete with $status") {
      requestObserver.get.fold {
        val err = s"Attempt to send complete with $status before request observer set"
        logger.warn(err)
        EitherT.leftT[FutureUnlessShutdown, Unit](err)
      } { observer =>
        logger.info(s"Sending channel completion with $status")
        observer.onCompleted()
        EitherTUtil.unitUS
      }
    }

  /** Sends channel error */
  private[channel] def sendError(error: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    performUnlessClosingEitherUSF(s"send error $error") {
      requestObserver.get.fold {
        val errSend = s"Attempt to send error $error before request observer set"
        logger.warn(errSend)
        EitherT.leftT[FutureUnlessShutdown, Unit](errSend)
      } { observer =>
        logger.warn(error)
        observer.onError(new IllegalStateException(error))
        EitherTUtil.unitUS
      }
    }
}
