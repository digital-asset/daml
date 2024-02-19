// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, ConcurrentHMap}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.Phase37Synchronizer.*
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  PendingRequestData,
  RequestType,
}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.PendingRequestDataOrReplayData
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success}

/** Synchronizes the request processing of phases 3 and 7.
  * At the end of phase 3, every request must signal that it has been confirmed via the handle
  * returned by [[registerRequest]].
  * At the beginning of phase 7, requests can wait on the completion of phase 3 via [[awaitConfirmed]].
  *
  * Eventually, all requests should either return a None or the corresponding data
  * if it is the first valid request.
  * After this point the request is cleaned from memory, otherwise, the synchronizer becomes a memory leak.
  */
class Phase37Synchronizer(
    override val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
) extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  /** Maps request timestamps to a promise and a future, which is used to chain each request's evaluation (i.e. filter).
    * The future completes with either the pending request data, if it's the first valid call,
    * or None otherwise.
    *
    * The key is the underlying timestamp of the request id.
    * A value of None encodes a request that has timed out.
    */
  private[this] val pendingRequests = ConcurrentHMap.empty[HMapRequestRelation]

  /** Register a request in 'pendingRequests' and returns an handle that can be used to complete the
    * underlying request promise with its corresponding data or None if it's a timeout. It initializes the
    * future, to chain the evaluations, to handle.future.
    *
    * @param requestId The request id (timestamp) of the request to register.
    * @return The promise handle.
    * @throws java.lang.IllegalArgumentException
    * <ul>
    * <li>If the request has already been registered</li>
    * </ul>
    */
  def registerRequest(requestType: RequestType)(
      requestId: RequestId
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): PendingRequestDataHandle[requestType.PendingRequestData] = {
    val ts = CantonTimestampWithRequestType[requestType.type](requestId.unwrap, requestType)
    implicit val evRequest = ts.pendingRequestRelation

    val promise: PromiseUnlessShutdown[Option[
      PendingRequestDataOrReplayData[requestType.PendingRequestData]
    ]] =
      mkPromise[Option[
        PendingRequestDataOrReplayData[requestType.PendingRequestData]
      ]]("phase37sync-register-request-data", futureSupervisor)

    logger.debug(s"Registering a new request $ts")

    blocking(synchronized {
      val requestRelation: RequestRelation[requestType.PendingRequestData] = RequestRelation(
        promise.future
          .map(_.onShutdown(None).orElse {
            blocking(synchronized {
              pendingRequests.remove_(ts)
            })
            None
          })
      )

      pendingRequests
        .putIfAbsent[ts.type, RequestRelation[requestType.PendingRequestData]](ts, requestRelation)
        .foreach(_ =>
          ErrorUtil.invalidState(
            s"Request $requestId has already been registered"
          )
        )
      new PendingRequestDataHandle[requestType.PendingRequestData](promise)
    })
  }

  /** The returned future completes with either the pending request data,
    * if it's the first valid call after the request has been registered and not marked as timed-out,
    * or None otherwise. Please note that for each request only the first
    * awaitConfirmed, where filter == true, completes with the pending request data even if the
    * filters are different.
    *
    * @param requestId The request id (timestamp) of the request to synchronize.
    * @param filter    A function that returns if a request is either valid or not (e.g. contains a valid signature).
    *                  This filter can be different for each call of awaitConfirmed, but only the first valid filter
    *                  will complete with the pending request data.
    */
  @SuppressWarnings(Array("com.digitalasset.canton.SynchronizedFuture"))
  def awaitConfirmed(requestType: RequestType)(
      requestId: RequestId,
      filter: PendingRequestDataOrReplayData[requestType.PendingRequestData] => Future[Boolean] =
        (_: PendingRequestDataOrReplayData[requestType.PendingRequestData]) =>
          Future.successful(true),
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[RequestOutcome[requestType.PendingRequestData]] = {
    val ts = CantonTimestampWithRequestType[requestType.type](requestId.unwrap, requestType)
    implicit val evRequest = ts.pendingRequestRelation

    blocking(synchronized {
      pendingRequests.get[ts.type, RequestRelation[requestType.PendingRequestData]](ts) match {
        case Some(rr @ RequestRelation(fut)) =>
          logger.debug(
            s"Request ${requestId.unwrap}: Request data is waiting to be validated"
          )
          val promise: PromiseUnlessShutdown[RequestOutcome[requestType.PendingRequestData]] =
            mkPromise[RequestOutcome[requestType.PendingRequestData]](
              "phase37sync-pending-request-data",
              futureSupervisor,
            )

          val newFut = fut.transformWith {
            /* either:
                (1) another call to awaitConfirmed has already received and successfully validated the data
                (2) the request was marked as a timeout
             */
            case Success(None) =>
              promise.outcome(RequestOutcome.AlreadyServedOrTimeout)
              Future.successful(None)
            case Success(Some(pData)) =>
              filter(pData).transform {
                case Success(true) =>
                  // we need a synchronized block here to avoid conflicts with the outer replace in awaitConfirmed
                  blocking(synchronized {
                    // the entry is removed when the first awaitConfirmed with a satisfied predicate is there
                    pendingRequests.remove_(ts)
                  })
                  promise.outcome(RequestOutcome.Success(pData))
                  Success(None)
                case Success(false) =>
                  promise.outcome(RequestOutcome.Invalid)
                  Success(Some(pData))
                case Failure(exception) =>
                  promise.tryFailure(exception).discard[Boolean]
                  Failure(exception)
              }
            case Failure(exception) =>
              promise.tryFailure(exception).discard[Boolean]
              Future.failed(exception)
          }
          pendingRequests.replace_[ts.type, RequestRelation[requestType.PendingRequestData]](
            ts,
            rr.copy(pendingRequestDataFuture = newFut),
          )
          promise.futureUS
        case None =>
          logger.debug(
            s"Request ${requestId.unwrap}: Request data was already returned to another caller" +
              s" or has timed out"
          )
          FutureUnlessShutdown.pure(RequestOutcome.AlreadyServedOrTimeout)
      }
    })
  }

  @VisibleForTesting
  private[protocol] def memoryIsCleaned(requestType: RequestType)(requestId: RequestId): Boolean = {
    val ts = CantonTimestampWithRequestType(requestId.unwrap, requestType)
    implicit val evRequest = ts.pendingRequestRelation

    blocking(synchronized {
      pendingRequests.get[ts.type, RequestRelation[RequestType#PendingRequestData]](ts).isEmpty
    })
  }

}

object Phase37Synchronizer {

  private final case class CantonTimestampWithRequestType[A <: RequestType](
      ts: CantonTimestamp,
      requestType: A,
  ) {
    def pendingRequestRelation
        : HMapRequestRelation[this.type, RequestRelation[A#PendingRequestData]] =
      HMapRequestRelation[this.type, RequestRelation[A#PendingRequestData]]
  }

  private trait HMapRequestRelation[K, V]
  private object HMapRequestRelation {
    private object HMapRequestRelationImpl extends HMapRequestRelation[Any, Any]
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def apply[K <: Singleton, V]: HMapRequestRelation[K, V] =
      HMapRequestRelationImpl.asInstanceOf[HMapRequestRelation[K, V]]
  }

  /** Contains a promise to fulfill with the request data and a future to use to chain evaluations.
    *
    * @param pendingRequestDataFuture we use it to chain futures stemming from calls to awaitConfirmed.
    *                                 This is first set by the first call to [[registerRequest]]
    *                                 that will create a chain of futures based on the current handle's future.
    *                                 Subsequently, future calls to awaitConfirmed will 'flatMap'/chain on this future.
    */
  private final case class RequestRelation[+T <: PendingRequestData](
      pendingRequestDataFuture: Future[Option[PendingRequestDataOrReplayData[T]]]
  )

  final class PendingRequestDataHandle[T <: PendingRequestData](
      private val handle: PromiseUnlessShutdown[Option[PendingRequestDataOrReplayData[T]]]
  ) {
    def complete(pendingData: Option[PendingRequestDataOrReplayData[T]]): Unit = {
      handle.outcome(pendingData)
    }
    def failed(exception: Throwable): Unit = {
      handle.tryFailure(exception).discard
    }
    def shutdown(): Unit = {
      handle.shutdown()
    }
  }

  /** Final outcome of a request outputted by awaitConfirmed.
    */
  sealed trait RequestOutcome[+T <: PendingRequestData]

  object RequestOutcome {

    /** Marks a given request as being successful and returns the data.
      */
    final case class Success[T <: PendingRequestData](
        pendingRequestData: PendingRequestDataOrReplayData[T]
    ) extends RequestOutcome[T]

    /** Marks a given request as already served or has timed out.
      */
    final case object AlreadyServedOrTimeout extends RequestOutcome[Nothing]

    /** Marks a given request as invalid.
      */
    final case object Invalid extends RequestOutcome[Nothing]
  }
}
