// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.ledger.error.groups.AuthorizationChecksErrors
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.localstore.api.UserManagementStore
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException
import io.grpc.stub.ServerCallStreamObserver
import org.apache.pekko.actor.Scheduler

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, blocking}

private[auth] final class OngoingAuthorizationObserver[A](
    observer: ServerCallStreamObserver[A],
    originalClaims: ClaimSet.Claims,
    nowF: () => Instant,
    userRightsCheckerO: Option[UserRightsChangeAsyncChecker],
    userRightsCheckIntervalInSeconds: Int,
    lastUserRightsCheckTime: AtomicReference[Instant],
    jwtTimestampLeeway: Option[JwtTimestampLeeway],
    tokenExpiryGracePeriodForStreams: Option[Duration],
    val loggerFactory: NamedLoggerFactory,
)(implicit traceContext: TraceContext)
    extends ServerCallStreamObserver[A]
    with NamedLogging {

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace(
    loggerFactory
  )
  private val errorLogger = ErrorLoggingContext(logger, loggerFactory.properties, traceContext)

  // Guards against propagating calls to delegate observer after either
  // [[onComplete]] or [[onError]] has already been called once.
  // We need this because [[onError]] can be invoked two concurrent sources:
  // 1) scheduled user rights state change task (see [[cancellableO]]),
  // 2) upstream component that is translating upstream Pekko stream into [[onNext]] and other signals.
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var afterCompletionOrError = false

  // TODO(i15769) as soon as ServerCallStreamObserver.setOnCloseHandler is not experimental anymore, it would be convenient
  // to add respective proxy logic here, and support for this in ServerSubscriber, and drop the cancel handler capture
  // workaround from here
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var onCancelHandler: Runnable = () =>
    logger.error(
      "Invalid state: OnCancelHandler was never set. Downstream cancellation cannot be done." +
        " This can result in detached/rogue server side stream processing, and a resulting memory leak!"
    )

  private val cancelUserRightsChecksO: Option[() => Unit] =
    userRightsCheckerO.map(
      _.schedule { () =>
        if (!isCancelled) {
          // Downstream cancellation could race with emittion of errors, therefore we only emit error if the stream is
          // not cancelled.
          abortGRPCStreamAndCancelUpstream(staleStreamAuthError)
        }
      }
    )

  override def isCancelled: Boolean = blocking(synchronized(observer.isCancelled))

  override def setOnCancelHandler(runnable: Runnable): Unit = blocking(
    synchronized {
      val newCancelHandler: Runnable = { () =>
        cancelUserRightsChecksO.foreach(_.apply())
        runnable.run()
      }
      observer.setOnCancelHandler(newCancelHandler)
      onCancelHandler = newCancelHandler
    }
  )

  override def setCompression(s: String): Unit = blocking(synchronized(observer.setCompression(s)))

  override def isReady: Boolean = blocking(synchronized(observer.isReady))

  override def setOnReadyHandler(runnable: Runnable): Unit = blocking(
    synchronized(
      observer.setOnReadyHandler(runnable)
    )
  )

  override def disableAutoInboundFlowControl(): Unit = blocking(
    synchronized(
      observer.disableAutoInboundFlowControl()
    )
  )

  override def request(i: Int): Unit = blocking(synchronized(observer.request(i)))

  override def setMessageCompression(b: Boolean): Unit = blocking(
    synchronized(
      observer.setMessageCompression(b)
    )
  )

  override def onNext(v: A): Unit = onlyBeforeCompletionOrError {
    val now = nowF()
    (for {
      _ <- checkClaimsExpiry(now)
      _ <- checkUserRightsRefreshTimeout(now)
    } yield ()) match {
      case Right(_) => observer.onNext(v)
      case Left(e) => abortGRPCStreamAndCancelUpstream(e)
    }
  }

  override def onError(throwable: Throwable): Unit = onlyBeforeCompletionOrError {
    afterCompletionOrError = true
    cancelUserRightsChecksO.foreach(_.apply())
    observer.onError(throwable)
  }

  override def onCompleted(): Unit = onlyBeforeCompletionOrError {
    afterCompletionOrError = true
    cancelUserRightsChecksO.foreach(_.apply())
    observer.onCompleted()
  }

  private def onlyBeforeCompletionOrError(body: => Unit): Unit =
    blocking(
      synchronized(
        if (!afterCompletionOrError) {
          body
        }
      )
    )

  private def checkUserRightsRefreshTimeout(now: Instant): Either[StatusRuntimeException, Unit] = {
    // Safety switch to abort the stream if the user-rights-state-check task
    // fails to refresh within 2*[[userRightsCheckIntervalInSeconds]] seconds.
    // In normal conditions we expected the refresh delay to be about [[userRightsCheckIntervalInSeconds]] seconds.
    if (
      originalClaims.resolvedFromUser &&
      lastUserRightsCheckTime.get.isBefore(
        now.minusSeconds(2 * userRightsCheckIntervalInSeconds.toLong)
      )
    ) {
      Left(staleStreamAuthError)
    } else Right(())
  }

  private def checkClaimsExpiry(now: Instant): Either[StatusRuntimeException, Unit] =
    originalClaims
      .notExpired(now, jwtTimestampLeeway, tokenExpiryGracePeriodForStreams)
      .left
      .map(authorizationError =>
        AuthorizationChecksErrors.PermissionDenied
          .Reject(authorizationError.reason)(errorLogger)
          .asGrpcError
      )

  private def staleStreamAuthError: StatusRuntimeException =
    // Terminate the stream, so that clients will restart their streams
    // and claims will be rechecked precisely.
    AuthorizationChecksErrors.StaleUserManagementBasedStreamClaims
      .Reject()(errorLogger)
      .asGrpcError

  private def abortGRPCStreamAndCancelUpstream(error: Throwable): Unit = blocking(synchronized {
    onError(error)
    onCancelHandler.run()
  })
}

private[auth] object OngoingAuthorizationObserver {

  /** @param userRightsCheckIntervalInSeconds - determines the interval at which to check whether user rights state has changed.
    *                                          Also, double of this value serves as timeout value for subsequent user rights state checks.
    */
  def apply[A](
      observer: ServerCallStreamObserver[A],
      originalClaims: ClaimSet.Claims,
      nowF: () => Instant,
      userManagementStore: UserManagementStore,
      userRightsCheckIntervalInSeconds: Int,
      pekkoScheduler: Scheduler,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
      tokenExpiryGracePeriodForStreams: Option[Duration] = None,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): ServerCallStreamObserver[A] = {

    val lastUserRightsCheckTime = new AtomicReference(nowF())
    val userRightsCheckerO = if (originalClaims.resolvedFromUser) {
      val checker = new UserRightsChangeAsyncChecker(
        lastUserRightsCheckTime = lastUserRightsCheckTime,
        originalClaims = originalClaims,
        nowF: () => Instant,
        userManagementStore: UserManagementStore,
        userRightsCheckIntervalInSeconds: Int,
        pekkoScheduler: Scheduler,
      )
      Some(checker)
    } else {
      None
    }
    new OngoingAuthorizationObserver(
      observer = observer,
      originalClaims = originalClaims,
      nowF = nowF,
      userRightsCheckerO = userRightsCheckerO,
      userRightsCheckIntervalInSeconds = userRightsCheckIntervalInSeconds,
      lastUserRightsCheckTime = lastUserRightsCheckTime,
      jwtTimestampLeeway = jwtTimestampLeeway,
      tokenExpiryGracePeriodForStreams = tokenExpiryGracePeriodForStreams,
      loggerFactory = loggerFactory,
    )
  }

}
