// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.util

import java.lang.Math.floor

import org.apache.pekko.actor.Scheduler
import org.apache.pekko.pattern.after
import com.daml.grpc.GrpcException
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Configuration values for initial service binding retrial
  */
trait IRetryConfig {

  /** @return The interval between retries.
    */
  def intervalMs: Long
  def interval: FiniteDuration = intervalMs.millis

  /** @return The total timeout we allow for the operation to succeed.
    */
  def timeoutMs: Long
  def timeout: FiniteDuration = timeoutMs.millis
}

object RetryHelper extends LazyLogging {

  /** Return '''true''' if you want to re-try a statement that caused the specified exception.
    */
  type RetryStrategy = PartialFunction[Throwable, Boolean]

  /** Always retries if exception is `NonFatal`.
    */
  val always: RetryStrategy = { case NonFatal(_) =>
    true
  }

  val failFastOnPermissionDenied: RetryStrategy = {
    case GrpcException.PERMISSION_DENIED() => false
    case NonFatal(_) => true
  }

  def retry[T](
      retryConfig: Option[(Scheduler, IRetryConfig)]
  )(retryStrategy: RetryStrategy)(f: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    retryConfig match {
      case None =>
        f
      case Some(rc) =>
        implicit val scheduler: Scheduler = rc._1
        retry(Option(rc._2))(retryStrategy)(f)
    }
  }

  def retry[T](retryConfig: Option[IRetryConfig])(
      retryStrategy: RetryStrategy
  )(f: => Future[T])(implicit ec: ExecutionContext, s: Scheduler): Future[T] = {
    retryConfig match {
      case None =>
        f
      case Some(rc) =>
        val maxAttempts = floor(rc.timeout / rc.interval).toInt
        retry(maxAttempts, rc.interval)(retryStrategy)(f)
    }
  }

  def retry[T](maxAttempts: Int, delay: FiniteDuration)(
      retryStrategy: RetryStrategy
  )(f: => Future[T])(implicit ec: ExecutionContext, s: Scheduler): Future[T] = {

    def shouldRetry(n: Int, e: Throwable): Boolean =
      n > 0 && retryStrategy.applyOrElse(e, (_: Throwable) => false)

    val remainingAttempts = maxAttempts - 1 // the next line will trigger a future evaluation

    f.recoverWith {
      case NonFatal(e) if shouldRetry(remainingAttempts, e) =>
        logWarning(remainingAttempts, e)
        after(delay, s)(retry(remainingAttempts, delay)(retryStrategy)(f))
    }
  }

  private def logWarning(remainingAttempts: Int, e: Throwable): Unit = {
    logger.warn(
      s"Retrying after failure. Attempts remaining: $remainingAttempts. Error: ${e.getMessage}"
    )
  }
}
