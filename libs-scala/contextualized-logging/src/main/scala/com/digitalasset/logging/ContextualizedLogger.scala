// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import akka.{Done, NotUsed}
import akka.stream.scaladsl.Flow
import com.daml.grpc.GrpcException
import com.daml.logging.entries.LoggingEntries
import io.grpc.Status
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

object ContextualizedLogger {

  // Caches loggers to prevent them from needlessly wasting memory
  // Replicates the behavior of the underlying Slf4j logger factory
  private[this] val cache = TrieMap.empty[String, ContextualizedLogger]

  // Allows to explicitly pass a logger, should be used for testing only
  private[logging] def createFor(withoutContext: Logger): ContextualizedLogger =
    new ContextualizedLogger(withoutContext)

  // Slf4j handles the caching of the underlying logger itself
  def createFor(name: String): ContextualizedLogger =
    cache.getOrElseUpdate(name, createFor(LoggerFactory.getLogger(name)))

  /** Gets from cache (or creates) a [[ContextualizedLogger]].
    * Automatically strips the `$` at the end of Scala `object`s' name.
    */
  def get(clazz: Class[_]): ContextualizedLogger =
    createFor(clazz.getName.stripSuffix("$"))

}

final class ContextualizedLogger private (val withoutContext: Logger) {

  val trace = new LeveledLogger.Trace(withoutContext)
  val debug = new LeveledLogger.Debug(withoutContext)
  val info = new LeveledLogger.Info(withoutContext)
  val warn = new LeveledLogger.Warn(withoutContext)
  val error = new LeveledLogger.Error(withoutContext)

  private def internalOrUnknown(code: Status.Code): Boolean =
    code == Status.Code.INTERNAL || code == Status.Code.UNKNOWN

  private def logError(t: Throwable)(implicit loggingContext: LoggingContext): Unit =
    error("Unhandled internal error", t)

  def logErrorsOnCall[Out](implicit
      loggingContext: LoggingContext
  ): PartialFunction[Try[Out], Unit] = {
    case Failure(e @ GrpcException(s, _)) =>
      if (internalOrUnknown(s.getCode)) {
        logError(e)
      }
    case Failure(NonFatal(e)) =>
      logError(e)
  }

  def logErrorsOnStream[Out](implicit loggingContext: LoggingContext): Flow[Out, Out, NotUsed] =
    Flow[Out].mapError {
      case e @ GrpcException(s, _) =>
        if (internalOrUnknown(s.getCode)) {
          logError(e)
        }
        e
      case NonFatal(e) =>
        logError(e)
        e
    }

  def debugStream[Out](
      toLoggable: Out => String
  )(implicit loggingContext: LoggingContext): Flow[Out, Out, NotUsed] =
    Flow[Out].map { item =>
      debug(toLoggable(item))
      item
    }

  def enrichedDebugStream[Out](
      msg: String,
      withContext: Out => LoggingEntries,
  )(implicit loggingContext: LoggingContext): Flow[Out, Out, NotUsed] =
    Flow[Out].map { item =>
      LoggingContext.withEnrichedLoggingContextFrom(withContext(item)) { implicit loggingContext =>
        debug(msg)
        item
      }
    }

  def logTermination[Mat](implicit loggingContext: LoggingContext, ec: ExecutionContext): (Mat, Future[Done]) => Mat =
    (mat, done) => {
      done.onComplete {
        case Success(_) => info(s"The stream has been closed")
        case _ => ()
      }
      mat
    }

}
