// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index.internal

import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[index] object RestartableManagedStream {
  def owner[Out](
      name: String,
      streamBuilder: () => Source[Out, NotUsed],
      sink: Sink[Out, Future[Done]],
      restartSettings: RestartSettings,
      teardown: Int => Unit,
  )(implicit
      mat: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[RestartableManagedStream[Out]] =
    ResourceOwner.forReleasable(() =>
      new RestartableManagedStream(
        name = name,
        sourceBuilder = streamBuilder,
        sink = sink,
        restartSettings = restartSettings,
        teardown = teardown,
      )
    )(_.release())
}

/** Generic lifecycle manager for Akka streams used internally in the Index.
  *
  * @param name The name of the stream.
  * @param sourceBuilder The source builder. On failure, this source is restarted according to the `restartSettings`.
  * @param sink The sink consuming the event stream. A failure in the sink is considered fatal and `teardown` is called.
  * @param restartSettings The restart settings for the stream source.
  * @param teardown The system teardown function.
  */
private[index] final class RestartableManagedStream[Out](
    name: String,
    sourceBuilder: () => Source[Out, NotUsed],
    sink: Sink[Out, Future[Done]],
    restartSettings: RestartSettings,
    teardown: Int => Unit = System.exit,
)(implicit mat: Materializer, loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(getClass)

  private val (killSwitch, eventuallyDone) = RestartSource
    .withBackoff(restartSettings)(sourceBuilder)
    .viaMat(KillSwitches.single)(Keep.right)
    .toMat(sink)(Keep.both)
    .run()

  eventuallyDone.onComplete {
    // AbruptStageTerminationException is propagated even when streams materializer/actorSystem are terminated normally
    case Success(_) | Failure(_: AbruptStageTerminationException) =>
      logger.info(s"Finished $name stream")
    case Failure(ex) =>
      logger.error(
        s"The $name stream encountered a non-recoverable error and will shutdown",
        ex,
      )
      teardown(1)
  }(ExecutionContext.parasitic)

  def release(): Future[Unit] = {
    killSwitch.shutdown()
    eventuallyDone.map(_ => ())(ExecutionContext.parasitic)
  }
}
