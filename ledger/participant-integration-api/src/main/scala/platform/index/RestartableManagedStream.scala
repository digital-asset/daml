// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object RestartableManagedStream {
  def withSyncConsumer[Out](
      name: String,
      streamBuilder: () => Source[Out, NotUsed],
      restartSettings: RestartSettings,
      consumeSync: Out => Unit,
      teardown: Int => Unit,
  )(implicit mat: Materializer, loggingContext: LoggingContext) =
    new RestartableManagedStream(
      name = name,
      streamBuilder = streamBuilder,
      restartSettings = restartSettings,
      consumingSink = Sink.foreach(consumeSync),
      teardown = teardown,
    )

  def withAsyncConsumer[Out](
      name: String,
      streamBuilder: () => Source[Out, NotUsed],
      restartSettings: RestartSettings,
      consumeAsync: Out => Future[Unit],
      teardown: Int => Unit,
  )(implicit mat: Materializer, loggingContext: LoggingContext) =
    new RestartableManagedStream(
      name = name,
      streamBuilder = streamBuilder,
      restartSettings = restartSettings,
      consumingSink = Sink.foreachAsync(1)(consumeAsync),
      teardown = teardown,
    )
}

final class RestartableManagedStream[Out](
    val streamBuilder: () => Source[Out, NotUsed],
    val consumingSink: Sink[Out, Future[Done]],
    name: String,
    restartSettings: RestartSettings,
    teardown: Int => Unit = System.exit,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends AbstractRestartableManagedSubscription[Out](name, restartSettings, teardown)

abstract class AbstractRestartableManagedSubscription[Out](
    name: String,
    restartSettings: RestartSettings,
    teardown: Int => Unit,
)(implicit
    mat: Materializer,
    loggingContext: LoggingContext,
) {

  def streamBuilder: () => Source[Out, NotUsed]
  def consumingSink: Sink[Out, Future[Done]]

  private val logger = ContextualizedLogger.get(getClass)

  private val (killSwitch, eventuallyDone) = RestartSource
    .withBackoff(restartSettings)(streamBuilder)
    .viaMat(KillSwitches.single)(Keep.right)
    .toMat(consumingSink)(Keep.both)
    .run()

  eventuallyDone.onComplete {
    // AbruptStageTerminationException is propagated even when streams materializer/actorSystem are terminated normally
    case Success(_) | Failure(_: AbruptStageTerminationException) =>
      logger.info(s"Finished $name subscription")
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
