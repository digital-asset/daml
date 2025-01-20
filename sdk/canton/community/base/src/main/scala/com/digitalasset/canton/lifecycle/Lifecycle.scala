// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import io.grpc.{ManagedChannel, Server}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.slf4j.event.Level

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.util.Try

/** Utilities for working with instances that support our lifecycle pattern.
  */
object Lifecycle extends NoTracing {

  /** How long will we wait for a graceful shutdown to complete
    */
  private val defaultGracefulShutdownTimeout = 3.seconds

  /** How long will we wait for a forced shutdown to complete
    */
  private val defaultForcedShutdownTimeout = 3.seconds

  /** Successful shutdowns taking longer than this will be reported */
  private val slowShutdownThreshold = 1.seconds

  /** Attempts to close all provided closable instances.
    * Instances are closed in the order that they are provided.
    * These instances are expected to synchronously close or throw.
    * If an exception is encountered when closing an instances, we will still attempt to close other closeables
    * and then throw a [[ShutdownFailedException]].
    * Exceptions thrown by `close` will be logged and
    * the names of failed instances are wrapped into the [[ShutdownFailedException]].
    */
  def close(instances: AutoCloseable*)(logger: TracedLogger): Unit = {
    def stopSingle(instance: AutoCloseable): Option[String] = {
      val prettiedInstance = show"${instance.toString.singleQuoted}"
      logger.debug(s"Attempting to close $prettiedInstance...")
      val mbException = Try(instance.close())
        .fold(
          err => Some(err),
          _ => None,
        ) // we're only interested in the error, so basically invert `Try`
      mbException.fold {
        logger.debug(s"Successfully closed $prettiedInstance.")
      } { ex =>
        logger.warn(s"Closing $prettiedInstance failed! Reason:", ex)
      }
      mbException.map(_ => instance.toString)
    }

    // Do not use mapFilter here because mapFilter does not guarantee to work from left to right
    val failedInstances = instances.foldLeft(Seq.empty[String]) { (acc, instance) =>
      acc ++ stopSingle(instance).toList
    }

    NonEmpty.from(failedInstances).foreach { i => throw new ShutdownFailedException(i) }
  }

  def toCloseableOption[A <: AutoCloseable](maybeClosable: Option[A]): AutoCloseable =
    () => maybeClosable.foreach(_.close())

  def toCloseableActorSystem(
      system: ActorSystem,
      logger: TracedLogger,
      timeouts: ProcessingTimeout,
  ): AutoCloseable =
    new AutoCloseable() {
      private val name = system.name
      override def close(): Unit = {
        implicit val loggingContext: ErrorLoggingContext =
          ErrorLoggingContext.fromTracedLogger(logger)(TraceContext.empty)
        timeouts.shutdownProcessing.await_(s"Actor system ($name)", logFailing = Some(Level.WARN))(
          system.terminate()
        )
      }
      override def toString: String = s"Actor system ($name)"
    }

  def toCloseableMaterializer(mat: Materializer, name: String): AutoCloseable =
    new AutoCloseable {
      override def close(): Unit = mat.shutdown()
      override def toString: String = s"Materializer ($name)"
    }

  def toCloseableChannel(
      channel: ManagedChannel,
      logger: TracedLogger,
      name: String,
  ): CloseableChannel =
    new CloseableChannel(channel, logger, name)
  def toCloseableServer(server: Server, logger: TracedLogger, name: String): CloseableServer =
    new CloseableServer(server, logger, name)

  class CloseableChannel(val channel: ManagedChannel, logger: TracedLogger, name: String)
      extends AutoCloseable {
    override def close(): Unit = {
      shutdownResource(
        s"channel: $channel ($name)",
        () => { val _ = channel.shutdown() },
        () => { val _ = channel.shutdownNow() },
        _ => true,
        duration => channel.awaitTermination(duration.toMillis, TimeUnit.MILLISECONDS),
        defaultGracefulShutdownTimeout,
        defaultForcedShutdownTimeout,
        logger,
        verbose = false,
      )
    }

    override def toString: String = s"ManagedChannel ($name)"
  }

  class CloseableServer(val server: Server, logger: TracedLogger, name: String)
      extends AutoCloseable {
    override def close(): Unit = {
      shutdownResource(
        s"server: $server (${name})",
        () => { val _ = server.shutdown() },
        () => { val _ = server.shutdownNow },
        _ => true,
        duration => server.awaitTermination(duration.toMillis, TimeUnit.MILLISECONDS),
        defaultGracefulShutdownTimeout,
        defaultForcedShutdownTimeout,
        logger,
      )
    }
    override def toString: String = s"ManagedServer (${name})"
  }

  def shutdownResource[A](
      name: String,
      shutdown: () => Unit,
      shutdownNow: () => Unit,
      awaitIdleness: FiniteDuration => Boolean,
      awaitTermination: FiniteDuration => Boolean,
      gracefulTimeout: FiniteDuration,
      forcedTimeout: FiniteDuration,
      logger: TracedLogger,
      verbose: Boolean = true,
  ): Unit = {
    val started = Deadline.now

    def forceShutdown(message: String): Unit = {
      logger.warn(s"$name: $message")
      shutdownNow()
      if (!awaitTermination(forcedTimeout)) {
        logger.error(s"$name: failed to terminate within $forcedTimeout when forced.")
      }
    }

    try {
      // give resource a little time to reach an idle state
      if (verbose)
        logger.debug(s"About to close $name within allotted $gracefulTimeout.")
      val idle = awaitIdleness(gracefulTimeout)
      if (!idle) {
        logger.info(s"$name: idleness not reached within allotted $gracefulTimeout.")
      }

      // give resource a little time to politely shutdown
      shutdown()
      val terminated = awaitTermination(gracefulTimeout)

      if (!terminated) {
        forceShutdown(s"shutdown did not complete gracefully in allotted $gracefulTimeout.")
      } else {
        val end = Deadline.now
        if (started + slowShutdownThreshold < end) {
          logger.info(s"$name: Slow shutdown of ${end - started}.")
        } else if (verbose) {
          logger.debug(s"Closed $name after ${end - started}.")
        }
      }
    } catch {
      case _: InterruptedException =>
        forceShutdown("Interrupt during shutdown. Forcing shutdown now.")
        // preserve interrupt status
        Thread.currentThread().interrupt()
    }
  }
}
