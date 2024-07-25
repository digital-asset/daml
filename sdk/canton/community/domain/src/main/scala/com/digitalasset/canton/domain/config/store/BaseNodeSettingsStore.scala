// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config.store

import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Base trait for individual node configuration stores
  *
  * Used by DomainManager, Sequencer and Mediator nodes.
  */
trait BaseNodeSettingsStore[T] extends AutoCloseable {
  this: NamedLogging =>
  // TODO(#11052) extend this to ParticipantSettings and move node_id table into this class
  //          also, replace the fetchConfiguration and saveConfiguration with initConfiguration
  //          and update configuration. We might also want to cache the configuration directly in memory
  //          (as we do it for participant settings).
  //          also, the update configuration should be atomic. right now, we do fetch / save, which is racy
  //          also, update this to mediator and sequencer. right now, we only do this for domain manager and domain nodes
  def fetchSettings(implicit traceContext: TraceContext): Future[Option[T]]

  def saveSettings(settings: T)(implicit traceContext: TraceContext): Future[Unit]

  // TODO(#15153) remove once we can assume that static domain parameters are persisted
  protected def fixPreviousSettings(resetToConfig: Boolean, timeout: NonNegativeDuration)(
      update: Option[T] => Future[Unit]
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Unit = {

    val result: Future[Unit] = fetchSettings.flatMap {
      // if a value is stored, don't do anything
      case Some(_) if !resetToConfig =>
        Future.unit
      case Some(value) =>
        noTracingLogger.warn(
          "Resetting static domain parameters to the ones defined in the config! Please disable this again."
        )
        update(Some(value))
      case None => update(None)
    }

    // wait until setting of static domain parameters completed
    timeout.await("Setting static domain parameters")(result)
  }

}

object BaseNodeSettingsStore {
  def factory[T](
      storage: Storage,
      dbFactory: DbStorage => BaseNodeSettingsStore[T],
      loggerFactory: NamedLoggerFactory,
  ): BaseNodeSettingsStore[T] =
    storage match {
      case _: MemoryStorage => new InMemoryBaseNodeConfigStore[T](loggerFactory)
      case storage: DbStorage => dbFactory(storage)
    }

}

class InMemoryBaseNodeConfigStore[T](val loggerFactory: NamedLoggerFactory)
    extends BaseNodeSettingsStore[T]
    with NamedLogging {

  private val currentSettings = new AtomicReference[Option[T]](None)

  override def fetchSettings(implicit
      traceContext: TraceContext
  ): Future[Option[T]] =
    Future.successful(currentSettings.get())

  override def saveSettings(settings: T)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    currentSettings.set(Some(settings))
    Future.unit
  }

  override def close(): Unit = ()

}
