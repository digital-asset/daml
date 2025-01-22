// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core

import com.digitalasset.canton.config
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.actor.typed.ActorSystem
import org.slf4j.event.Level

import scala.concurrent.Future

final class CloseableActorSystem(
    system: ActorSystem[?],
    logger: TracedLogger,
    shutdownProcessing: config.NonNegativeDuration,
) extends AutoCloseable {

  private val name = system.name

  override def close(): Unit =
    shutdownProcessing.await_(
      s"Actor system ($name)",
      logFailing = Some(Level.WARN),
    )(
      Future.successful(system.terminate())
    )(
      ErrorLoggingContext.fromTracedLogger(logger)(TraceContext.empty)
    )

  override def toString: String = s"Actor system ($name)"
}
