// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.CircuitBreakerConfig
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.apache.pekko.actor.Scheduler
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

/** Circuit breaker used for stopping accepting requests when the sequencer is overloaded and
  * falling behind on processing blocks. It works by keeping track of the lastTs on each block
  * process and computing a delay between that timestamp and current time. If the delay is larger
  * than allowedBlockDelay, a failure is registered.
  *
  * Following the circuit breaker logic described in
  * https://doc.akka.io/libraries/akka-core/current/common/circuitbreaker.html, after maxFailures
  * consecutive failures, requests will no longer be accepted (state goes from closed to open).
  *
  * After a resetTimeout amount of time has passed in the open state, the circuit goes to half-open
  * state, where requests are accepted, and if the next block delay registered is in the accepted
  * interval, the circuit breaker closes again and requests are accepted, otherwise, it goes back to
  * being open (and requests not accepted). In this case, the resetTimeout is multiplied by the
  * exponentialBackoffFactor to compute the next timeout (up until at most maxResetTimeout).
  */
class BlockSequencerCircuitBreaker(
    allowedBlockDelay: FiniteDuration,
    maxFailures: Int,
    resetTimeout: FiniteDuration,
    exponentialBackoffFactor: Double,
    maxResetTimeout: FiniteDuration,
    clock: Clock,
    metrics: SequencerMetrics,
    scheduler: Scheduler,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val enabled: AtomicBoolean = new AtomicBoolean(true)
  private val previousTimestamp: AtomicReference[CantonTimestamp] =
    new AtomicReference(CantonTimestamp.MinValue)

  def registerLastBlockTimestamp(lastTs: Traced[CantonTimestamp]): Unit = {
    val timestamp = lastTs.value
    val blockDelay = clock.now - timestamp
    metrics.block.delay.updateValue(blockDelay.toMillis)

    // Ignore timestamps that are equal to the previous one. That happens when the block is empty.
    // We do that in order to avoid that a series of empty blocks cause the circuit breaker to think that
    // it is falling behind (because the block delay would continue to grow with each empty block).
    if (enabled.get() && timestamp != previousTimestamp.get()) {
      if (blockDelay.compareTo(allowedBlockDelay.toJava) > 0)
        pekkoCircuitBreaker.fail()
      else
        pekkoCircuitBreaker.succeed()
    }

    previousTimestamp.set(timestamp)
  }

  def shouldRejectRequests: Boolean = enabled.get() && pekkoCircuitBreaker.isOpen

  def enable(): Unit = enabled.set(true)
  def disable(): Unit = enabled.set(false)

  private val pekkoCircuitBreaker = {
    import org.apache.pekko.pattern.CircuitBreaker
    new CircuitBreaker(
      scheduler,
      maxFailures = maxFailures,
      resetTimeout = resetTimeout,
      exponentialBackoffFactor = exponentialBackoffFactor,
      maxResetTimeout = maxResetTimeout,
      // callTimeout is not used, because we are calling fail() explicitly instead of withCircuitBreaker()
      callTimeout = 0.seconds,
    ).onOpen {
      logger.debug(
        s"Sequencer not accepting requests momentarily, after $maxFailures consecutive blocks behind more than $allowedBlockDelay"
      )(TraceContext.empty)
    }.onHalfOpen {
      logger.debug(
        s"Sequencer temporarily taking requests while assessing situation"
      )(TraceContext.empty)
    }.onClose {
      logger.debug(
        s"Sequencer now accepting requests again, after seeing a block with delay below $allowedBlockDelay"
      )(TraceContext.empty)
    }
  }
}

object BlockSequencerCircuitBreaker {
  def apply(
      config: CircuitBreakerConfig,
      clock: Clock,
      metrics: SequencerMetrics,
      materializer: Materializer,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) = new BlockSequencerCircuitBreaker(
    allowedBlockDelay = config.allowedBlockDelay.underlying,
    maxFailures = config.maxFailures,
    resetTimeout = config.resetTimeout.underlying,
    exponentialBackoffFactor = config.exponentialBackoffFactor,
    maxResetTimeout = config.maxResetTimeout.underlying,
    clock,
    metrics,
    materializer.system.scheduler,
    loggerFactory,
  )
}
