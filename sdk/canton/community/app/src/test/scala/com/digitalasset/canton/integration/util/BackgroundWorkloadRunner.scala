// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  ExecutorServiceExtensions,
  Threading,
}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.*

/** Enables tests to run workload in the background across specified participants that share a
  * synchronizer while executing test code.
  */
trait BackgroundWorkloadRunner {
  this: NamedLogging =>

  protected def withWorkload[T](
      participants: NonEmpty[Seq[ParticipantReference]]
  )(code: => T)(implicit traceContext: TraceContext): T = {
    val stop = new AtomicBoolean(false)
    val scheduler = Threading.singleThreadedExecutor(
      "test-workload-runner",
      noTracingLogger,
    )
    try {
      scheduler.submit(new WorkloadRunner(scheduler, stop, participants))
      code
    } finally {
      logger.debug("Done executing code, asking workload runner to stop submitting")
      stop.set(true)
      // Use resource lifecycle helper to close awaiting idleness before shutting down scheduler
      ExecutorServiceExtensions(scheduler)(logger, DefaultProcessingTimeouts.testing).close()
    }
  }

  private class WorkloadRunner(
      scheduler: ExecutionContextIdlenessExecutorService,
      stop: AtomicBoolean,
      participants: NonEmpty[Seq[ParticipantReference]],
  )(implicit traceContext: TraceContext)
      extends Runnable {
    var participantIndex: Int = 0

    def node(i: Int): ParticipantReference = participants(
      (participantIndex + i) % participants.size
    )

    def payer: ParticipantReference = node(0)

    def owner: ParticipantReference = node(1)

    override def run(): Unit = {
      pay(payer, owner)

      unlessStopped {
        // switch submitter before scheduling again
        participantIndex += 1
        scheduler.submit(this)
      }
    }

    private def pay(payer: ParticipantReference, owner: ParticipantReference): Unit =
      unlessStopped {
        val cid = JavaDecodeUtil
          .decodeAllCreated(Iou.COMPANION)(
            payer.ledger_api.javaapi.commands.submit_flat(
              Seq(payer.id.adminParty),
              new Iou(
                payer.id.adminParty.toProtoPrimitive,
                owner.id.adminParty.toProtoPrimitive,
                new Amount(3.50.toBigDecimal, "CHF"),
                List().asJava,
              ).create.commands.asScala.toSeq,
            )
          )
          .headOption
          .getOrElse(throw new IllegalStateException("unexpected empty result"))
          .id

        unlessStopped {
          owner.ledger_api.javaapi.commands
            .submit_flat(
              Seq(owner.id.adminParty),
              cid.exerciseCall().commands.asScala.toSeq,
            )
            .discard
        }
      }

    private def unlessStopped(code: => Unit): Unit =
      if (!stop.get()) code
      else logger.debug("Workload runner stopped. Not running more code.")
  }

}
