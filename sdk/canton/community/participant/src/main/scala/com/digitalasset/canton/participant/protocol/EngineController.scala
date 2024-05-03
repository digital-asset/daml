// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference

/** Simple class to embody the connection between a component that wants to abort an engine computation and the engine itself.
  * An instance of this class is created by the protocol processor for each request and passed along during processing.
  * When a condition determines that the computation should be aborted, the component calls [[abort]].
  * The instance is also passed to the engine when performing a computation. During the execution of a `ResultInterruption`,
  * the engine will call [[abortStatus]] to determine whether it should abort or continue.
  *
  * @param participantId the participant processing the associated request
  * @param requestId the associated request
  * @param testHookFor hooks meant to be used in tests to perform actions during engine processing, such as slowing it down;
  *                    see [[com.digitalasset.canton.config.TestingConfigInternal]] for a more detailed explanation
  */
final case class EngineController(
    participantId: ParticipantId,
    requestId: RequestId,
    protected val loggerFactory: NamedLoggerFactory,
    testHookFor: String => () => Unit = _ => () => (),
) extends NamedLogging {
  import EngineController.*

  private val abortStatusRef = new AtomicReference[Option[String]](None)

  /** Ask the engine computation for this request to be aborted for the given reason. */
  def abort(reason: String)(implicit traceContext: TraceContext): Unit = {
    logger.info(s"Requesting to abort engine computation for $requestId: reason = $reason")

    abortStatusRef
      .getAndUpdate {
        case None => Some(reason)
        case oldReason => oldReason
      }
      .foreach(oldReason =>
        logger.info(
          s"Aborting the engine computation of $requestId has already been requested for reason \"$oldReason\"" +
            " -- ignoring new request"
        )
      )
  }

  /** Return the engine abort status for this request. */
  def abortStatus: EngineAbortStatus = {
    testHookFor(participantId.uid.identifier.str).apply()

    EngineAbortStatus(abortStatusRef.get)
  }
}

object EngineController {
  final case class EngineAbortStatus(reasonO: Option[String]) extends PrettyPrinting {
    def isAborted: Boolean = reasonO.nonEmpty

    override def pretty: Pretty[EngineAbortStatus] = prettyOfClass(
      paramIfTrue("not aborted", _.reasonO.isEmpty),
      paramIfDefined("aborted with reason", _.reasonO.map(_.unquoted)),
    )
  }

  object EngineAbortStatus {
    def aborted(reason: String): EngineAbortStatus = EngineAbortStatus(Some(reason))

    def notAborted: EngineAbortStatus = EngineAbortStatus(None)
  }

  type GetEngineAbortStatus = () => EngineAbortStatus
}
