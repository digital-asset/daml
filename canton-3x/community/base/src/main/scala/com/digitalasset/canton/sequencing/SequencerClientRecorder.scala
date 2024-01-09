// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.SequencerClientRecorder.{Extensions, withExtension}
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.MessageRecorder

import java.nio.file.Path

/** Record interactions that the Sequencer client has with its domain.
  * If enabled will record sends to the Sequencer and events received from the Sequencer subscription.
  * Callers must call `start` with a path for recording before recording sequencer interactions.
  */
class SequencerClientRecorder(
    path: Path,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
) extends FlagCloseable
    with NamedLogging {
  private val submissionRecorder = new MessageRecorder(timeouts, loggerFactory)
  private val eventRecorder = new MessageRecorder(timeouts, loggerFactory)

  withNewTraceContext { implicit traceContext =>
    logger.debug(s"Starting recording of sequencer interactions to [$path]")
    path.getParent.toFile.mkdirs()

    submissionRecorder.startRecording(withExtension(path, Extensions.Submissions))
    eventRecorder.startRecording(withExtension(path, Extensions.Events))
  }

  def recordSubmission(submission: SubmissionRequest): Unit =
    submissionRecorder.record(submission)

  def recordEvent(event: OrdinarySerializedEvent): Unit =
    eventRecorder.record(event)

  override protected def onClosed(): Unit = {
    submissionRecorder.close()
    eventRecorder.close()
  }
}

object SequencerClientRecorder {
  def withExtension(path: Path, extension: String): Path =
    path.resolveSibling(path.getFileName.toString + "." + extension)

  def loadSubmissions(path: Path, logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): List[SubmissionRequest] =
    MessageRecorder.load[SubmissionRequest](withExtension(path, Extensions.Submissions), logger)

  def loadEvents(path: Path, logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): List[OrdinarySerializedEvent] =
    MessageRecorder.load[OrdinarySerializedEvent](withExtension(path, Extensions.Events), logger)

  object Extensions {
    val Submissions = "submissions"
    val Events = "events"
  }
}
