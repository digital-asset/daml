// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.opentelemetry.trace.TracingContextUtils

/**
  * A wafer-thin abstraction over OpenTelemetry so other packages don't need to
  * use `opentelemetry-api` directly.
  */
object Spans {
  def addEventToCurrentSpan(event: Event): Unit =
    TracingContextUtils.getCurrentSpan.addEvent(event)

  def setCurrentSpanAttribute(attribute: SpanAttribute, value: String): Unit =
    TracingContextUtils.getCurrentSpan.setAttribute(attribute.key, value)

  val ExecutionConsume = "daml.execution.consume"
  val ExecutionEngineRunning = "daml.execution.engine_running"
  val ExecutionTotal = "daml.execution.total"
  val IndexerPrepareStateUpdates = "daml.indexer.prepare_state_updates"
  val IndexerProcessedStateUpdates = "daml.indexer.processed_state_updates"
  // TODO: check if name makes sense
  val LegderConfigProviderInitialConfig = "daml.ledger.config-provider.initial-config"
  val ReadParseUpdates = "daml.kvutils.reader.parse_updates"
  val RunnerUploadDar = "daml.runner.upload-dar"
  // TODO: check if name is correct
  val SubmissionDeduplicate = "daml.commands.submissions.deduplicate"
  val ValidatorValidate = "daml.validator.validate"
}
