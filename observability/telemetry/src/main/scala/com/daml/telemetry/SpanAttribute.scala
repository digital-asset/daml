// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import io.opentelemetry.api.common.AttributeKey

/** Represents a well-known span attribute key.
  */
sealed case class SpanAttribute(key: AttributeKey[String])

object SpanAttribute {
  val ApplicationId: SpanAttribute = SpanAttribute("daml.application_id")
  val CommandId: SpanAttribute = SpanAttribute("daml.command_id")
  val Offset: SpanAttribute = SpanAttribute("daml.offset")
  val OffsetFrom: SpanAttribute = SpanAttribute("daml.offset_from")
  val OffsetTo: SpanAttribute = SpanAttribute("daml.offset_to")
  val Submitter: SpanAttribute = SpanAttribute("daml.submitter")
  val TransactionId: SpanAttribute = SpanAttribute("daml.transaction_id")
  val WorkflowId: SpanAttribute = SpanAttribute("daml.workflow_id")

  def apply(key: String): SpanAttribute = SpanAttribute(AttributeKey.stringKey(key))
}
