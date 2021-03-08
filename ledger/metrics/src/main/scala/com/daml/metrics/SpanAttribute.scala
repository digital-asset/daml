// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.opentelemetry.api.common.AttributeKey

/** Represents a well-known span attribute key.
  */
sealed case class SpanAttribute(key: AttributeKey[String])
object SpanAttribute {
  val CommandId: SpanAttribute = SpanAttribute("daml.command_id")
  val WorkflowId: SpanAttribute = SpanAttribute("daml.workflow_id")
  val TransactionId: SpanAttribute = SpanAttribute("daml.transaction_id")
  val Offset: SpanAttribute = SpanAttribute("daml.offset")
  val OffsetFrom: SpanAttribute = SpanAttribute("daml.offset_from")
  val OffsetTo: SpanAttribute = SpanAttribute("daml.offset_to")

  def apply(key: String): SpanAttribute = SpanAttribute(AttributeKey.stringKey(key))
}
