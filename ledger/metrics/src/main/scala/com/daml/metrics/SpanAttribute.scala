// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

/**
  * Represents a well-known span attribute key.
  */
sealed case class SpanAttribute(key: String)
object SpanAttribute {
  val CommandId: SpanAttribute = SpanAttribute("daml.command_id")
  val WorkflowId: SpanAttribute = SpanAttribute("daml.workflow_id")
  val TransactionId: SpanAttribute = SpanAttribute("daml.transaction_id")
  val Offset: SpanAttribute = SpanAttribute("daml.offset")
  val OffsetFrom: SpanAttribute = SpanAttribute("daml.offset_from")
  val OffsetTo: SpanAttribute = SpanAttribute("daml.offset_to")
}
