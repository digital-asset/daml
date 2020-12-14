// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

package com.daml.metrics

import io.opentelemetry.trace.Span

sealed case class SpanKind(kind: Span.Kind)
object SpanKind {
  val Internal = SpanKind(Span.Kind.INTERNAL)
  val Client = SpanKind(Span.Kind.CLIENT)
  val Server = SpanKind(Span.Kind.SERVER)
  val Producer = SpanKind(Span.Kind.PRODUCER)
  val Consumer = SpanKind(Span.Kind.CONSUMER)
}
