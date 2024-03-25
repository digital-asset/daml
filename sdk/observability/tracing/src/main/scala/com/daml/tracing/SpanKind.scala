// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tracing

import io.opentelemetry.api.trace.{SpanKind => Kind}

sealed case class SpanKind(kind: Kind)

object SpanKind {
  val Internal: SpanKind = SpanKind(Kind.INTERNAL)
  val Client: SpanKind = SpanKind(Kind.CLIENT)
  val Server: SpanKind = SpanKind(Kind.SERVER)
  val Producer: SpanKind = SpanKind(Kind.PRODUCER)
  val Consumer: SpanKind = SpanKind(Kind.CONSUMER)
}
