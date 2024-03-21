// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tracing

import io.opentelemetry.api.common.Attributes

/** Event object for com.daml.metrics.Spans.
  */
final case class Event(name: String, attributeMap: Map[SpanAttribute, String]) {

  lazy val getAttributes: Attributes = {
    val attributes = Attributes.builder()
    attributeMap.foreach { case (k, v) => if (!v.isEmpty) attributes.put(k.key, v) }
    attributes.build
  }
}
