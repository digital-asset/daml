package com.daml.telemetry

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
