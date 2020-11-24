// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import akka.Done
import akka.stream.scaladsl.{Keep, Source}
import io.opentelemetry.common.Attributes
import io.opentelemetry.trace.TracingContextUtils

import scala.concurrent.Future

object Spans {
  def attachEventsToCurrentSpan[Out, Mat](
      extractEvents: Out => Iterable[Event],
      source: => Source[Out, Mat]): Source[Out, Mat] = {
    source
      .watchTermination()(Keep.both[Mat, Future[Done]])
      .map(item => {
        val span = TracingContextUtils.getCurrentSpan
        extractEvents(item).foreach(span.addEvent)
        item
      })
      .mapMaterializedValue(_._1)
  }

  def setCurrentSpanAttribute(key: String, value: String): Unit =
    TracingContextUtils.getCurrentSpan.setAttribute(key, value)
}

case class Event(name: String, attributeMap: Map[String, String])
    extends io.opentelemetry.trace.Event {
  override val getName: String = name

  override lazy val getAttributes: Attributes = {
    val attributes = Attributes.newBuilder()
    attributeMap.foreach { case (k, v) => attributes.setAttribute(k, v) }
    attributes.build
  }
}
