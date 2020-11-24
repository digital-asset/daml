// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.opentelemetry.common.Attributes

/**
  * Wraps [[io.opentelemetry.trace.Event]].
  */
final case class Event(name: String, attributeMap: Map[String, String])
    extends io.opentelemetry.trace.Event {
  override val getName: String = name

  override lazy val getAttributes: Attributes = {
    val attributes = Attributes.newBuilder()
    attributeMap.foreach { case (k, v) => attributes.setAttribute(k, v) }
    attributes.build
  }
}
