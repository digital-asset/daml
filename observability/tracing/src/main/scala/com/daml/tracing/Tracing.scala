// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tracing

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.{TextMapGetter, TextMapSetter}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object Tracing {

  def encodeTraceMetadata(context: Context): Map[String, String] = {
    val buffer = mutable.Map[String, String]()
    GlobalOpenTelemetry.getPropagators.getTextMapPropagator
      .inject(context, buffer, TracingMetadataSetter)

    buffer.toMap
  }

  def decodeTraceMetadata(data: java.util.Map[String, String]): Option[Context] =
    if (data == null) {
      None
    } else {
      Some(
        GlobalOpenTelemetry.getPropagators.getTextMapPropagator
          .extract(Context.root, data.asScala.toMap, TracingMetadataGetter)
      )
    }

  /** Helper object used by TextMapPropagator.inject().
    */
  object TracingMetadataSetter extends TextMapSetter[mutable.Map[String, String]] {
    override def set(carrier: mutable.Map[String, String], key: String, value: String): Unit = {
      carrier += ((key, value))
    }
  }

  /** Helper object used by TextMapPropagator.extract().
    */
  object TracingMetadataGetter extends TextMapGetter[Map[String, String]] {
    override def get(carrier: Map[String, String], key: String): String = {
      carrier.get(key).orNull
    }
    override def keys(carrier: Map[String, String]): java.lang.Iterable[String] = {
      carrier.keys.asJava
    }
  }
}
