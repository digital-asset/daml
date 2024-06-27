// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.telemetry

import com.digitalasset.canton.metrics.OnDemandMetricsReader
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder

/** Keeps a reference to the open telemetry instance built through autoconfiguration and local configuration.
  * The [[tracerProviderBuilder]] is the same one used to create the [[openTelemetry]] instance. We can use it to
  * piggy back on the already configured tracer provider and add resource attributes.
  */
final case class ConfiguredOpenTelemetry(
    openTelemetry: OpenTelemetrySdk,
    tracerProviderBuilder: SdkTracerProviderBuilder,
    onDemandMetricsReader: OnDemandMetricsReader,
) extends AutoCloseable {

  override def close(): Unit = {
    openTelemetry.getSdkMeterProvider.close()
    openTelemetry.getSdkTracerProvider.close()
  }
}
