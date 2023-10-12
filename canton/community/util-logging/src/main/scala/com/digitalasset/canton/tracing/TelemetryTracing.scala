// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.daml.tracing.Telemetry

trait TelemetryTracing {

  protected def telemetry: Telemetry

  protected implicit def traceContext: TraceContext =
    TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())

}
