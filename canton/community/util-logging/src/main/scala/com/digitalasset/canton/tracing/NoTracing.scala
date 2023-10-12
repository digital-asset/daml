// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

/** Exposes an empty TraceContext so a [[logging.TracedLogger]] can still be used. */
trait NoTracing {
  protected implicit def traceContext: TraceContext = TraceContext.empty
}
