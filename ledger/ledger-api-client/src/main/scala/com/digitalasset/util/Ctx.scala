// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.util

import com.daml.tracing.{NoOpTelemetryContext, TelemetryContext}

/** Ctx wraps a value with some contextual information.
  */
final case class Ctx[+Context, +Value](
    context: Context,
    value: Value,
    telemetryContext: TelemetryContext = NoOpTelemetryContext,
) {

  def map[T](transform: Value => T): Ctx[Context, T] =
    Ctx(context, transform(value), telemetryContext)

  def enrich[NewContext](
      enrichingFunction: (Context, Value) => NewContext
  ): Ctx[NewContext, Value] =
    Ctx(enrichingFunction(context, value), value, telemetryContext)

}

object Ctx {

  def unit[T](item: T): Ctx[Unit, T] = Ctx((), item)

}
