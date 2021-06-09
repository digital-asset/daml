// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.util

import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}

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

  def fromPair[Context, Value](pair: (Context, Value)) = Ctx(pair._1, pair._2)

  def unit[T](item: T): Ctx[Unit, T] = Ctx((), item)

  def derive[T, U](transform: T => U)(item: T) = Ctx(transform(item), item)

}
