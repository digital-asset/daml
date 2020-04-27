// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.util

/**
  * Ctx wraps a value with some contextual information.
  */
final case class Ctx[+Context, +Value](context: Context, value: Value) {

  def map[T](transform: Value => T): Ctx[Context, T] = Ctx(context, transform(value))

  def enrich[NewContext](enrichingFunction: (Context, Value) => NewContext) =
    Ctx(enrichingFunction(context, value), value)

}

object Ctx {

  def fromPair[Context, Value](pair: (Context, Value)) = Ctx(pair._1, pair._2)

  def unit[T](item: T): Ctx[Unit, T] = Ctx((), item)

  def derive[T, U](transform: T => U)(item: T) = Ctx(transform(item), item)

}
