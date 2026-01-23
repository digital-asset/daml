// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import scala.collection.mutable

class LazinessManager:
  private val lazyValues = mutable.Queue.empty[() => ?]

  def apply[A](compute: => A): () => A =
    lazy val value = compute
    val computation = () => value
    lazyValues.enqueue(computation)
    computation

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
  def force: Unit =
    var count = 0
    while lazyValues.nonEmpty
    do
      count += 1
      if count % 10_000 == 0 then println(s"Forced $count lazy values")
      if count > 1_000_000 then throw Error(s"Looks like there's an infinite loop. Exiting.")
      lazyValues.dequeue()(): Unit // force lazy value

  def withForcedValues[A](code: => A): A =
    val result = code
    force
    result
