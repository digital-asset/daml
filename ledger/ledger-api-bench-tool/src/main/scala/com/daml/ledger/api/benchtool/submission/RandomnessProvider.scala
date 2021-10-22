// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

trait RandomnessProvider {
  def randomDouble(): Double // 0.0 <= x <= 1.0
  def randomBytes(n: Int): Array[Byte]
}

object RandomnessProvider {
  object Default extends RandomnessProvider {
    private val r = new scala.util.Random(System.currentTimeMillis())
    override def randomDouble(): Double = r.nextDouble()
    override def randomBytes(n: Int): Array[Byte] = r.nextBytes(n)
  }
}
