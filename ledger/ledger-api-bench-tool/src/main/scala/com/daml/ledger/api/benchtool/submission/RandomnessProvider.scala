// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

trait RandomnessProvider {
  def randomDouble(): Double // 0.0 <= randomDouble() < 1.0
  def randomString(n: Int): String
  def randomNatural(n: Int): Int // 0 <= randomNatural(n) < n
}

object RandomnessProvider {
  object Default extends RandomnessProvider {
    private val r = new scala.util.Random(System.currentTimeMillis())
    override def randomDouble(): Double = r.nextDouble()
    override def randomNatural(n: Int): Int = r.nextInt(n)
    override def randomString(n: Int): String = {
      val buffer = new StringBuilder(n)
      0.until(n).foreach{ _ =>
        buffer.append(r.nextInt(127).toChar)
      }
      buffer.toString()
    }
  }
}
