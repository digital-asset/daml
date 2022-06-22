// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

trait RandomnessProvider {
  def randomDouble(): Double // 0.0 <= randomDouble() < 1.0
  /** Guarantees that each character will take exactly one byte in UTF-8.
    */
  def randomAsciiString(n: Int): String
  def randomNatural(n: Int): Int // 0 <= randomNatural(n) < n
}

object RandomnessProvider {
  object Default extends Seeded(System.currentTimeMillis())

  def forSeed(seed: Long) = new Seeded(seed = seed)

  class Seeded(seed: Long) extends RandomnessProvider {
    private val r = new scala.util.Random(seed)
    override def randomDouble(): Double = r.nextDouble()
    override def randomNatural(n: Int): Int = r.nextInt(n)
    override def randomAsciiString(n: Int): String = {
      val buffer = new StringBuilder(n)
      0.until(n).foreach { _ =>
        buffer.append(r.nextInt(127).toChar)
      }
      buffer.toString()
    }
  }
}
