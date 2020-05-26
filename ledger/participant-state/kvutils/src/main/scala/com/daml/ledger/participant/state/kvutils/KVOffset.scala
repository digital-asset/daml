// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.v1.Offset

object KVOffset {
  private[kvutils] val highestStart = 0
  private[kvutils] val middleStart = 8
  private[kvutils] val lowestStart = 12
  private[kvutils] val end = 16

  private val maxValuePlusOne = BigInt(1) << (end * 8)

  def onlyKeepHighestIndex(offset: Offset): Offset = {
    val highest = highestIndex(offset)
    fromLong(highest)
  }

  def setMiddleIndex(offset: Offset, middle: Int): Offset = {
    val highest = highestIndex(offset)
    val lowest = lowestIndex(offset)
    fromLong(highest.toLong, middle, lowest.toInt)
  }

  def setLowestIndex(offset: Offset, lowest: Int): Offset = {
    val highest = highestIndex(offset)
    val middle = middleIndex(offset)
    fromLong(highest.toLong, middle.toInt, lowest)
  }

  def fromLong(first: Long, second: Int = 0, third: Int = 0): Offset = {
    val highest = BigInt(first) << ((end - middleStart) * 8)
    val middle = BigInt(second) << ((end - lowestStart) * 8)
    val lowest = BigInt(third)
    val bytes = (maxValuePlusOne | highest | middle | lowest).toByteArray.drop(1) // this retains leading zeros
    Offset.fromByteArray(bytes)
  }

  def highestIndex(offset: Offset): Long =
    BigInt(offset.toByteArray.slice(highestStart, middleStart)).toLong
  def middleIndex(offset: Offset): Long =
    BigInt(offset.toByteArray.slice(middleStart, lowestStart)).toLong
  def lowestIndex(offset: Offset): Long = BigInt(offset.toByteArray.slice(lowestStart, end)).toLong

}
