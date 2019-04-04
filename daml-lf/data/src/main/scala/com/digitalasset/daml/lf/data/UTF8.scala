// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.nio.charset.StandardCharsets

import scala.collection.mutable

// This object defines functions to emulates UTF8 string
// behavior while still using underlying UTF-16 encoding.
object UTF8 {

  // The DAML-LF strings are supposed to be UTF-8.
  // However standard "exploding" java/scala methods like
  // _.toList split in Character which are not Unicode codepoint.
  def explode(s: String): ImmArray[String] = {
    val len = s.length
    val arr = new mutable.ArraySeq[String](s.codePointCount(0, len))
    var i = 0
    var j = 0
    while (i < len) {
      // if s(i) is a high surrogate the current codepoint uses 2 chars
      val next = if (s(i).isHighSurrogate) i + 2 else i + 1
      arr(j) = s.substring(i, next)
      j += 1
      i = next
    }
    ImmArray.unsafeFromArraySeq(arr)
  }

  // The DAML-LF should sort string according UTF-8 encoding.
  // Java standard string ordering uses UTF-16, which does not match
  // expected one. Note that unlike UTF-16, UTF-8 ordering matches
  // Unicode ordering, so we can order by codepoints.
  //
  // For instance consider the two following unicode code points:
  //
  //    ｡  (Unicode 0x00ff61, UTF-8 [0xef, 0xbd, 0xa1],     , UTF-16 [0xff61])
  //    😂 (Unicode 0x01f602, UTF-8 [0xf0, 0x9f, 0x98, 0x82], UTF-16 [0xd83d, 0xde02])
  //
  // The comparison "｡" < "😂" returns false in java/scala, but it
  // should return true. Note it returns True in Haskell.
  //
  // See https://ssl.icu-project.org/docs/papers/utf16_code_point_order.html
  // for more explanations.

  val ordering: Ordering[String] = new Ordering[String] {
    override def compare(xs: String, ys: String): Int = {
      val lim = xs.length min ys.length
      var i = 0
      while (i < lim) {
        val x = xs(i)
        val y = ys(i)
        if (x != y) {
          // If x is a low surrogate, then the current codepoint starts at the
          // previous char, otherwise the codepoint starts at the current char.
          val j = if (x.isLowSurrogate) i - 1 else i
          return xs.codePointAt(j) - ys.codePointAt(j)
        }
        i += 1
      }
      xs.length - ys.length
    }
  }

  def getBytes(s: String): Array[Byte] =
    s.getBytes(StandardCharsets.UTF_8)

}
