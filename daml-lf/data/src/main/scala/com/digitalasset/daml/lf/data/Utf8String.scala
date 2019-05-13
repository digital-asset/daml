// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import scalaz.Equal
import scalaz.std.string._

import scala.annotation.tailrec

// The DAML-LF strings are supposed to be UTF-8 while standard java strings are UTF16
// We box standard UTF16 java strings to prevent non intentional usages of UTF16 operations
// (for instance length, charAt, ordering ...) and provide UTF8 emulation methods.

// Use `javaString` wisely. As a rule on the thumb, you should use `javaString` only for logging,
// testing, or sending to external libraries (as for instance protobuf/json builders).
case class Utf8String(javaString: String) extends Ordered[Utf8String] {

  // The DAML-LF strings are supposed to be UTF-8.
  // However standard "exploding" java/scala methods like
  // _.toList split in Character which are not Unicode codepoint.
  def explode: ImmArray[Utf8String] = {
    val len = javaString.length
    val arr = ImmArray.newBuilder[Utf8String]
    arr.sizeHint(javaString.codePointCount(0, len))
    var i = 0
    var j = 0
    while (i < len) {
      // if s(i) is a high surrogate the current codepoint uses 2 chars
      val next = if (javaString(i).isHighSurrogate) i + 2 else i + 1
      arr += Utf8String(javaString.substring(i, next))
      j += 1
      i = next
    }
    arr.result()
  }

  def getBytes: Array[Byte] =
    javaString.getBytes(StandardCharsets.UTF_8)

  def +(other: Utf8String): Utf8String =
    Utf8String(javaString + other.javaString)

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
  def compare(that: Utf8String): Int = {
    val xs = this.javaString
    val ys = that.javaString
    val lim = xs.length min ys.length

    @tailrec
    def lp(i: Int): Int =
      if (i < lim) {
        val x = xs(i)
        val y = ys(i)
        if (x != y) {
          // If x is a low surrogate, then the current codepoint starts at the
          // previous char, otherwise the codepoint starts at the current char.
          val j = if (x.isLowSurrogate) i - 1 else i
          xs.codePointAt(j) - ys.codePointAt(j)
        } else lp(i + 1)
      } else xs.length - ys.length

    lp(0)
  }

  def sha256: Utf8String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val array = digest.digest(getBytes)
    Utf8String(array.map("%02x" format _).mkString)
  }

}

object Utf8String {

  def implode(ts: ImmArray[Utf8String]): Utf8String =
    Utf8String(ts.map(_.javaString).toSeq.mkString)

  implicit def ut8StringEqualInstance: Equal[Utf8String] = Equal.equalBy(_.javaString)

}
