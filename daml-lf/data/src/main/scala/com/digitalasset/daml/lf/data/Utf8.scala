// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import scala.annotation.tailrec
import scala.collection.JavaConverters._

// The DAML-LF strings are supposed to be UTF-8 while standard java strings are UTF16
// Note number of UTF16 operations are not Utf8 equivalent (for instance length, charAt, ordering ...)
// This module provides UTF8 emulation functions.
object Utf8 {

  // The DAML-LF strings are supposed to be UTF-8.
  // However standard "exploding" java/scala methods like
  // _.toList split in Character which are not Unicode codepoint.
  def explode(s: String): ImmArray[String] = {
    val len = s.length
    val arr = ImmArray.newBuilder[String]
    var i = 0
    while (i < len) {
      // if s(i) is a high surrogate the current codepoint uses 2 chars
      val next = if (s(i).isHighSurrogate) i + 2 else i + 1
      arr += s.substring(i, next)
      i = next
    }
    arr.result()
  }

  def getBytes(s: String): Array[Byte] =
    s.getBytes(StandardCharsets.UTF_8)

  def sha256(s: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val array = digest.digest(getBytes(s))
    array.map("%02x" format _).mkString
  }

  def implode(ts: ImmArray[String]): String =
    ts.toSeq.mkString

  val Ordering: Ordering[String] = (xs: String, ys: String) => {
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

  def unpack(s: String): ImmArray[Long] =
    ImmArray(s.codePoints().iterator().asScala.map(_.toLong).toIterable)

  @throws[IllegalArgumentException]
  def pack(codePoints: ImmArray[Long]): String = {
    val builder = new StringBuilder()
    for (cp <- codePoints) {
      if (Character.MIN_VALUE <= cp && cp < Character.MIN_SURROGATE ||
        Character.MAX_SURROGATE < cp && cp <= Character.MAX_VALUE) {
        // cp is a legal code point from the Basic Multilingual Plan,
        // then it needs only one UTF16 Char to be encoded.
        builder += cp.toChar
      } else if (Character.MAX_VALUE < cp && cp <= Character.MAX_CODE_POINT) {
        // cp is from one of the Supplementary Plans,
        // then it needs 2 UTF16 Char to be encoded.
        builder += Character.highSurrogate(cp.toInt)
        builder += Character.lowSurrogate(cp.toInt)
      } else {
        throw new IllegalArgumentException(s"invalid code point 0x${cp.toHexString}.")
      }
    }
    builder.result()
  }

}
