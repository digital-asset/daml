// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package data

import com.daml.crypto.MessageDigestPrototype
import com.daml.scalautil.Statement.discard
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import scalaz.Order

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

// The Daml-LF strings are supposed to be UTF-8 while standard java strings are UTF16
// Note number of UTF16 operations are not Utf8 equivalent (for instance length, charAt, ordering ...)
// This module provides UTF8 emulation functions.
object Utf8 {

  // The Daml-LF strings are supposed to be UTF-8.
  // However standard "exploding" java/scala methods like
  // _.toList split in Character which are not Unicode codepoint.
  def explode(s: String): ImmArray[String] = {
    val len = s.length
    val arr = ImmArray.newBuilder[String]
    var i = 0
    while (i < len) {
      // if s(i) is a high surrogate the current codepoint uses 2 chars
      val next = if (s(i).isHighSurrogate) i + 2 else i + 1
      discard(arr += s.substring(i, next))
      i = next
    }
    arr.result()
  }

  def getBytes(s: String): Bytes =
    Bytes.fromByteString(ByteString.copyFromUtf8(s))

  def sha256(s: String): String = {
    val digest = MessageDigestPrototype.Sha256.newDigest
    digest.update(getBytes(s).toByteBuffer)
    BaseEncoding.base16().lowerCase().encode(digest.digest())
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

  object ImplicitOrder {
    implicit val `String Utf8 Order`: Order[String] = scalaz.Order fromScalaOrdering Ordering
  }

  def unpack(s: String): ImmArray[Long] =
    s.codePoints().iterator().asScala.map(_.toLong).to(ImmArray)

  // Converts the List of Unicode code points into a String if all code points are legal.
  // Returns the first illegal code point as Left otherwise.
  def pack(codePoints: ImmArray[Long]): Either[Long, String] = {
    val builder = new StringBuilder()
    var illegalCodePoint = Option.empty[Long]
    val iterator = codePoints.iterator
    while (iterator.nonEmpty && illegalCodePoint.isEmpty) {
      val cp = iterator.next()
      if (
        Character.MIN_VALUE <= cp && cp < Character.MIN_SURROGATE ||
        Character.MAX_SURROGATE < cp && cp <= Character.MAX_VALUE
      ) {
        // cp is a legal code point from the Basic Multilingual Plan,
        // then it needs only one UTF16 Char to be encoded.
        builder += cp.toChar
      } else if (Character.MAX_VALUE < cp && cp <= Character.MAX_CODE_POINT) {
        // cp is from one of the Supplementary Plans,
        // then it needs 2 UTF16 Chars to be encoded.
        discard(builder += Character.highSurrogate(cp.toInt))
        discard(builder += Character.lowSurrogate(cp.toInt))
      } else {
        // cp is not a legal Unicode code point
        illegalCodePoint = Some(cp)
      }
    }
    illegalCodePoint.toLeft(builder.result())
  }

}
