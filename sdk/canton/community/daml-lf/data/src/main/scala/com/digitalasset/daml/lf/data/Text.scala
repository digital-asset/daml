// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

sealed abstract class TextModule {

  type Text <: String

  def fromString(s: String): Either[String, Text]

  final def assertFromString(s: String) = assertRight(fromString(s))

}

private[data] object TextModuleImpl extends TextModule {
  override type Text = String
  override def fromString(s: String): Either[String, Text] = {
    val length = s.length

    @scala.annotation.tailrec
    def loop(i: Int): Either[String, Text] =
      if (i >= length) Right(new Text(s))
      else {
        val ch = s.charAt(i)

        // Check for null character
        if (ch == '\u0000')
          Left("Text contains null character")
        // Check for valid surrogate pairs
        else if (Character.isHighSurrogate(ch)) {
          if (i + 1 >= length || !Character.isLowSurrogate(s.charAt(i + 1))) {
            Left("Text contains unpaired high surrogate")
          } else {
            loop(i + 2) // Skip the low surrogate
          }
        } else if (Character.isLowSurrogate(ch)) {
          Left("Text contains unpaired low surrogate")
        } else {
          loop(i + 1)
        }
      }

    loop(0)
  }
}
