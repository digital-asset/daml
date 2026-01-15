// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data

package object keywords:
  private val damlKeywords =
    """case catch class default data do else if implements import interface then try type with"""
      .split("\\s+")
      .to(Set)

  private def kw(str: String) =
    (str.stripMargin.split("\\s+").to(Set) -- damlKeywords).toList.sorted

  val keywords: Seq[String] = kw(
    """abstract assert boolean break byte case catch char class const continue def default do double else enum extends
    |final finally float for goto if implements implicit import instanceof int interface lazy long match native new
    |object override package private protected public return sealed short static strictfp super switch synchronized this
    |throw throws trait transient try type val var void volatile while with yield true false null clone equals
    |hashCode instanceof notifyAll getClass toString finalize"""
  )

  val Keywords: Seq[String] = kw(
    """Int Integer Long Short Byte Unit Promise Class Object Enum Variant"""
  )
