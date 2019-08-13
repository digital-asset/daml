// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.language.{LanguageVersion => LV}
import com.digitalasset.daml.lf.transaction.TransactionVersions
import com.digitalasset.daml.lf.value.ValueVersions

object EngineInfo {

  override lazy val toString: String = show

  lazy val show: String =
    s"DAML LF Engine supports LF versions: $formatLfVersions; Transaction versions: $formatTransactionVersions; Value versions: $formatValueVersions"

  private def formatValueVersions: String =
    format(ValueVersions.acceptedVersions.map(_.protoValue))

  private def formatTransactionVersions: String =
    format(TransactionVersions.acceptedVersions.map(_.protoValue))

  private def formatLfVersions: String = {
    val allVersions: Iterable[String] =
      LV.Major.All flatMap (mv => lfVersions(mv.pretty, mv.supportedMinorVersions))
    format(allVersions)
  }

  private def lfVersions(
      majorVersion: String,
      minorVersions: Iterable[LV.Minor]): Iterable[String] =
    minorVersions.map { a =>
      val ap = a.toProtoIdentifier
      s"$majorVersion${if (ap.isEmpty) "" else s".$ap"}"
    }

  private def format(as: Iterable[String]): String = as.mkString(", ")
}
