// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.archive.{LanguageMajorVersion => LMV}
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
      lfVersions("0", LMV.V0.supportedMinorVersions) ++
        lfVersions("1", LMV.V1.supportedMinorVersions)
    format(allVersions)
  }

  private def lfVersions(
      majorVersion: String,
      minorVersions: Iterable[String]): Iterable[String] = {
    val nonEmptyMinorVersions = minorVersions.filter(_.nonEmpty)
    if (nonEmptyMinorVersions.isEmpty) Seq(majorVersion)
    else nonEmptyMinorVersions.map(a => s"$majorVersion.$a")
  }

  private def format(as: Iterable[String]): String = as.mkString(", ")
}
