// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

class EngineInfo(config: EngineConfig) {

  import language.{LanguageVersion => LV}

  override def toString: String = show
  def show: String = pretty.mkString(System.lineSeparator())

  lazy val pretty: Iterable[String] = {

    val allLangVersions =
      for {
        major <- LV.Major.All
        minor <- major.supportedMinorVersions
      } yield LV(major, minor)

    val allTransactionVersions =
      transaction.TransactionVersions.acceptedVersions

    val allValueVersions =
      value.ValueVersions.acceptedVersions

    val allOutputTransactionVersions =
      allTransactionVersions.filter(transaction.TransactionVersions.DevOutputVersions.contains)

    val allOutputValueVersions =
      allOutputTransactionVersions.map(transaction.TransactionVersions.assignValueVersion)

    val allowedLangVersions =
      allLangVersions.filter(config.allowedLanguageVersions.contains)

    val allowedInputTransactionVersions =
      allTransactionVersions.filter(config.allowedInputTransactionVersions.contains)

    val allowedInputValueVersions =
      allValueVersions.filter(config.allowedInputValueVersions.contains)

    val allowedOutputTransactionVersions =
      allTransactionVersions.filter(config.allowedOutputTransactionVersions.contains)

    val allowedOutputValueVersions =
      allValueVersions.filter(config.allowedOutputValueVersions.contains)

    List(
      List(
        formatLangVersions(allLangVersions),
        formatTxVersions("input", allTransactionVersions),
        formatValVersions("input", allValueVersions),
        formatTxVersions("output", allOutputTransactionVersions),
        formatValVersions("output", allOutputValueVersions)
      ).mkString("DAML LF Engine supports ", "; ", "."),
      List(
        formatLangVersions(allowedLangVersions),
        formatTxVersions("input", allowedInputTransactionVersions),
        formatValVersions("input", allowedInputValueVersions),
        formatTxVersions("output", allowedOutputTransactionVersions),
        formatValVersions("output", allowedOutputValueVersions)
      ).mkString("DAML LF Engine config allows ", "; ", ".")
    )
  }

  private[this] def formatLangVersions(versions: Iterable[LV]) = {
    val prettyVersions = versions.map {
      case LV(major, minor) =>
        val ap = minor.toProtoIdentifier
        s"${major.pretty}${if (ap.isEmpty) "" else s".$ap"}"
    }
    s"LF versions: ${prettyVersions.mkString(", ")}"
  }

  private[this] def formatTxVersions(
      prefix: String,
      versions: List[transaction.TransactionVersion],
  ) =
    s"$prefix transaction versions: ${versions.map(_.protoValue).mkString(", ")}"

  private[this] def formatValVersions(prefix: String, versions: List[value.ValueVersion]) =
    s"$prefix value versions: ${versions.map(_.protoValue).mkString(", ")}"

}
