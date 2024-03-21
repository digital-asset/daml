// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

class EngineInfo(config: EngineConfig) {

  import language.{LanguageVersion => LV}

  override def toString: String = show
  def show: String = {

    val allLangVersions =
      for {
        major <- LV.Major.All
        minor <- major.supportedMinorVersions
      } yield LV(major, minor)

    val allowedLangVersions =
      allLangVersions.filter(config.allowedLanguageVersions.contains)

    s"Daml-LF Engine supports LF versions: ${formatLangVersions(allowedLangVersions)}"
  }

  private[this] def formatLangVersions(versions: Iterable[LV]) =
    versions
      .map { case LV(major, minor) =>
        val ap = minor.toProtoIdentifier
        s"${major.pretty}${if (ap.isEmpty) "" else s".$ap"}"
      }
      .mkString(", ")

}
