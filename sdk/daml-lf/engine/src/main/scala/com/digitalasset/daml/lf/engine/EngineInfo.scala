// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.language.LanguageVersion

class EngineInfo(config: EngineConfig) {

  import language.{LanguageVersion => LV}

  override def toString: String = show
  def show: String = {

    val allLangVersions = LanguageVersion.allLegacy ++ LanguageVersion.all

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
