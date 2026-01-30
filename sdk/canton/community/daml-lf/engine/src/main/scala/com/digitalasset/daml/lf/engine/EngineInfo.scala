// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.language.LanguageVersion

class EngineInfo(config: EngineConfig) {

  import language.{LanguageVersion => LV}

  override def toString: String = show
  def show: String = {

    val allLangVersions = LanguageVersion.allLegacyLfVersions ++ LanguageVersion.allLfVersions

    val allowedLangVersions =
      allLangVersions.filter(config.allowedLanguageVersions.contains)

    s"Daml-LF Engine supports LF versions: ${formatLangVersions(allowedLangVersions)}"
  }

  private[this] def formatLangVersions(versions: Iterable[LV]) =
    versions.map(_.pretty).mkString(", ")
}
