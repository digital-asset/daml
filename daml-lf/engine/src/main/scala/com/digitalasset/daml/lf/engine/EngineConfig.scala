// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.nio.file.Path

import com.daml.lf.language.{LanguageVersion => LV}
import com.daml.lf.transaction.{TransactionVersions, TransactionVersion => TV}

final case class EngineConfig(
    allowedLanguageVersions: VersionRange[LV],
    allowedInputTransactionVersions: VersionRange[TV],
    allowedOutputTransactionVersions: VersionRange[TV],
    stackTraceMode: Boolean = false,
    profileDir: Option[Path] = None,
) {

  private[lf] val allowedInputValueVersions =
    VersionRange(
      TransactionVersions.assignValueVersion(allowedInputTransactionVersions.min),
      TransactionVersions.assignValueVersion(allowedInputTransactionVersions.max),
    )

  private[lf] val allowedOutputValueVersions =
    VersionRange(
      TransactionVersions.assignValueVersion(allowedOutputTransactionVersions.min),
      TransactionVersions.assignValueVersion(allowedOutputTransactionVersions.max),
    )

}

object EngineConfig {

  private[this] def toDev(config: EngineConfig): EngineConfig =
    config.copy(
      allowedLanguageVersions =
        config.allowedLanguageVersions.copy(max = LV(LV.Major.V1, LV.Minor.Dev)),
      allowedInputTransactionVersions = config.allowedInputTransactionVersions.copy(
        max = TransactionVersions.acceptedVersions.last),
      allowedOutputTransactionVersions = config.allowedOutputTransactionVersions.copy(
        max = TransactionVersions.acceptedVersions.last),
    )

  // recommended configuration
  val Stable: EngineConfig = new EngineConfig(
    allowedLanguageVersions = VersionRange(
      LV(LV.Major.V1, LV.Minor.Stable("6")),
      LV(LV.Major.V1, LV.Minor.Stable("8")),
    ),
    allowedInputTransactionVersions = VersionRange(TV("10"), TV("10")),
    allowedOutputTransactionVersions = VersionRange(TV("10"), TV("10"))
  )

  // development configuration, should not be used in PROD.
  // accept all language and transaction versions supported by SDK_1_x plus development versions.
  val Dev: EngineConfig = toDev(Stable)

  // Legacy configuration, to be used by sandbox classic only
  @deprecated("Sandbox_Classic_Stable is to be used by sandbox classic only", since = "1.5.0")
  val Sandbox_Classic_Stable: EngineConfig =
    Stable.copy(
      allowedLanguageVersions =
        Stable.allowedLanguageVersions.copy(min = LV(LV.Major.V1, LV.Minor.Stable("0"))),
      allowedInputTransactionVersions = Stable.allowedInputTransactionVersions.copy(
        min = TransactionVersions.acceptedVersions.head),
    )

  // Legacy configuration, to be used by sandbox classic only
  @deprecated("Sandbox_Classic_Dev is to be used by sandbox classic only", since = "1.5.0")
  val Sandbox_Classic_Dev = toDev(Sandbox_Classic_Stable)

}
