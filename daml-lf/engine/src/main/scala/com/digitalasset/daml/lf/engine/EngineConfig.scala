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

  // Development configuration, should not be used in PROD.
  val Dev: EngineConfig = new EngineConfig(
    allowedLanguageVersions = VersionRange(
      LV(LV.Major.V1, LV.Minor.Stable("6")),
      LV(LV.Major.V1, LV.Minor.Dev),
    ),
    allowedInputTransactionVersions = VersionRange(
      TV("10"),
      TransactionVersions.acceptedVersions.last
    ),
    allowedOutputTransactionVersions = TransactionVersions.DevOutputVersions
  )

  // Legacy configuration, to be used by sandbox classic only
  @deprecated("Sandbox_Classic is to be used by sandbox classic only", since = "1.4.0")
  val Sandbox_Classic: EngineConfig = new EngineConfig(
    allowedLanguageVersions = VersionRange(
      LV(LV.Major.V1, LV.Minor.Stable("0")),
      LV(LV.Major.V1, LV.Minor.Dev),
    ),
    allowedInputTransactionVersions = VersionRange(
      TransactionVersions.acceptedVersions.head,
      TransactionVersions.acceptedVersions.last
    ),
    allowedOutputTransactionVersions = VersionRange(
      TV("10"),
      TransactionVersions.acceptedVersions.last
    )
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

}
