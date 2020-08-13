// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.VersionRange
import com.daml.lf.language.{LanguageVersion => LV}
import com.daml.lf.transaction.{TransactionVersions, TransactionVersion => TV}

// FIXME: https://github.com/digital-asset/daml/issues/5164
// Currently only outputTransactionVersions is used.
// languageVersions and outputTransactionVersions should be plug
final case class EngineConfig(
    // constrains the versions of language accepted by the engine
    allowedLanguageVersions: VersionRange[LV],
    // constrains the versions of input transactions
    allowedInputTransactionVersions: VersionRange[TV],
    // constrains the versions of output transactions
    allowedOutputTransactionVersions: VersionRange[TV],
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
