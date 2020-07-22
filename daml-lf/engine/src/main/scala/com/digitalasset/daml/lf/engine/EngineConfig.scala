// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.data.NoCopy
import com.daml.lf.{VersionRange, data}
import com.daml.lf.language.{LanguageVersion => LV}
import com.daml.lf.transaction.{TransactionVersion => TV, TransactionVersions}

// FIXME: https://github.com/digital-asset/daml/issues/5164
// Currently only outputTransactionVersions is used.
// languageVersions and outputTransactionVersions should be plug
final case class EngineConfig private (
    // constrains the version of language accepted by the engine
    languageVersions: VersionRange[LV],
    // constrains the version of output transactions
    inputTransactionVersions: VersionRange[TV],
    // constrains the version of output transactions
    outputTransactionVersions: VersionRange[TV],
) extends NoCopy

object EngineConfig {

  // development configuration, should not be used in PROD.
  // accept all language and transaction versions supported by SDK_1_x plus development versions.
  val Dev: EngineConfig = new EngineConfig(
    languageVersions = VersionRange(
      LV(LV.Major.V1, LV.Minor.Stable("6")),
      LV(LV.Major.V1, LV.Minor.Dev),
    ),
    inputTransactionVersions = VersionRange(
      TV("10"),
      TransactionVersions.acceptedVersions.last
    ),
    outputTransactionVersions = VersionRange(
      TV("10"),
      TransactionVersions.acceptedVersions.last
    )
  )

  // Legacy configuration, to be used by sandbox classic only
  @deprecated("Sandbox_Classic is to be used by sandbox classic only", since = "1.4.0")
  val Sandbox_Classic: EngineConfig = new EngineConfig(
    languageVersions = VersionRange(
      LV(LV.Major.V1, LV.Minor.Stable("1")),
      LV(LV.Major.V1, LV.Minor.Dev),
    ),
    inputTransactionVersions = VersionRange(
      TransactionVersions.acceptedVersions.head,
      TransactionVersions.acceptedVersions.last
    ),
    outputTransactionVersions = VersionRange(
      TV("10"),
      TransactionVersions.acceptedVersions.last
    )
  )

  def build(
      languageVersions: VersionRange[LV],
      inputTransactionVersions: VersionRange[TV],
      outputTransactionVersions: VersionRange[TV],
  ): Either[String, EngineConfig] = {
    val config = new EngineConfig(
      languageVersions = languageVersions intersect Dev.languageVersions,
      inputTransactionVersions = inputTransactionVersions intersect Dev.inputTransactionVersions,
      outputTransactionVersions = outputTransactionVersions intersect Dev.outputTransactionVersions,
    )

    Either.cond(
      config.languageVersions.nonEmpty && config.inputTransactionVersions.nonEmpty && config.outputTransactionVersions.nonEmpty,
      config,
      "invalid engine configuration"
    )
  }

  def assertBuild(
      languageVersions: VersionRange[LV],
      inputTransactionVersions: VersionRange[TV],
      outputTransactionVersions: VersionRange[TV],
  ): EngineConfig =
    data.assertRight(
      build(
        languageVersions: VersionRange[LV],
        inputTransactionVersions: VersionRange[TV],
        outputTransactionVersions: VersionRange[TV],
      )
    )

  // recommended configuration
  val Stable: EngineConfig = assertBuild(
    languageVersions = VersionRange(
      LV(LV.Major.V1, LV.Minor.Stable("6")),
      LV(LV.Major.V1, LV.Minor.Stable("8")),
    ),
    inputTransactionVersions = VersionRange(TV("10"), TV("10")),
    outputTransactionVersions = VersionRange(TV("10"), TV("10"))
  )

}
