// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.nio.file.Path

import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.{TransactionVersions, TransactionVersion}
import com.daml.lf.value.ValueVersion

/**
  * The Engine configurations describes the versions of language and
  * transaction the engine is allowed to read and write together with
  * engine debugging feature.
  *
  * <p>
  *
  * @param allowedLanguageVersions The range of language versions the
  *     engine is allowed to load.  The engine will crash if it asked
  *     to load a language version that is not included in this range.
  * @param allowedInputTransactionVersions The range of transaction
  *     version the engine is allowed to load. The engine will crash
  *     if it is asked to load transaction version that is not
  *     included in this range
  * @param allowedOutputTransactionVersions The range of output
  *     transactions the engine is allowed to produce. The Engine
  *     will always use the lowest possible version from this range to
  *     encode the output transaction, and fails if such version does
  *     not exist.
  * @param stackTraceMode The flag enables the runtime support for
  *     stack trace.
  * @param profileDir The optional specifies the directory where to
  *     save the output of the DAML scenario profiler. The profiler is
  *     disabled if the option is empty.
  */
final case class EngineConfig(
    allowedLanguageVersions: VersionRange[LanguageVersion],
    allowedInputTransactionVersions: VersionRange[TransactionVersion],
    allowedOutputTransactionVersions: VersionRange[TransactionVersion],
    packageValidation: Boolean = true,
    stackTraceMode: Boolean = false,
    profileDir: Option[Path] = None,
) {

  /**
    * The range of value versions the engine is allowed to load.  This
    * is deterministically derived from
    * [[allowedInputTransactionVersions]].
    */
  private[lf] val allowedInputValueVersions: VersionRange[ValueVersion] =
    VersionRange(
      TransactionVersions.assignValueVersion(allowedInputTransactionVersions.min),
      TransactionVersions.assignValueVersion(allowedInputTransactionVersions.max),
    )

  /**
    * The range of value versions the engine is allowed to produce.
    * This is deterministically derived from
    * [[allowedOutputTransactionVersions]].
    */
  private[lf] val allowedOutputValueVersions: VersionRange[ValueVersion] =
    VersionRange(
      TransactionVersions.assignValueVersion(allowedOutputTransactionVersions.min),
      TransactionVersions.assignValueVersion(allowedOutputTransactionVersions.max),
    )

}

object EngineConfig {

  /**
    * Most lenient production engine configuration. This allows the
    * engine to load and produce all non-deprecated stable versions of
    * language and transaction.
    */
  val Lenient: EngineConfig = new EngineConfig(
    allowedLanguageVersions = VersionRange(
      LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Stable("6")),
      LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Stable("8")),
    ),
    allowedInputTransactionVersions =
      VersionRange(TransactionVersion("10"), TransactionVersion("10")),
    allowedOutputTransactionVersions =
      VersionRange(TransactionVersion("10"), TransactionVersion("10"))
  )

  private[this] def toDev(config: EngineConfig): EngineConfig =
    config.copy(
      allowedLanguageVersions = config.allowedLanguageVersions.copy(
        max = LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Dev)),
      allowedInputTransactionVersions = config.allowedInputTransactionVersions.copy(
        max = TransactionVersions.acceptedVersions.last),
      allowedOutputTransactionVersions = config.allowedOutputTransactionVersions.copy(
        max = TransactionVersions.acceptedVersions.last),
    )

  /**
    * Recommanded production configuration.
    */
  def Stable: EngineConfig = Lenient

  /**
    * Development configuration, should not be used in PROD.  Allowed
    * the same input and output versions as [[Lenient]] plus the
    * development versions.
    */
  val Dev: EngineConfig = toDev(Lenient)

}
