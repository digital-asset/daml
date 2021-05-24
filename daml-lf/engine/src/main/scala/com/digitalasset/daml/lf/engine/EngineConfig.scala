// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.nio.file.Path

import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.ContractKeyUniquenessMode

/** The Engine configurations describes the versions of language and
  * transaction the engine is allowed to read and write together with
  * engine debugging feature.
  *
  * <p>
  *
  * @param allowedLanguageVersions The range of language versions the
  *     engine is allowed to load.  The engine will crash if it asked
  *     to load a language version that is not included in this range.
  * @param stackTraceMode The flag enables the runtime support for
  *     stack trace.
  * @param profileDir The optional specifies the directory where to
  *     save the output of the Daml scenario profiler. The profiler is
  *     disabled if the option is empty.
  */
final case class EngineConfig(
    allowedLanguageVersions: VersionRange[language.LanguageVersion],
    packageValidation: Boolean = true,
    stackTraceMode: Boolean = false,
    profileDir: Option[Path] = None,
    contractKeyUniqueness: ContractKeyUniquenessMode = ContractKeyUniquenessMode.On,
) {

  private[lf] def getCompilerConfig: speedy.Compiler.Config =
    speedy.Compiler.Config(
      allowedLanguageVersions,
      packageValidation =
        if (packageValidation)
          speedy.Compiler.FullPackageValidation
        else
          speedy.Compiler.NoPackageValidation,
      stacktracing =
        if (stackTraceMode)
          speedy.Compiler.FullStackTrace
        else
          speedy.Compiler.NoStackTrace,
      profiling =
        if (profileDir.isDefined)
          speedy.Compiler.FullProfile
        else
          speedy.Compiler.NoProfile,
    )

}

object EngineConfig {

  /** Recommended production configuration.
    * Allows the all stable versions of language.
    */
  @deprecated("use LanguageVersion.StableVersions directly", since = "1.9.0")
  def Stable: EngineConfig = new EngineConfig(
    allowedLanguageVersions = LanguageVersion.StableVersions
  )

  /** Only allows language versions compatible with the legacy contract ID scheme.
    */
  @deprecated("use LanguageVersion.LegacyVersions directly", since = "1.9.0")
  def Legacy: EngineConfig = new EngineConfig(
    allowedLanguageVersions = LanguageVersion.LegacyVersions
  )

  /** Development configuration, should not be used in PROD.
    * Allows all language versions
    */
  @deprecated("use LanguageVersion.DevVersions directly", since = "1.9.0")
  def Dev: EngineConfig = new EngineConfig(
    allowedLanguageVersions = LanguageVersion.DevVersions
  )

}
