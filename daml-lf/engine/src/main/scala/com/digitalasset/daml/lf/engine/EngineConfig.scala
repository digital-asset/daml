// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.nio.file.Path

import com.daml.lf.language.LanguageVersion

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
  * @param stackTraceMode The flag enables the runtime support for
  *     stack trace.
  * @param profileDir The optional specifies the directory where to
  *     save the output of the DAML scenario profiler. The profiler is
  *     disabled if the option is empty.
  */
final case class EngineConfig(
    allowedLanguageVersions: VersionRange[language.LanguageVersion],
    packageValidation: Boolean = true,
    stackTraceMode: Boolean = false,
    profileDir: Option[Path] = None,
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

  /**
    * Most lenient production engine configuration. This allows the
    * engine to load and produce all non-deprecated stable versions of
    * language and transaction.
    */
  val Lenient: EngineConfig = new EngineConfig(
    allowedLanguageVersions = transaction.VersionTimeline.stableLanguageVersions,
  )

  private[this] def toDev(config: EngineConfig): EngineConfig =
    config.copy(
      allowedLanguageVersions = config.allowedLanguageVersions.copy(
        max = LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Dev)),
    )

  /**
    * Recommended production configuration.
    */
  def Stable: EngineConfig = Lenient

  /**
    * Development configuration, should not be used in PROD.  Allowed
    * the same input and output versions as [[Lenient]] plus the
    * development versions.
    */
  val Dev: EngineConfig = toDev(Lenient)

}
