// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.nio.file.Path
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
  * @param requireSuffixedGlobalCids Since August 2018 we expect new
  *     ledgers to suffix CIDs before committing a transaction.
  *     This option should be disable for backward compatibility in ledger
  *     that do not (i.e. Sandboxes, KV, Corda).
  */
final case class EngineConfig(
    allowedLanguageVersions: VersionRange[language.LanguageVersion],
    packageValidation: Boolean = true,
    stackTraceMode: Boolean = false,
    profileDir: Option[Path] = None,
    contractKeyUniqueness: ContractKeyUniquenessMode = ContractKeyUniquenessMode.Strict,
    forbidV0ContractId: Boolean = false,
    requireSuffixedGlobalContractId: Boolean = false,
    limits: interpretation.Limits = interpretation.Limits.Lenient,
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
