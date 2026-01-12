// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.speedy.{
  AuthorizationChecker,
  DefaultAuthorizationChecker,
  NoopAuthorizationChecker,
}

import java.nio.file.Path
import com.digitalasset.daml.lf.transaction.ContractKeyUniquenessMode

/** The Engine configurations describes the versions of language and
  * transaction the engine is allowed to read and write together with
  * engine debugging feature.
  *
  * <p>
  *
  * @param allowedLanguageVersions The range of language versions the
  *     engine is allowed to load.  The engine will crash if it asked
  *     to load a language version that is not included in this range.
  * @param transactionTraceMaxLenght Specified the maximum length of
  *     the stack trace reported in case of interpretation error.
  * @param stackTraceMode The flag enables the runtime support for
  *     stack trace.
  * @param profileDir The optional specifies the directory where to
  *     save the output of the Daml scenario profiler. The profiler is
  *     disabled if the option is empty.
  * @param snapshotDir The optional specifies the directory where Daml snapshots
  *      will be saved. Snapshots are disabled if the option is empty.
  * @param forbidLocalContractIds Since August 2018 we expect new
  *     ledgers to suffix CIDs before committing a transaction.
  *     This option should be disabled for backward compatibility in ledger
  *     that do not (i.e. Sandboxes).
  * @param checkAuthorization Whether to check authorization of transaction.
  *     A value of false is insecure and should be used for security testing only.
  * @param iterationsBetweenInterruptions bound the maximal number of interpreter
  *     steps needed to produce a Result.
  * @param useDefensiveContractLookup When true, contracts passed to the ResultNeedContract
  *      callback are type-checked, authenticated and upgraded by the engine. Explicit disclosures
  *      are assumed to be provided via ResultNeedContract.
  *     This parameter is temporary and will be retired once all client code has migrated
  *     to the new `ResultNeedContract` question.
  */
final case class EngineConfig(
    allowedLanguageVersions: VersionRange[language.LanguageVersion],
    packageValidation: Boolean = true,
    transactionTraceMaxLength: Int = 10,
    stackTraceMode: Boolean = false,
    profileDir: Option[Path] = None,
    snapshotDir: Option[Path] = None,
    contractKeyUniqueness: ContractKeyUniquenessMode = ContractKeyUniquenessMode.Strict,
    forbidLocalContractIds: Boolean = false,
    limits: interpretation.Limits = interpretation.Limits.Lenient,
    checkAuthorization: Boolean = true,
    iterationsBetweenInterruptions: Long = 10000,
    paranoid: Boolean = false,
    useDefensiveContractLookup: Boolean = false,
    gasBudget: Option[Long] = None,
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

  private[lf] def authorizationChecker: AuthorizationChecker =
    if (checkAuthorization) DefaultAuthorizationChecker else NoopAuthorizationChecker
}
