// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import java.io.File
import com.digitalasset.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.daml.lf.language.{
  LanguageVersion,
  LanguageMajorVersion => LMajV,
  LanguageMinorVersion => LMinV
}
import com.digitalasset.platform.apitesting.CommandTransactionChecksHighLevel

/** Runs tests with an older version of the language that does not include changes
  * performed in #1866 -- specifically the fact that the submitter must be in
  * lookup maintainers.
  *
  * If you're tempted to remove this because the compiler cannot generate DAML-LF 1.5
  * anymore, don't! Instead, generate a DAML-LF 1.5 file with an older compiler
  * or manually, and check it in. It's important to preserve these tests.
  *
  * TODO extract version-dependent parts out of the huge `CommandTransactionChecks`.
  */
class CommandTransactionChecksNo1866IT extends CommandTransactionChecksHighLevel {
  override protected val languageVersion: LanguageVersion =
    LanguageVersion(LMajV.V1, LMinV.fromProtoIdentifier("5"))
  override protected lazy val config: Config = Config.default.copy(
    darFiles = List(new File(rlocation("ledger/sandbox/Test-1.5.dar")).toPath)
  )
}
