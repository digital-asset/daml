// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import java.io.File

import com.digitalasset.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.daml.lf.language.{LanguageVersion, LanguageMajorVersion => LMajV, LanguageMinorVersion => LMinV}
import com.digitalasset.platform.tests.integration.ledger.api.ContractKeysIT

/** Runs tests with an older version of the language that does not include changes
  * performed in #1866 -- specifically the fact that the submitter must be in
  * lookup maintainers.
  */
class ContractKeysNo1866IT extends ContractKeysIT {
  override protected val languageVersion: LanguageVersion =
    LanguageVersion(LMajV.V1, LMinV.fromProtoIdentifier("5"))
  override protected lazy val config: Config = Config.default.copy(
    darFiles = List(new File(rlocation("ledger/sandbox/historical-dars/Test-1.5.dar")).toPath)
  )
}
