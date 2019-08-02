// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import com.digitalasset.daml.lf.language.{
  LanguageVersion,
  LanguageMajorVersion => LMajV,
  LanguageMinorVersion => LMinV
}
import com.digitalasset.platform.apitesting.ContractKeysChecks

class ContractKeysIT extends ContractKeysChecks {
  protected override val languageVersion: LanguageVersion = LanguageVersion(LMajV.V1, LMinV.Dev)
}
