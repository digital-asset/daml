// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import org.scalatest.flatspec.AsyncFlatSpec

// We factorize the 3 test to spin Canton only once.

class CodegenTest extends AsyncFlatSpec with LedgerTest with InterfacesTest with StakeholdersTest {
  // TODO(https://github.com/digital-asset/daml/issues/18457): split key test cases and revert to
  //  devMode = false for non-key test cases.
  override lazy val devMode = true
}
