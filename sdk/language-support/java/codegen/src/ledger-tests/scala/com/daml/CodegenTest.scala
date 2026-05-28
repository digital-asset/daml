// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import org.scalatest.flatspec.AsyncFlatSpec
import com.daml.integrationtest.CantonConfig
import com.daml.integrationtest.CantonConfig.ProtocolVersion

// We factorize the 3 test to spin Canton only once.

class CodegenTest extends AsyncFlatSpec with LedgerTest with InterfacesTest with StakeholdersTest {
  // TODO(https://github.com/digital-asset/daml/issues/18457): split key test cases and revert to
  //  protocolVersion = "latest" for non-key test cases.
  // TODO[23052]: be smarter about this ProtocolVersion when we unhardcode staging
  override protected lazy val protocolVersion = ProtocolVersion.Dev

  // TODO[23052]: be smarter about this dev mode when we unhardcode staging
  override protected def cantonConfig(): CantonConfig =
    super.cantonConfig().copy(enableLfDevVersionSupport = true)
}
