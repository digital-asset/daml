// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import com.digitalasset.canton.config.SessionSigningKeysConfig
import org.scalatest.wordspec.AnyWordSpec

class SyncCryptoSignerWithLongTermKeysTest extends AnyWordSpec with SyncCryptoSignerTest {
  // we explicitly disable any use of session signing keys
  override protected lazy val sessionSigningKeysConfig: SessionSigningKeysConfig =
    SessionSigningKeysConfig.disabled

  "A long-term key SyncCryptoSigner" must {

    "use correct sync crypto signer with long term keys" in {
      syncCryptoSignerP1 shouldBe a[SyncCryptoSignerWithLongTermKeys]
    }

    behave like syncCryptoSignerTest()
  }

}
