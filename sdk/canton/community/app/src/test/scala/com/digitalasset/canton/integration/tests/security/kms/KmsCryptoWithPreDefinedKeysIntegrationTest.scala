// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.digitalasset.canton.config.CryptoProvider
import com.digitalasset.canton.crypto.store.KmsCryptoPrivateStore
import com.digitalasset.canton.integration.{CommunityIntegrationTest, EnvironmentSetup}

/** Runs a crypto integration tests with one participant using a KMS provider with pre-generated
  * keys. Runs with persistence so we also check that it is able to recover from an unexpected
  * shutdown.
  */
trait KmsCryptoWithPreDefinedKeysIntegrationTest extends KmsCryptoIntegrationTestBase {
  self: CommunityIntegrationTest & EnvironmentSetup =>

  "be able to restart from a persisted state" in { implicit env =>
    import env.*

    assert(participant1.config.crypto.provider == CryptoProvider.Kms)
    assert(participant1.crypto.cryptoPrivateStore.isInstanceOf[KmsCryptoPrivateStore])

    /* restarting the node means that we need to rebuild our mappings between the public keys
     * stored in Canton and the private keys externally stored in the KMS */
    participant1.stop()

    participant1.start()
    participant1.health.wait_for_running()

    participant1.synchronizers.connect_local(sequencer1, alias = daName)

    eventually() {
      assertPingSucceeds(participant1, participant2)
    }
  }

}
