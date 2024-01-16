// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.topology.{
  DomainTopologyManagerId,
  Identifier,
  Namespace,
  TopologyManagerTest,
  UniqueIdentifier,
}

// TODO(#15303) Remove this test
class DomainTopologyManagerTest extends TopologyManagerTest {

  "domain topology manager" should {
    behave like topologyManager { (clock, store, crypto, factory) =>
      for {
        keys <- crypto.cryptoPublicStore.signingKeys.valueOrFail("signing keys")
      } yield {
        val id =
          UniqueIdentifier(Identifier.tryCreate("da"), Namespace(keys.headOption.value.fingerprint))

        new DomainTopologyManager(
          DomainTopologyManagerId(id),
          clock,
          store,
          crypto,
          DefaultProcessingTimeouts.testing,
          BaseTest.testedProtocolVersion,
          factory,
          futureSupervisor,
        )
      }
    }
  }
}
