// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.topology.{
  Identifier,
  Namespace,
  ParticipantId,
  TopologyManagerTest,
  UniqueIdentifier,
}

// TODO(#15303) Remove this test
class ParticipantTopologyManagerTest extends TopologyManagerTest {

  "participant topology state manager" should {
    behave like topologyManager { (clock, store, crypto, factory) =>
      for {
        keys <- crypto.cryptoPublicStore.signingKeys.valueOrFail("signing keys")
      } yield {
        val id =
          UniqueIdentifier(Identifier.tryCreate("da"), Namespace(keys.headOption.value.fingerprint))

        val mgr = new ParticipantTopologyManager(
          clock,
          store,
          crypto,
          new PackageDependencyResolver(
            new InMemoryDamlPackageStore(loggerFactory),
            timeouts,
            loggerFactory,
          ),
          DefaultProcessingTimeouts.testing,
          testedProtocolVersion,
          factory,
          futureSupervisor,
        )
        mgr.setParticipantId(ParticipantId(id))
        mgr
      }
    }

  }

}
