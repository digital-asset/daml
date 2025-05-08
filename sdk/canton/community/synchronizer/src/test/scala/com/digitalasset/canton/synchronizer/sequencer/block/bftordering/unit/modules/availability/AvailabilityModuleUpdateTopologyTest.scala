// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.availability

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import org.scalatest.wordspec.AnyWordSpec

class AvailabilityModuleUpdateTopologyTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  "The availability module" should {

    "update the topology during state transfer if it's more recent" in {
      val initialMembership = Membership.forTesting(Node0)
      val initialCryptoProvider = failingCryptoProvider[IgnoringUnitTestEnv]
      val newMembership = Membership.forTesting(Node0, Set(Node1))
      val newOrderingTopology = newMembership.orderingTopology
      val newCryptoProvider = failingCryptoProvider[IgnoringUnitTestEnv]

      val availability =
        createAvailability[IgnoringUnitTestEnv](cryptoProvider = initialCryptoProvider)

      // double-check initial values
      availability.getActiveMembership shouldBe initialMembership
      availability.getActiveCryptoProvider shouldBe initialCryptoProvider
      availability.getMessageAuthorizer shouldBe initialMembership.orderingTopology

      availability.receive(
        Availability.Consensus
          .UpdateTopologyDuringStateTransfer(newOrderingTopology, newCryptoProvider)
      )

      // make sure new values are different
      availability.getActiveMembership.orderingTopology shouldBe newOrderingTopology // we don't care about other fields
      availability.getActiveCryptoProvider shouldBe newCryptoProvider
      availability.getMessageAuthorizer shouldBe newOrderingTopology
    }

    "do not update the topology to an outdated one" in {
      val initialMembership = Membership
        .forTesting(Node0)
        .copy(orderingTopology =
          OrderingTopologyNode0
            .copy(activationTime = TopologyActivationTime(CantonTimestamp.MaxValue))
        )
      val initialOrderingTopology = initialMembership.orderingTopology
      val initialCryptoProvider = failingCryptoProvider[IgnoringUnitTestEnv]
      val newMembership = Membership.forTesting(Node0, Set(Node1))
      val newOrderingTopology = newMembership.orderingTopology.copy(activationTime =
        TopologyActivationTime(initialOrderingTopology.activationTime.value.minusSeconds(1))
      )
      val newCryptoProvider = failingCryptoProvider[IgnoringUnitTestEnv]

      val availability =
        createAvailability[IgnoringUnitTestEnv](
          cryptoProvider = initialCryptoProvider,
          customMembership = Some(initialMembership),
        )

      suppressProblemLogs(
        availability.receive(
          Availability.Consensus
            .UpdateTopologyDuringStateTransfer(newOrderingTopology, newCryptoProvider)
        )
      )

      availability.getActiveMembership.orderingTopology shouldBe initialOrderingTopology
      availability.getActiveCryptoProvider shouldBe initialCryptoProvider
      availability.getMessageAuthorizer shouldBe initialOrderingTopology
    }
  }
}
