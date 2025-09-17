// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.topology.transaction.*

/** Onboarding transactions for an external party
  */
final case class OnboardingTransactions(
    namespaceDelegation: SignedTopologyTransaction[TopologyChangeOp.Replace, NamespaceDelegation],
    partyToParticipant: SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
    partyToKeyMapping: SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToKeyMapping],
) {
  def toSeq: Seq[SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]] =
    Seq(namespaceDelegation, partyToParticipant, partyToKeyMapping)
}
