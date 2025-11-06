// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.topology.transaction.*

/** Onboarding transactions for an external party
  */
final case class OnboardingTransactions(
    namespace: SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping],
    partyToParticipant: SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
    partyToKeyMapping: SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToKeyMapping],
) {
  def toSeq: Seq[SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]] =
    Seq(namespace, partyToParticipant, partyToKeyMapping)

  def transactionsWithSingleSignature
      : Seq[(TopologyTransaction[TopologyChangeOp.Replace, TopologyMapping], Seq[Signature])] =
    toSeq.map { signedTransaction =>
      signedTransaction.transaction -> signedTransaction.signatures.collect {
        case SingleTransactionSignature(_, signature) => signature
      }.toSeq
    }

  def multiTransactionSignatures: Seq[Signature] = toSeq.flatMap(_.signatures).collect {
    case MultiTransactionSignature(_, signature) => signature
  }
}
