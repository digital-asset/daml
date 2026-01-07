// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.topology.transaction.*

/** Onboarding transactions for an external party
  * @param partyToParticipant
  *   is mandatory and can be enough if self-authorized (i.e one of the protocol signing keys
  *   controls the party's namespace)
  * @param optionalDecentralizedNamespace
  *   optional to create external parties controlled by a decentralized namespace
  */
final case class OnboardingTransactions(
    partyToParticipant: SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
    optionalDecentralizedNamespace: Option[
      SignedTopologyTransaction[TopologyChangeOp.Replace, DecentralizedNamespaceDefinition]
    ] = None,
) {
  def toSeq: Seq[SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]] =
    Seq(partyToParticipant) ++ optionalDecentralizedNamespace.toList

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
