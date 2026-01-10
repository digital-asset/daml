// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.parties

import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.admin.party_management_service
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.{Fingerprint, Hash}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.TopologyTransaction.PositiveTopologyTransaction
import com.digitalasset.canton.topology.transaction.{TopologyChangeOp, TopologyTransaction}

final case class GenerateExternalPartyTopology(
    partyId: PartyId,
    publicKeyFingerprint: Fingerprint,
    topologyTransactions: Seq[PositiveTopologyTransaction],
    multiHash: Hash,
)

object GenerateExternalPartyTopology {
  def fromProto(
      value: party_management_service.GenerateExternalPartyTopologyResponse
  ): ParsingResult[GenerateExternalPartyTopology] = {
    val party_management_service.GenerateExternalPartyTopologyResponse(
      partyIdP,
      publicKeyFingerprintP,
      topologyTransactionsP,
      multiHashP,
    ) = value
    for {
      partyId <- PartyId.fromProtoPrimitive(partyIdP, "party_id")
      fingerprint <- Fingerprint.fromProtoPrimitive(publicKeyFingerprintP)
      topologyTransactions <- topologyTransactionsP.traverse(
        TopologyTransaction.fromTrustedByteString
      )
      positive = topologyTransactions.mapFilter(_.selectOp[TopologyChangeOp.Replace])
      _ <- Either.cond(
        topologyTransactions.sizeIs == positive.length,
        (),
        ProtoDeserializationError.ValueDeserializationError(
          "topology_transactions",
          "Unexpected Remove topology transaction",
        ),
      )
      multiHash <- Hash.fromProtoPrimitive(multiHashP)
    } yield GenerateExternalPartyTopology(partyId, fingerprint, positive, multiHash)
  }
}
