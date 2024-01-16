// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.sequencing.protocol.{AllMembersOfDomain, OpenEnvelope}

/** This package contains data structures used in the transaction protocol.
  * However, generic data structures, e.g. [[com.digitalasset.canton.data.MerkleTree]] etc. are
  * kept in [[com.digitalasset.canton.data]] package.
  */
package object messages {

  type TransferOutResult = TransferResult[SourceDomainId]
  val TransferOutResult: TransferResult.type = TransferResult

  type TransferInResult = TransferResult[TargetDomainId]
  val TransferInResult: TransferResult.type = TransferResult

  type TransactionViewMessage = EncryptedViewMessage[TransactionViewType]

  type DefaultOpenEnvelope = OpenEnvelope[ProtocolMessage]
  object DefaultOpenEnvelopesFilter {
    def containsTopologyX(envelopes: Seq[DefaultOpenEnvelope]): Boolean = envelopes.exists {
      envelope =>
        val broadcastO = ProtocolMessage.select[TopologyTransactionsBroadcastX](envelope)
        val envelopeIsValidBroadcast =
          broadcastO.exists(_.recipients.allRecipients.contains(AllMembersOfDomain))

        envelopeIsValidBroadcast
    }
  }

}
