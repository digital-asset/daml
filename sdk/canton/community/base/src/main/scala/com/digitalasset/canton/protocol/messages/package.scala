// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.sequencing.protocol.{AllMembersOfSynchronizer, OpenEnvelope}

/** This package contains data structures used in the transaction protocol. However, generic data
  * structures, e.g. [[com.digitalasset.canton.data.MerkleTree]] etc. are kept in
  * [[com.digitalasset.canton.data]] package.
  */
package object messages {

  type TransactionViewMessage = EncryptedViewMessage[TransactionViewType]

  type DefaultOpenEnvelope = OpenEnvelope[ProtocolMessage]
  object DefaultOpenEnvelopesFilter {

    /** @param withExplicitTopologyTimestamp
      *   Whether the event contained a prescribed topology timestamp.
      */
    def containsTopology(
        envelopes: Seq[DefaultOpenEnvelope],
        withExplicitTopologyTimestamp: Boolean,
    ): Boolean = !withExplicitTopologyTimestamp && envelopes.exists { envelope =>
      val broadcastO = ProtocolMessage.select[TopologyTransactionsBroadcast](envelope)
      val envelopeIsValidBroadcast =
        broadcastO.exists(_.recipients.allRecipients.contains(AllMembersOfSynchronizer))

      envelopeIsValidBroadcast
    }
  }

}
