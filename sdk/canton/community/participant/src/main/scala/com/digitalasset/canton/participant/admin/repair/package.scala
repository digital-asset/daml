// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApiParticipantProvider}
import com.digitalasset.canton.protocol.TransactionId

package object repair {

  /** Cooks up a random dummy transaction id.
    *
    * With single-participant repair commands, we have little hope of coming up with a transactionId
    * that matches up with other participants. We can get away with differing transaction ids across
    * participants because the AcsCommitmentProcessor does not compare transaction ids.
    */
  private[repair] def randomTransactionId(syncCrypto: SyncCryptoApiParticipantProvider) = {
    // We take as much entropy as for a random UUID.
    // This should be enough to guard against clashes between the repair requests executed on a single participant.
    // We don't have to worry about clashes with ordinary transaction IDs as the hash purpose is different.
    val randomness = syncCrypto.pureCrypto.generateRandomByteString(16)
    val hash = syncCrypto.pureCrypto.digest(HashPurpose.RepairTransactionId, randomness)
    TransactionId(hash)
  }
}
