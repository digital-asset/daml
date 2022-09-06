// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref

/** Identifier for ledger changes used by command deduplication.
  * Equality is defined in terms of the cryptographic hash.
  *
  * @see ReadService.stateUpdates for the command deduplication guarantee
  */
final case class ChangeId(
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    actAs: Set[Ref.Party],
) {

  /** A stable hash of the change id.
    * Suitable for storing in persistent storage.
    */
  lazy val hash: Hash = Hash.hashChangeId(applicationId, commandId, actAs)

  override def equals(that: Any): Boolean = that match {
    case other: ChangeId =>
      if (this eq other) true
      else other.canEqual(this) && this.hash == other.hash
    case _ => false
  }

  override def hashCode(): Int = hash.hashCode()
}
