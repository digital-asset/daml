// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref

/** Identifier for ledger changes used by command deduplication
  *
  * @see ReadService.stateUpdates for the command deduplication guarantee
  */
final class ChangeId(
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    actAs: Set[Ref.Party],
) extends scala.Equals {

  /** A stable hash of the change id.
    * Suitable for storing in persistent storage.
    */
  val hash: Hash = Hash.hashChangeId(applicationId, commandId, actAs)

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  override def canEqual(that: Any): Boolean = that.isInstanceOf[ChangeId]

  override def equals(that: Any): Boolean = that match {
    case other: ChangeId =>
      if (this eq other) true
      else other.canEqual(this) && this.hash == other.hash
    case _ => false
  }

  override def hashCode(): Int = hash.hashCode()

  override def toString: String =
    s"ChangeId(applicationId=$applicationId, command ID=$commandId, actAs={${actAs.mkString(", ")}})"
}

object ChangeId {
  def apply(
      applicationId: Ref.ApplicationId,
      commandId: Ref.CommandId,
      actAs: Set[Ref.Party],
  ): ChangeId =
    new ChangeId(applicationId, commandId, actAs)
}
