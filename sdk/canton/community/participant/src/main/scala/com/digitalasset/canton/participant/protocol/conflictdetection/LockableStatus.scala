// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.participant.store.ActiveContractStore

/** Type class for operations on statuses that can be locked.
  *
  * Also defines the eviction strategy for items.
  */
private[conflictdetection] trait LockableStatus[-Status] {

  /** The kind of item this `Status` can be used for. Used for logging and pretty printing. */
  def kind: String

  /** Determines whether the activeness check for being free should pass for unlocked items in this status */
  def isFree(status: Status): Boolean

  /** Determines whether the activeness check for being active should pass for unlocked items in this status */
  def isActive(status: Status): Boolean

  /** Determines whether the conflict detector should not keep items with this status in memory
    * if it is safe from the conflict detection perspective to evict them.
    */
  def shouldEvict(status: Status): Boolean
}

private[conflictdetection] object LockableStatus {
  def apply[Status](implicit instance: LockableStatus[Status]): LockableStatus[Status] = instance

  implicit val activeContractStoreLockableStatus: LockableStatus[ActiveContractStore.Status] =
    new LockableStatus[ActiveContractStore.Status] {
      import ActiveContractStore.*

      override def kind: String = "contract"

      override def isFree(status: Status): Boolean = status match {
        case TransferredAway(_, _) => true
        case Active(_) | Archived | Purged => false
      }

      override def isActive(status: Status): Boolean = status match {
        case Active(_) => true
        case Archived | Purged | TransferredAway(_, _) => false
      }

      override def shouldEvict(status: Status): Boolean = true
    }
}
