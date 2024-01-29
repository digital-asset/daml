// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.participant.store.{ActiveContractStore, ContractKeyJournal}

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
        case TransferredAway(_) => true
        case Active | Archived => false
      }

      override def isActive(status: Status): Boolean = status match {
        case Active => true
        case Archived | TransferredAway(_) => false
      }

      override def shouldEvict(status: Status): Boolean = true
    }

  implicit val contractKeyJournalStateLockableStatus: LockableStatus[ContractKeyJournal.Status] =
    new LockableStatus[ContractKeyJournal.Status] {
      import ContractKeyJournal.*

      override def kind: String = "key"

      override def isFree(status: Status): Boolean = status match {
        case Unassigned => true
        case Assigned => false
      }

      override def isActive(status: Status): Boolean = status match {
        case Assigned => true
        case Unassigned => false
      }

      override def shouldEvict(status: Status): Boolean = true
    }

}
