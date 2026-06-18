// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.syntax.functor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.ledger.participant.state.AcsChange
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.CommitmentSnapshot
import com.digitalasset.canton.protocol.messages.AcsCommitment.CommitmentType

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.{Map, SortedSet}
import scala.concurrent.blocking

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class RunningCommitments(
    initRt: RecordTime,
    commitments: TrieMap[SortedSet[LfPartyId], LtHash16],
) extends HasLoggerName {

  private val lock = new Object
  @volatile private var rt: RecordTime = initRt
  private val deltaB = Map.newBuilder[SortedSet[LfPartyId], LtHash16]

  /** The latest (immutable) snapshot. Taking the snapshot also garbage collects empty commitments.
    */
  def snapshot(gc: Boolean = true): CommitmentSnapshot = {

    /* Delete all hashes that have gone empty since the last snapshot if gc is true;
      returns the corresponding stakeholder sets */
    def garbageCollect(
        candidates: Map[SortedSet[LfPartyId], LtHash16]
    ): Set[SortedSet[LfPartyId]] = {
      val deletedB = Set.newBuilder[SortedSet[LfPartyId]]
      candidates.foreach { case (stkhs, h) =>
        if (h.isEmpty) {
          deletedB += stkhs
          if (gc) commitments -= stkhs
        }
      }
      deletedB.result()
    }

    blocking {
      lock.synchronized {
        val delta = deltaB.result()
        if (gc) deltaB.clear()
        val deleted = garbageCollect(delta)
        val activeDelta = (delta -- deleted).fmap(_.getByteString())
        // Note that it's crucial to eagerly (via fmap, as opposed to, say mapValues) snapshot the LtHash16 values,
        // since they're mutable
        CommitmentSnapshot(
          rt,
          commitments.readOnlySnapshot().toMap.fmap(_.getByteString()),
          activeDelta,
          deleted,
        )
      }
    }
  }

  def update(rt: RecordTime, change: AcsChange)(implicit
      loggingContext: NamedLoggingContext
  ): Unit = {
    import com.digitalasset.canton.lfPartyOrdering
    blocking {
      lock.synchronized {
        this.rt = rt
        change.activations.foreach { case (cid, stakeholdersAndReassignmentCounter) =>
          val sortedStakeholders =
            SortedSet(stakeholdersAndReassignmentCounter.stakeholders.toSeq*)
          val h = commitments.getOrElseUpdate(sortedStakeholders, LtHash16())
          AcsCommitmentProcessor.addContractToCommitmentDigest(
            h,
            cid,
            stakeholdersAndReassignmentCounter.reassignmentCounter,
          )
          loggingContext.debug(
            s"Adding to commitment activation cid $cid reassignmentCounter ${stakeholdersAndReassignmentCounter.reassignmentCounter}"
          )
          deltaB += sortedStakeholders -> h
        }
        change.deactivations.foreach { case (cid, stakeholdersAndReassignmentCounter) =>
          val sortedStakeholders =
            SortedSet(stakeholdersAndReassignmentCounter.stakeholders.toSeq*)
          val h = commitments.getOrElseUpdate(sortedStakeholders, LtHash16())
          AcsCommitmentProcessor.removeContractFromCommitmentDigest(
            h,
            cid,
            stakeholdersAndReassignmentCounter.reassignmentCounter,
          )
          loggingContext.debug(
            s"Removing from commitment deactivation cid $cid reassignmentCounter ${stakeholdersAndReassignmentCounter.reassignmentCounter}"
          )
          deltaB += sortedStakeholders -> h
        }
      }
    }
  }

  def watermark: RecordTime = rt

  def reinitialize(snapshot: Map[SortedSet[LfPartyId], CommitmentType], recordTime: RecordTime) =
    blocking {
      lock.synchronized {
        // delete all active
        deltaB.clear()
        commitments.clear()
        snapshot.foreach { case (stkhd, cmt) =>
          commitments += stkhd -> LtHash16.tryCreate(cmt)
          deltaB += stkhd -> LtHash16.tryCreate(cmt)
        }
        rt = recordTime
      }
    }
}
