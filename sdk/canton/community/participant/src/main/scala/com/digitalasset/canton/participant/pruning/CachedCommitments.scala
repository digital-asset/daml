// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.protocol.messages.AcsCommitment
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.util.Mutex

import scala.collection.immutable.{Map, SortedSet}

/** Caches the commitments per participant and the commitments per stakeholder group in a period, in
  * order to optimize the computation of commitments for the subsequent period. It optimizes the
  * computation of a counter-participant commitments when at most half of the stakeholder
  * commitments shared with that participant change in the next period.
  *
  * The class is thread-safe w.r.t. calling [[setCachedCommitments]] and [[computeCmtFromCached]].
  * However, for correct commitment computation, the caller needs to call [[setCachedCommitments]]
  * before [[computeCmtFromCached]], because [[computeCmtFromCached]] uses the state set by
  * [[setCachedCommitments]].
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class CachedCommitments(
    private var prevParticipantCmts: Map[ParticipantId, AcsCommitment.CommitmentType] =
      Map.empty[ParticipantId, AcsCommitment.CommitmentType],
    private var prevStkhdCmts: Map[SortedSet[InternedPartyId], AcsCommitment.CommitmentType] = Map
      .empty[SortedSet[InternedPartyId], AcsCommitment.CommitmentType],
    private var prevParticipantToStkhd: Map[ParticipantId, Set[SortedSet[InternedPartyId]]] =
      Map.empty[ParticipantId, Set[SortedSet[InternedPartyId]]],
) {
  private val lock = new Mutex()

  def setCachedCommitments(
      cmts: Map[ParticipantId, AcsCommitment.CommitmentType],
      stkhdCmts: Map[SortedSet[InternedPartyId], AcsCommitment.CommitmentType],
      participantToStkhd: Map[ParticipantId, Set[SortedSet[InternedPartyId]]],
  ): Unit =
    lock.exclusive {
      // cache participant commitments
      prevParticipantCmts = cmts
      // cache stakeholder group commitments
      prevStkhdCmts = stkhdCmts
      prevParticipantToStkhd = participantToStkhd
    }

  def computeCmtFromCached(
      participant: ParticipantId,
      newStkhdCmts: Map[SortedSet[InternedPartyId], AcsCommitment.CommitmentType],
  ): Option[AcsCommitment.CommitmentType] =
    lock.exclusive {
      // a commitment is cached when we have the participant commitment, and
      // all commitments for all its stakeholder groups are cached, and exist
      // in the new stakeholder commitments (a delete exists as an empty commitment)
      val commitmentIsCached =
        prevParticipantCmts.contains(participant) &&
          prevParticipantToStkhd
            .get(participant)
            .exists(set =>
              set.forall(stkhds => prevStkhdCmts.contains(stkhds) && newStkhdCmts.contains(stkhds))
            )
      if (commitmentIsCached) {
        // remove from old commitment all stakeholder commitments that have changed
        val changedKeys = newStkhdCmts.filter { case (stkhd, newCmt) =>
          prevStkhdCmts
            .get(stkhd)
            .fold(false)(_ != newCmt && prevParticipantToStkhd(participant).contains(stkhd))
        }
        if (changedKeys.sizeIs > prevParticipantToStkhd(participant).size / 2) None
        else {
          val c = LtHash16.tryCreate(prevParticipantCmts(participant))
          changedKeys.foreach { case (stkhd, cmt) =>
            c.remove(LtHash16.tryCreate(prevStkhdCmts(stkhd)).get())
            // if the stakeholder group is still active, add its commitment
            if (cmt != AcsCommitmentProcessor.emptyCommitment) c.add(cmt.toByteArray)
          }
          // add new stakeholder group commitments for groups that were not active before
          newStkhdCmts.foreach { case (stkhds, cmt) =>
            if (
              !prevParticipantToStkhd(participant).contains(
                stkhds
              ) && cmt != AcsCommitmentProcessor.emptyCommitment
            )
              c.add(cmt.toByteArray)
          }
          Some(c.getByteString())
        }
      } else None
    }

  def clear(): Unit = setCachedCommitments(
    Map.empty[ParticipantId, AcsCommitment.CommitmentType],
    Map.empty[SortedSet[InternedPartyId], AcsCommitment.CommitmentType],
    Map.empty[ParticipantId, Set[SortedSet[InternedPartyId]]],
  )
}
