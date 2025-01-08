// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.util.SetsUtil.requireDisjoint

/** Defines the contracts and reassignments for conflict detection.
  * Reassignments are not locked because the reassigned contracts are already being locked.
  */
final case class ActivenessSet(
    contracts: ActivenessCheck[LfContractId],
    reassignmentIds: Set[ReassignmentId],
) extends PrettyPrinting {

  override protected def pretty: Pretty[ActivenessSet] = prettyOfClass(
    param("contracts", _.contracts),
    paramIfNonEmpty("reassignmentIds", _.reassignmentIds),
  )
}

object ActivenessSet {
  val empty: ActivenessSet =
    ActivenessSet(
      ActivenessCheck.empty[LfContractId],
      Set.empty,
    )
}

/** Defines the activeness checks and locking for one kind of states (contracts, keys, ...).
  *
  * [[ActivenessCheck.checkFresh]], [[ActivenessCheck.checkFree]], and [[ActivenessCheck.checkActive]]
  * must be pairwise disjoint.
  *
  * @param needPriorState The set of `Key`s whose prior state should be returned in [[ActivenessCheckResult.priorStates]].
  * @tparam Key The identifier of the items to check (contract IDs or contract keys)
  * @throws java.lang.IllegalArgumentException if [[ActivenessCheck.checkFresh]], [[ActivenessCheck.checkFree]],
  *                                            and [[ActivenessCheck.checkActive]] are not pairwise disjoint;
  *                                            or if [[ActivenessCheck.needPriorState]] is not covered jointly by
  *                                            [[ActivenessCheck.checkFresh]], [[ActivenessCheck.checkFree]],
  *                                            [[ActivenessCheck.checkActive]], and [[ActivenessCheck.lock]]
  */
private[participant] final case class ActivenessCheck[Key] private (
    checkFresh: Set[Key],
    checkFree: Set[Key],
    checkActive: Set[Key],
    lock: Set[Key],
    needPriorState: Set[Key],
)(implicit val prettyK: Pretty[Key])
    extends PrettyPrinting {

  requireDisjoint(checkFresh -> "fresh", checkFree -> "free")
  requireDisjoint(checkFresh -> "fresh", checkActive -> "active")
  requireDisjoint(checkFree -> "free", checkActive -> "active")

  locally {
    val uncovered = needPriorState.filterNot(k =>
      checkFresh.contains(k) || checkFree.contains(k) || checkActive.contains(k) || lock.contains(k)
    )
    require(uncovered.isEmpty, show"$uncovered are not being checked for activeness")
  }

  val lockOnly: Set[Key] = lock -- checkFresh -- checkFree -- checkActive

  override protected def pretty: Pretty[ActivenessCheck.this.type] = prettyOfClass(
    paramIfNonEmpty("fresh", _.checkFresh),
    paramIfNonEmpty("free", _.checkFree),
    paramIfNonEmpty("active", _.checkActive),
    paramIfNonEmpty("lock", _.lock),
    paramIfNonEmpty("need prior state", _.needPriorState),
  )
}

private[participant] object ActivenessCheck {
  def empty[Key: Pretty]: ActivenessCheck[Key] =
    ActivenessCheck(Set.empty, Set.empty, Set.empty, Set.empty, Set.empty)

  def tryCreate[Key](
      checkFresh: Set[Key],
      checkFree: Set[Key],
      checkActive: Set[Key],
      lock: Set[Key],
      needPriorState: Set[Key],
  )(implicit prettyK: Pretty[Key]): ActivenessCheck[Key] =
    ActivenessCheck(checkFresh, checkFree, checkActive, lock, needPriorState)
}
