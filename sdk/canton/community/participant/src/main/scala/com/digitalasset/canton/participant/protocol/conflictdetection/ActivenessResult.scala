// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.{ActiveContractStore, ContractKeyJournal}
import com.digitalasset.canton.protocol.{LfContractId, LfGlobalKey, TransferId}

/** The result of the activeness check for an [[ActivenessSet]].
  * If all sets are empty, the activeness check was successful.
  *
  * @param contracts The contracts whose activeness check has failed
  * @param inactiveTransfers The transfers that shall be completed, but that are not active.
  */
final case class ActivenessResult(
    contracts: ActivenessCheckResult[LfContractId, ActiveContractStore.Status],
    inactiveTransfers: Set[TransferId],
    keys: ActivenessCheckResult[LfGlobalKey, ContractKeyJournal.Status],
) extends PrettyPrinting {

  def isSuccessful: Boolean =
    contracts.isSuccessful && inactiveTransfers.isEmpty && keys.isSuccessful

  override def pretty: Pretty[ActivenessResult] = {
    prettyOfClass(
      param("contracts", _.contracts, !_.contracts.isEmpty),
      paramIfNonEmpty("inactiveTransfers", _.inactiveTransfers),
      param("keys", _.keys, !_.keys.isEmpty),
    )
  }

}

/** The result of the activeness check for an [[ActivenessCheck]].
  * If all sets are empty, the activeness check was successful.
  *
  * @param alreadyLocked The items that have already been locked at the activeness check.
  * @param notFresh The items that are supposed to not exist, but do.
  * @param notFree The items that shall be free, but are not.
  * @param notActive The contracts that shall be active, but are not.
  * @param priorStates The prior states for the items from [[ActivenessCheck.needPriorState]].
  *                    Does not contain items that were already locked
  *                    as the prior state of a locked item is not known during conflict detection.
  *                    Mapped to [[scala.None$]] if the item is fresh.
  */
private[conflictdetection] final case class ActivenessCheckResult[Key, Status <: PrettyPrinting](
    alreadyLocked: Set[Key],
    notFresh: Set[Key],
    unknown: Set[Key],
    notFree: Map[Key, Status],
    notActive: Map[Key, Status],
    priorStates: Map[Key, Option[Status]],
)(implicit val prettyK: Pretty[Key])
    extends PrettyPrinting {

  private[conflictdetection] def isEmpty: Boolean = isSuccessful && priorStates.isEmpty

  /** Returns whether all checks were successful. The caller must check the requested prior states separately. */
  def isSuccessful: Boolean =
    alreadyLocked.isEmpty && notFresh.isEmpty && unknown.isEmpty && notFree.isEmpty && notActive.isEmpty

  override def pretty: Pretty[ActivenessCheckResult.this.type] = prettyOfClass(
    paramIfNonEmpty("alreadyLocked", _.alreadyLocked),
    paramIfNonEmpty("notFresh", _.notFresh),
    paramIfNonEmpty("unknown", _.unknown),
    paramIfNonEmpty("notFree", _.notFree),
    paramIfNonEmpty("notActive", _.notActive),
    paramIfNonEmpty("priorStates", _.priorStates),
  )
}
