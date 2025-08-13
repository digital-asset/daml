// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

/** Represents a change to the ACS. The contracts are accompanied by their stakeholders.
  *
  * Note that we include the LfContractId (for uniqueness), but we do not include the contract hash
  * because the contract id already authenticates the contract contents.
  */
final case class AcsChange(
    activations: Map[LfContractId, ContractStakeholdersAndReassignmentCounter],
    deactivations: Map[LfContractId, ContractStakeholdersAndReassignmentCounter],
)

final case class ContractStakeholdersAndReassignmentCounter(
    stakeholders: Set[LfPartyId],
    reassignmentCounter: ReassignmentCounter,
) extends PrettyPrinting {
  override protected def pretty: Pretty[ContractStakeholdersAndReassignmentCounter] = prettyOfClass(
    param("stakeholders", _.stakeholders),
    param("reassignment counter", _.reassignmentCounter),
  )
}

object AcsChange {
  val empty: AcsChange = AcsChange(Map.empty, Map.empty)
}

/** Staged version of AcsChange, which requires reassignment counters for the archived contracts.
  * This AcsChange refers to one single Update, and should not contain transient contracts.
  */
sealed trait AcsChangeFactory {

  /** @return
    *   The non-transient archivals which for the reassignmentCounterForArchivals is needed
    */
  def archivalCids: Set[LfContractId]

  /** Building the final AcsChange
    *
    * @param reassignmentCounterForArchivals
    *   must have an entry for each archivalCids
    */
  def tryAcsChange(
      reassignmentCounterForArchivals: Map[LfContractId, ReassignmentCounter]
  )(implicit loggingContext: NamedLoggingContext): AcsChange

  /** Whether contractId is affected by this AcsChange. For transient contracts this returns false.
    */
  def contractActivenessChanged(contractId: LfContractId): Boolean
}

final case class AcsChangeFactoryImpl(
    initialAcsChange: AcsChange,
    archivalDeactivations: Map[LfContractId, Set[LfPartyId]],
) extends AcsChangeFactory
    with HasLoggerName {
  // This invariant is used in contractActivenessChanged computation
  assert(
    !initialAcsChange.activations.keysIterator.exists(archivalDeactivations.contains) &&
      !initialAcsChange.activations.keysIterator.exists(initialAcsChange.deactivations.contains),
    "AcsChange should not include transients",
  )

  override def archivalCids: Set[LfContractId] = archivalDeactivations.keySet

  override def tryAcsChange(
      reassignmentCounterForArchivals: Map[LfContractId, ReassignmentCounter]
  )(implicit loggingContext: NamedLoggingContext): AcsChange = {
    val enrichedArchivalDeactivations
        : Map[LfContractId, ContractStakeholdersAndReassignmentCounter] =
      archivalDeactivations.map { case (contractId, stakeholders) =>
        contractId -> ContractStakeholdersAndReassignmentCounter(
          stakeholders = stakeholders,
          reassignmentCounter = reassignmentCounterForArchivals.getOrElse(
            contractId,
            ErrorUtil.internalError(
              new IllegalStateException(
                s"contract ID $contractId not provided in reassignmentCounterForArchivals"
              )
            ),
          ),
        )
      }
    initialAcsChange.copy(
      deactivations = initialAcsChange.deactivations ++ enrichedArchivalDeactivations
    )
  }

  override def contractActivenessChanged(contractId: LfContractId): Boolean =
    initialAcsChange.activations.contains(contractId) ||
      initialAcsChange.deactivations.contains(contractId) ||
      archivalDeactivations.contains(contractId)
}

final case class TestAcsChangeFactory(
    contractActivenessChanged: Boolean = true
) extends AcsChangeFactory {
  override def archivalCids: Set[LfContractId] = Set.empty
  override def tryAcsChange(
      reassignmentCounterForArchivals: Map[LfContractId, ReassignmentCounter]
  )(implicit loggingContext: NamedLoggingContext): AcsChange = AcsChange.empty
  override def contractActivenessChanged(contractId: LfContractId): Boolean =
    contractActivenessChanged
}
