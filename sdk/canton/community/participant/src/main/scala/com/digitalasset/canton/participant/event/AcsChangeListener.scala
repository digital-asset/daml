// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.Future

/** Components that need to keep a running snapshot of ACS.
  */
trait AcsChangeListener {

  /** ACS change notification. Any response logic needs to happen in the background. The ACS change set may be empty,
    * (e.g., in case of time proofs).
    *
    * @param toc time of the change
    * @param acsChange active contract set change descriptor
    * @param waitFor processing won't start until this Future completes
    */
  def publish(toc: RecordTime, acsChange: AcsChange, waitFor: Future[Unit])(implicit
      traceContext: TraceContext
  ): Unit

}

/** Represents a change to the ACS. The deactivated contracts are accompanied by their stakeholders.
  *
  * Note that we include the LfContractId (for uniqueness), but we do not include the contract hash because
  * it already authenticates the contract contents.
  */
final case class AcsChange(
    activations: Map[LfContractId, ContractStakeholdersAndReassignmentCounter],
    deactivations: Map[LfContractId, ContractStakeholdersAndReassignmentCounter],
)

final case class ContractStakeholdersAndReassignmentCounter(
    stakeholders: Set[LfPartyId],
    reassignmentCounter: ReassignmentCounter,
) extends PrettyPrinting {
  override def pretty: Pretty[ContractStakeholdersAndReassignmentCounter] = prettyOfClass(
    param("stakeholders", _.stakeholders),
    param("reassignment counter", _.reassignmentCounter),
  )
}

object AcsChange extends HasLoggerName {
  val empty: AcsChange = AcsChange(Map.empty, Map.empty)

  // TODO(i12904) The ACS commitments processor expects the caller to ensure that:
  //  (1) The activations/deactivations passed to it really describe a set of contracts. Double activations or double deactivations for a contract (due to a bug
  //  or maliciousness) will violate this expectation.
  //  Examples of malicious cases we need to handle:
  //    1. archival without a prior create
  //    2. archival followed by a create
  //    3. if we have a double archive as in "create -> archive -> archive"
  //  We should define a sensible semantics for non-repudiation in all such cases.
  //  (2) The reassignment counters of the contracts in activations/deactivations are consistent, in the sense that
  //    - all reassignment counters are None for protocol versions that do not support reassignments
  //    - for protocol versions that support reassignment counters
  //        - reassignment counters of creates are 0
  //        - reassignment counters of unassignments increment the last activation (assignment or create) counter
  //        - reassignment counters of archivals match the reassignment counter of the last contract activation (assignment or create),
  //      unless the activation is part of the same CommitSet. If the activation is part of the same
  //      commit set as the archival, the function `fromCommitSet` assigns the correct reassignment counter to archivals

  /** Returns an AcsChange based on a given CommitSet.
    *
    * @param commitSet The commit set from which to build the AcsChange.
    * @param reassignmentCounterOfNonTransientArchivals A map containing reassignment counters for every non-transient
    *                                               archived contracts in the commitSet, i.e., `commitSet.archivals`.
    * @param reassignmentCounterOfTransientArchivals A map containing reassignment counters for every transient
    *                                                archived contracts in the commitSet, i.e., `commitSet.archivals`.
    * @throws java.lang.IllegalStateException if the contract ids in `reassignmentCounterOfTransientArchivals`;
    *         if the contract ids in `reassignmentCounterOfNonTransientArchivals` are not a subset of `commitSet.archivals` ;
    *         if the union of contracts ids in `reassignmentCounterOfTransientArchivals` and
    *         `reassignmentCounterOfNonTransientArchivals` does not equal the contract ids in `commitSet.archivals`;
    */
  def tryFromCommitSet(
      commitSet: CommitSet,
      reassignmentCounterOfNonTransientArchivals: Map[LfContractId, ReassignmentCounter],
      reassignmentCounterOfTransientArchivals: Map[LfContractId, ReassignmentCounter],
  )(implicit loggingContext: NamedLoggingContext): AcsChange = {

    if (
      reassignmentCounterOfTransientArchivals.keySet.union(
        reassignmentCounterOfNonTransientArchivals.keySet
      ) != commitSet.archivals.keySet
    ) {
      ErrorUtil.internalError(
        new IllegalStateException(
          s"the union of contracts ids in $reassignmentCounterOfTransientArchivals and " +
            s"$reassignmentCounterOfNonTransientArchivals does not equal the contract ids in ${commitSet.archivals}"
        )
      )
    }
    /* Temporary maps built to easily remove the transient contracts from activate and deactivate the common contracts.
       The keys are made of the contract id and reassignment counter.
       An unassignment with reassignment counter c cancels out an assignment / create with reassignment counter c-1.
       Thus, to be able to match active contracts that are being deactivated, we decrement the reassignment counters for unassignments.
       We *do not* need to decrement the transfer counter for archives, because we already obtain each archival's
       reassignment counter from the last create / assign event on that contract.
     */
    val tmpActivations = commitSet.creations.map { case (contractId, data) =>
      contractId -> ContractStakeholdersAndReassignmentCounter(
        data.contractMetadata.stakeholders,
        data.reassignmentCounter,
      )
    }
      ++ commitSet.assignments.map { case (contractId, data) =>
        contractId -> ContractStakeholdersAndReassignmentCounter(
          data.contractMetadata.stakeholders,
          data.reassignmentCounter,
        )
      }

    /*
    Subtracting the reassignment counter of unassignments to correctly match deactivated contracts as explained above
     */
    val tmpUnassignments = commitSet.unassignments.map { case (contractId, data) =>
      contractId -> ContractStakeholdersAndReassignmentCounter(
        data.stakeholders,
        data.reassignmentCounter - 1,
      )
    }

    val archivalDeactivations = commitSet.archivals.collect {
      case (contractId, data) if reassignmentCounterOfNonTransientArchivals.contains(contractId) =>
        contractId -> ContractStakeholdersAndReassignmentCounter(
          data.stakeholders,
          reassignmentCounterOfNonTransientArchivals.getOrElse(
            contractId,
            // This should not happen (see assertion above)
            ErrorUtil.internalError(
              new IllegalStateException(s"Unable to find reassignment counter for $contractId")
            ),
          ),
        )
    }

    val tmpArchivals = commitSet.archivals.collect {
      case (contractId, data) if reassignmentCounterOfTransientArchivals.contains(contractId) =>
        contractId -> ContractStakeholdersAndReassignmentCounter(
          data.stakeholders,
          reassignmentCounterOfTransientArchivals.getOrElse(
            contractId,
            ErrorUtil.internalError(
              new IllegalStateException(
                s"${reassignmentCounterOfTransientArchivals.keySet} is not a subset of ${commitSet.archivals}"
              )
            ),
          ),
        )
    }

    val transient = tmpActivations.keySet.intersect((tmpArchivals ++ tmpUnassignments).keySet)
    val activations = tmpActivations -- transient
    val unassignmentDeactivations = tmpUnassignments -- transient

    loggingContext.debug(
      show"Called fromCommitSet with inputs commitSet creations=${commitSet.creations};" +
        show"assignments=${commitSet.assignments}; archivals=${commitSet.archivals}; unassignments=${commitSet.unassignments};" +
        show"archival reassignment counters from DB $reassignmentCounterOfNonTransientArchivals and" +
        show"archival reassignment counters from transient $reassignmentCounterOfTransientArchivals" +
        show"Completed fromCommitSet with results transient=$transient;" +
        show"activations=$activations; archivalDeactivations=$archivalDeactivations; unassignmentDeactivations=$unassignmentDeactivations"
    )
    AcsChange(
      activations = activations,
      deactivations = archivalDeactivations ++ unassignmentDeactivations,
    )
  }

  @VisibleForTesting
  def reassignmentCountersForArchivedTransient(
      commitSet: CommitSet
  ): Map[LfContractId, ReassignmentCounter] = {

    // We first search in assignments, because they would have the most recent reassignment counter.
    val transientCidsTransferredIn = commitSet.assignments.collect {
      case (contractId, tcAndContractHash) if commitSet.archivals.keySet.contains(contractId) =>
        (contractId, tcAndContractHash.reassignmentCounter)
    }

    // Then we search in creations
    val transientCidsCreated = commitSet.creations.collect {
      case (contractId, tcAndContractHash)
          if commitSet.archivals.keySet.contains(contractId) && !transientCidsTransferredIn.keySet
            .contains(contractId) =>
        (contractId, tcAndContractHash.reassignmentCounter)
    }

    transientCidsTransferredIn ++ transientCidsCreated
  }
}
