// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  AcsChangeFactory,
  AcsChangeFactoryImpl,
  ContractStakeholdersAndReassignmentCounter,
}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

/** Components that need to keep a running snapshot of ACS.
  */
trait AcsChangeListener {

  /** ACS change notification. Any response logic needs to happen in the background. The ACS change
    * set may be empty, (e.g., in case of time proofs).
    *
    * @param toc
    *   time of the change
    * @param acsChange
    *   active contract set change descriptor
    */
  def publish(toc: RecordTime, acsChange: AcsChange)(implicit
      traceContext: TraceContext
  ): Unit

  def publish(toc: RecordTime, acsChangeFactoryO: Option[AcsChangeFactory])(implicit
      traceContext: TraceContext
  ): Unit

}

object AcsChangeSupport extends HasLoggerName {
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
    * @param commitSet
    *   The commit set from which to build the AcsChangeFactory.
    */
  def fromCommitSet(
      commitSet: CommitSet
  )(implicit loggingContext: NamedLoggingContext): AcsChangeFactory = {
    /* Temporary maps built to easily remove the transient contracts from activate and deactivate the common contracts.
       The keys are made of the contract id and reassignment counter.
       An unassignment with reassignment counter c cancels out an assignment / create with reassignment counter c-1.
       Thus, to be able to match active contracts that are being deactivated, we decrement the reassignment counters for unassignments.
       We *do not* need to decrement the reassignment counter for archives, because we already obtain each archival's
       reassignment counter from the last create / assign event on that contract.
     */
    val tmpActivations = commitSet.creations.map { case (contractId, data) =>
      contractId -> ContractStakeholdersAndReassignmentCounter(
        data.contractMetadata.stakeholders,
        data.reassignmentCounter,
      )
    } ++ commitSet.assignments.map { case (contractId, data) =>
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

    val tmpArchivals = commitSet.archivals.collect { case (contractId, data) =>
      contractId -> data.stakeholders
    }

    val transient = tmpActivations.keySet.intersect((tmpArchivals ++ tmpUnassignments).keySet)
    val activations = tmpActivations -- transient
    val unassignmentDeactivations = tmpUnassignments -- transient
    val archivalDeactivations = tmpArchivals -- transient

    loggingContext.debug(
      show"Called fromCommitSet with inputs commitSet creations=${commitSet.creations};" +
        show"assignments=${commitSet.assignments}; archivals=${commitSet.archivals}; unassignments=${commitSet.unassignments};" +
        show"Completed fromCommitSet with results transient=$transient;" +
        show"activations=$activations; archivalDeactivations=$archivalDeactivations; unassignmentDeactivations=$unassignmentDeactivations"
    )
    val acsChange = AcsChange(
      activations = activations,
      deactivations = unassignmentDeactivations,
    )
    AcsChangeFactoryImpl(
      initialAcsChange = acsChange,
      archivalDeactivations = archivalDeactivations,
    )
  }
}
