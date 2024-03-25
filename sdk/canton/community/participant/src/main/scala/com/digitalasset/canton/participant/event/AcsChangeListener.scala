// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.protocol.{ContractMetadata, LfContractId, WithContractHash}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPartyId, TransferCounter}
import com.google.common.annotations.VisibleForTesting

/** Components that need to keep a running snapshot of ACS.
  */
trait AcsChangeListener {

  /** ACS change notification. Any response logic needs to happen in the background. The ACS change set may be empty,
    * (e.g., in case of time proofs).
    *
    * @param toc time of the change
    * @param acsChange active contract set change descriptor
    */
  def publish(toc: RecordTime, acsChange: AcsChange)(implicit traceContext: TraceContext): Unit

}

/** Represents a change to the ACS. The deactivated contracts are accompanied by their stakeholders.
  *
  * Note that we include both the LfContractId (for uniqueness) and the LfHash (reflecting contract content).
  */
final case class AcsChange(
    activations: Map[LfContractId, WithContractHash[ContractMetadataAndTransferCounter]],
    deactivations: Map[LfContractId, WithContractHash[ContractStakeholdersAndTransferCounter]],
)

final case class ContractMetadataAndTransferCounter(
    contractMetadata: ContractMetadata,
    transferCounter: TransferCounter,
) extends PrettyPrinting {
  override def pretty: Pretty[ContractMetadataAndTransferCounter] = prettyOfClass(
    param("contract metadata", _.contractMetadata),
    param("transfer counter", _.transferCounter),
  )
}

final case class ContractStakeholdersAndTransferCounter(
    stakeholders: Set[LfPartyId],
    transferCounter: TransferCounter,
) extends PrettyPrinting {
  override def pretty: Pretty[ContractStakeholdersAndTransferCounter] = prettyOfClass(
    param("stakeholders", _.stakeholders),
    param("transfer counter", _.transferCounter),
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
  //  (2) The transfer counters of the contracts in activations/deactivations are consistent, in the sense that
  //    - all transfer counters are None for protocol versions that do not support reassignments
  //    - for protocol versions that support transfer counters
  //        - transfer counters of creates are 0
  //        - transfer counters of transfer-outs increment the last activation (transfer-in or create) counter
  //        - transfer counters of archivals match the transfer counter of the last contract activation (transfer-in or create),
  //      unless the activation is part of the same CommitSet. If the activation is part of the same
  //      commit set as the archival, the function `fromCommitSet` assigns the correct transfer counter to archivals

  /** Returns an AcsChange based on a given CommitSet.
    *
    * @param commitSet The commit set from which to build the AcsChange.
    * @param transferCounterOfNonTransientArchivals A map containing transfer counters for every non-transient
    *                                               archived contracts in the commitSet, i.e., `commitSet.archivals`.
    * @param transferCounterOfTransientArchivals A map containing transfer counters for every transient
    *                                                archived contracts in the commitSet, i.e., `commitSet.archivals`.
    * @throws java.lang.IllegalStateException if the contract ids in `transferCounterOfTransientArchivals`;
    *         if the contract ids in `transferCounterOfNonTransientArchivals` are not a subset of `commitSet.archivals` ;
    *         if the union of contracts ids in `transferCounterOfTransientArchivals` and
    *         `transferCounterOfNonTransientArchivals` does not equal the contract ids in `commitSet.archivals`;
    */
  def tryFromCommitSet(
      commitSet: CommitSet,
      transferCounterOfNonTransientArchivals: Map[LfContractId, TransferCounter],
      transferCounterOfTransientArchivals: Map[LfContractId, TransferCounter],
  )(implicit loggingContext: NamedLoggingContext): AcsChange = {

    if (
      transferCounterOfTransientArchivals.keySet.union(
        transferCounterOfNonTransientArchivals.keySet
      ) != commitSet.archivals.keySet
    ) {
      ErrorUtil.internalError(
        new IllegalStateException(
          s"the union of contracts ids in $transferCounterOfTransientArchivals and " +
            s"$transferCounterOfNonTransientArchivals does not equal the contract ids in ${commitSet.archivals}"
        )
      )
    }
    /* Temporary maps built to easily remove the transient contracts from activate and deactivate the common contracts.
       The keys are made of the contract id and transfer counter.
       A transfer-out with transfer counter c cancels out a transfer-in / create with transfer counter c-1.
       Thus, to be able to match active contracts that are being deactivated, we decrement the transfer counters for transfer-outs.
       We *do not* need to decrement the transfer counter for archives, because we already obtain each archival's
       transfer counter from the last create / transfer-in event on that contract.
     */
    val tmpActivations = commitSet.creations.map { case (contractId, data) =>
      (
        (contractId, data.unwrap.transferCounter),
        WithContractHash(data.unwrap.contractMetadata, data.contractHash),
      )
    }
      ++ commitSet.transferIns.map { case (contractId, data) =>
        (
          (contractId, data.unwrap.transferCounter),
          WithContractHash(data.unwrap.contractMetadata, data.contractHash),
        )
      }

    /*
    Subtracting the transfer counter of transfer-outs to correctly match deactivated contracts as explained above
     */
    val tmpTransferOuts = commitSet.transferOuts.map { case (contractId, data) =>
      (
        (
          contractId,
          data.unwrap.transferCounter - 1,
        ),
        data.map(_.stakeholders),
      )
    }

    val tmpArchivalsClean = commitSet.archivals.collect {
      case (contractId, data) if transferCounterOfNonTransientArchivals.contains(contractId) =>
        (
          (
            contractId,
            transferCounterOfNonTransientArchivals.getOrElse(
              contractId,
              // This should not happen (see assertion above)
              ErrorUtil.internalError(
                new IllegalStateException(s"Unable to find transfer counter for $contractId")
              ),
            ),
          ),
          WithContractHash(data.unwrap.stakeholders, data.contractHash),
        )
    }

    val tmpArchivals = commitSet.archivals.collect {
      case (contractId, data) if transferCounterOfTransientArchivals.contains(contractId) =>
        (
          (
            contractId,
            transferCounterOfTransientArchivals.getOrElse(
              contractId,
              ErrorUtil.internalError(
                new IllegalStateException(
                  s"${transferCounterOfTransientArchivals.keySet} is not a subset of ${commitSet.archivals}"
                )
              ),
            ),
          ),
          WithContractHash(data.unwrap.stakeholders, data.contractHash),
        )
    }

    val transient = tmpActivations.keySet.intersect((tmpArchivals ++ tmpTransferOuts).keySet)
    val tmpActivationsClean = tmpActivations -- transient
    val tmpTransferOutsClean = tmpTransferOuts -- transient

    val activations = tmpActivationsClean.map { case ((contractId, transferCounter), metadata) =>
      (
        contractId,
        metadata.map(data =>
          ContractMetadataAndTransferCounter(
            data,
            transferCounter,
          )
        ),
      )
    }
    val archivalDeactivations = tmpArchivalsClean.map {
      case ((contractId, transferCounter), metadata) =>
        (
          contractId,
          metadata.map(data => ContractStakeholdersAndTransferCounter(data, transferCounter)),
        )
    }
    val transferOutDeactivations = tmpTransferOutsClean.map {
      case ((contractId, transferCounter), metadata) =>
        (
          contractId,
          metadata.map(data => ContractStakeholdersAndTransferCounter(data, transferCounter)),
        )
    }
    loggingContext.debug(
      show"Called fromCommitSet with inputs commitSet creations=${commitSet.creations};" +
        show"transferIns=${commitSet.transferIns}; archivals=${commitSet.archivals}; transferOuts=${commitSet.transferOuts};" +
        show"archival transfer counters from DB $transferCounterOfNonTransientArchivals and" +
        show"archival transfer counters from transient $transferCounterOfTransientArchivals" +
        show"Completed fromCommitSet with results transient=$transient;" +
        show"activations=$activations; archivalDeactivations=$archivalDeactivations; transferOutDeactivations=$transferOutDeactivations"
    )
    AcsChange(
      activations = activations,
      deactivations = archivalDeactivations ++ transferOutDeactivations,
    )
  }

  @VisibleForTesting
  def transferCountersForArchivedTransient(
      commitSet: CommitSet
  ): Map[LfContractId, TransferCounter] = {

    // We first search in transfer-ins, because they would have the most recent transfer counter.
    val transientCidsTransferredIn = commitSet.transferIns.collect {
      case (contractId, tcAndContractHash) if commitSet.archivals.keySet.contains(contractId) =>
        (contractId, tcAndContractHash.unwrap.transferCounter)
    }

    // Then we search in creations
    val transientCidsCreated = commitSet.creations.collect {
      case (contractId, tcAndContractHash)
          if commitSet.archivals.keySet.contains(contractId) && !transientCidsTransferredIn.keySet
            .contains(contractId) =>
        (contractId, tcAndContractHash.unwrap.transferCounter)
    }

    transientCidsTransferredIn ++ transientCidsCreated
  }
}
