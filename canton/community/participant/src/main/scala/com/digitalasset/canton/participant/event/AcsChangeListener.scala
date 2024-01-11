// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.protocol.{ContractMetadata, LfContractId, WithContractHash}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPartyId, TransferCounterO}
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
    transferCounter: TransferCounterO,
) extends PrettyPrinting {
  override def pretty: Pretty[ContractMetadataAndTransferCounter] = prettyOfClass(
    param("contract metadata", _.contractMetadata),
    param("transfer counter", _.transferCounter),
  )
}

final case class ContractStakeholdersAndTransferCounter(
    stakeholders: Set[LfPartyId],
    transferCounter: TransferCounterO,
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
    * @param transferCounterOfArchivalIncomplete A map containing transfer counters for every contract in archived contracts
    *                                  in the commitSet, i.e., `commitSet.archivals`. If there are archived contracts
    *                                  that do not exist in the map, they are assumed to have transfer counter None.
    */
  def fromCommitSet(
      commitSet: CommitSet,
      transferCounterOfArchivalIncomplete: Map[LfContractId, TransferCounterO],
  )(implicit loggingContext: NamedLoggingContext): AcsChange = {

    val transferCounterOfArchival = commitSet.archivals.keySet
      .map(k => (k, transferCounterOfArchivalIncomplete.getOrElse(k, None)))
      .toMap
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

    val tmpArchivals = commitSet.archivals.map { case (contractId, data) =>
      (
        (
          contractId,
          // If the transfer counter for an archival is None, either the protocol version does not support
          // transfer counters, or the contract might be transient and created / transferred-in in
          // the same commit set. Thus we search in the commit set the latest transfer counter of the same contract.
          // transferCounterOfArchival.get(contractId).getOrElse(transferCounterTransient(contractId))
          transferCountersforArchivedCidInclTransient(
            contractId,
            commitSet,
            transferCounterOfArchival,
          ),
        ),
        WithContractHash(data.unwrap.stakeholders, data.contractHash),
      )
    }

    /*
    Subtracting the transfer counter of transfer-outs to correctly match deactivated contracts as explained above
     */
    val tmpTransferOuts = commitSet.transferOuts.map { case (contractId, data) =>
      (
        (
          contractId,
          data.unwrap.transferCounter.map(_ - 1),
        ),
        data.map(_.stakeholders),
      )
    }

    val transient = tmpActivations.keySet.intersect((tmpArchivals ++ tmpTransferOuts).keySet)
    val tmpActivationsClean = tmpActivations -- transient
    val tmpArchivalsClean = tmpArchivals -- transient
    val tmpTransferOutsClean = tmpTransferOuts -- transient

    val activations = tmpActivationsClean.map { case ((contractId, transferCounter), metadata) =>
      (
        contractId,
        metadata.map(data => ContractMetadataAndTransferCounter(data, transferCounter)),
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
      show"Called fromCommitSet with inputs commitSet creations ${commitSet.creations}" +
        show"transferIns ${commitSet.transferIns} archivals ${commitSet.archivals} transferOuts ${commitSet.transferOuts} and" +
        show"archival transfer counters from DB $transferCounterOfArchivalIncomplete" +
        show"Completed fromCommitSet with results transient $transient" +
        show"activations $activations archivalDeactivations $archivalDeactivations transferOutDeactivations $transferOutDeactivations"
    )
    AcsChange(
      activations = activations,
      deactivations = archivalDeactivations ++ transferOutDeactivations,
    )
  }

  @VisibleForTesting
  def transferCountersforArchivedCidInclTransient(
      contractId: LfContractId,
      commitSet: CommitSet,
      transferCounterOfArchival: Map[LfContractId, TransferCounterO],
  ): TransferCounterO = {
    transferCounterOfArchival.get(contractId) match {
      case Some(tc) if tc.isDefined => tc
      case _ =>
        // We first search in transfer-ins, because they would have the most recent transfer counter.
        commitSet.transferIns.get(contractId) match {
          case Some(tcAndContractHash) if tcAndContractHash.unwrap.transferCounter.isDefined =>
            tcAndContractHash.unwrap.transferCounter
          case _ =>
            // Then we search in creations
            commitSet.creations.get(contractId) match {
              case Some(tcAndCHash) if tcAndCHash.unwrap.transferCounter.isDefined =>
                tcAndCHash.unwrap.transferCounter
              case _ => None
            }
        }
    }
  }
}
