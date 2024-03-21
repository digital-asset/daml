// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.protocol.{ContractMetadata, LfContractId, WithContractHash}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

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
    activations: Map[LfContractId, WithContractHash[ContractMetadata]],
    deactivations: Map[LfContractId, WithContractHash[ContractStakeholders]],
)

final case class ContractStakeholders(
    stakeholders: Set[LfPartyId]
) extends PrettyPrinting {
  override def pretty: Pretty[ContractStakeholders] = prettyOfClass(
    param("stakeholders", _.stakeholders)
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

  /** Returns an AcsChange based on a given CommitSet.
    *
    * @param commitSet The commit set from which to build the AcsChange.
    */
  def fromCommitSet(
      commitSet: CommitSet
  )(implicit loggingContext: NamedLoggingContext): AcsChange = {

    val tmpActivations = commitSet.creations.map { case (contractId, data) =>
      (
        contractId,
        WithContractHash(data.unwrap.contractMetadata, data.contractHash),
      )
    }
      ++ commitSet.transferIns.map { case (contractId, data) =>
        (
          contractId,
          WithContractHash(data.unwrap.contractMetadata, data.contractHash),
        )
      }

    val tmpArchivals = commitSet.archivals.map { case (contractId, data) =>
      contractId -> WithContractHash(data.unwrap.stakeholders, data.contractHash)
    }

    /*
    Subtracting the transfer counter of transfer-outs to correctly match deactivated contracts as explained above
     */
    val tmpTransferOuts = commitSet.transferOuts.map { case (contractId, data) =>
      contractId -> data.map(_.stakeholders)
    }

    val transient = tmpActivations.keySet.intersect((tmpArchivals ++ tmpTransferOuts).keySet)
    val tmpActivationsClean = tmpActivations -- transient
    val tmpArchivalsClean = tmpArchivals -- transient
    val tmpTransferOutsClean = tmpTransferOuts -- transient

    val activations = tmpActivationsClean
    val archivalDeactivations = tmpArchivalsClean.map { case (contractId, metadata) =>
      (
        contractId,
        metadata.map(data => ContractStakeholders(data)),
      )
    }
    val transferOutDeactivations = tmpTransferOutsClean.map { case (contractId, metadata) =>
      (
        contractId,
        metadata.map(data => ContractStakeholders(data)),
      )
    }
    loggingContext.debug(
      show"Called fromCommitSet with inputs commitSet creations ${commitSet.creations}" +
        show"transferIns ${commitSet.transferIns} archivals ${commitSet.archivals} transferOuts ${commitSet.transferOuts} and" +
        show"Completed fromCommitSet with results transient $transient" +
        show"activations $activations archivalDeactivations $archivalDeactivations transferOutDeactivations $transferOutDeactivations"
    )
    AcsChange(
      activations = activations,
      deactivations = archivalDeactivations ++ transferOutDeactivations,
    )
  }
}
