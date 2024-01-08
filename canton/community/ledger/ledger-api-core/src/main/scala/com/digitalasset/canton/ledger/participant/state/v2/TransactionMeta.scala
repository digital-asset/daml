// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2

import com.daml.lf.crypto
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.NodeId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.DomainId

/** Meta-data of a transaction visible to all parties that can see a part of
  * the transaction.
  *
  * @param ledgerEffectiveTime: the submitter-provided time at which the
  *   transaction should be interpreted. This is the time returned by the
  *   Daml interpreter on a `getTime :: Update Time` call. See the docs on
  *   [[WriteService.submitTransaction]] for how it relates to the notion of
  *   `recordTime`.
  *
  * @param workflowId: a submitter-provided identifier used for monitoring
  *   and to traffic-shape the work handled by Daml applications
  *   communicating over the ledger.
  *
  * @param submissionTime: the transaction submission time
  *
  * @param submissionSeed: the seed used to derive the transaction contract IDs.
  *
  * @param optUsedPackages: the set of package IDs the transaction is depending on.
  *   Undefined means 'not known'.
  *
  * @param optNodeSeeds: an association list that maps to each ID if create and exercise nodes
  *   its respective seed. Undefined is not known.
  *
  * @param optByKeyNodes: the list of each ID of the fetch and exercise nodes that correspond
  *   to a fetch-by-key, lookup-by-key, or exercise-by-key command. Undefined is not known.
  */
final case class TransactionMeta(
    ledgerEffectiveTime: Time.Timestamp,
    workflowId: Option[Ref.WorkflowId],
    submissionTime: Time.Timestamp,
    submissionSeed: crypto.Hash,
    optUsedPackages: Option[Set[Ref.PackageId]],
    optNodeSeeds: Option[ImmArray[(NodeId, crypto.Hash)]],
    optByKeyNodes: Option[ImmArray[NodeId]],
    optDomainId: Option[
      DomainId
    ], // TODO(#13173) None for backwards compatibility, expected to be set for X nodes
) extends PrettyPrinting {
  override def pretty: Pretty[TransactionMeta.this.type] = prettyOfClass(
    param("ledgerEffectiveTime", _.ledgerEffectiveTime),
    paramIfDefined("workflowId", _.workflowId),
    param("submissionTime", _.submissionTime),
    paramIfDefined("domainId", _.optDomainId),
    indicateOmittedFields,
  )
}
