// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.keys

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlContractKey
import com.daml.ledger.participant.state.kvutils.committer.transaction.keys.ContractKeysValidation.{
  Inconsistent,
  KeyValidationError,
  RawContractId,
}
import com.daml.lf.data.Ref.TypeConName
import com.daml.lf.transaction.{Node, NodeId}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

private[keys] object KeyConsistencyValidation {
  type KeyConsistencyValidationStatus =
    Either[KeyValidationError, Map[DamlContractKey, Option[RawContractId]]]

  def checkNodeKeyConsistency(
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      node: Node.GenActionNode[NodeId, ContractId],
      submittedContractKeysToContractIds: Map[DamlContractKey, Option[RawContractId]],
  ): KeyConsistencyValidationStatus =
    node match {
      case exercise: Node.NodeExercises[NodeId, ContractId] =>
        checkKeyConsistency(
          contractKeysToContractIds,
          exercise.key,
          Some(exercise.targetCoid),
          exercise.templateId,
          submittedContractKeysToContractIds,
        )

      case create: Node.NodeCreate[ContractId] =>
        checkKeyConsistency(
          contractKeysToContractIds,
          create.key,
          None,
          create.templateId,
          submittedContractKeysToContractIds,
        )

      case fetch: Node.NodeFetch[ContractId] =>
        checkKeyConsistency(
          contractKeysToContractIds,
          fetch.key,
          Some(fetch.coid),
          fetch.templateId,
          submittedContractKeysToContractIds,
        )

      case lookupByKey: Node.NodeLookupByKey[ContractId] =>
        checkKeyConsistency(
          contractKeysToContractIds,
          Some(lookupByKey.key),
          lookupByKey.result,
          lookupByKey.templateId,
          submittedContractKeysToContractIds,
        )
    }

  private def checkKeyConsistency(
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      key: Option[Node.KeyWithMaintainers[Value[ContractId]]],
      targetContractId: Option[ContractId],
      templateId: TypeConName,
      submittedContractKeysToContractIds: Map[DamlContractKey, Option[RawContractId]],
  ): KeyConsistencyValidationStatus =
    key match {
      case None => Right(submittedContractKeysToContractIds)
      case Some(submittedKeyWithMaintainers) =>
        val submittedDamlContractKey =
          Conversions.encodeContractKey(templateId, submittedKeyWithMaintainers.key)
        val newSubmittedContractKeysToContractIds =
          if (submittedContractKeysToContractIds.contains(submittedDamlContractKey)) {
            submittedContractKeysToContractIds
          } else {
            submittedContractKeysToContractIds.updated(
              submittedDamlContractKey,
              targetContractId.map(_.coid),
            )
          }
        checkKeyConsistency(
          contractKeysToContractIds,
          submittedDamlContractKey,
          newSubmittedContractKeysToContractIds,
        )
    }

  private def checkKeyConsistency(
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      submittedDamlContractKey: DamlContractKey,
      submittedContractKeysToContractIds: Map[DamlContractKey, Option[RawContractId]],
  ): KeyConsistencyValidationStatus =
    if (
      submittedContractKeysToContractIds
        .get(
          submittedDamlContractKey
        )
        .flatten != contractKeysToContractIds.get(submittedDamlContractKey)
    )
      Left(Inconsistent)
    else
      Right(submittedContractKeysToContractIds)
}
