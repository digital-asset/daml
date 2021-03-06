// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.keys

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlContractKey
import com.daml.ledger.participant.state.kvutils.committer.transaction.TransactionCommitter.damlContractKey
import com.daml.ledger.participant.state.kvutils.committer.transaction.keys.ContractKeysValidation.{
  Inconsistent,
  KeyValidationState,
  KeyValidationStatus,
  RawContractId,
}
import com.daml.lf.data.Ref.TypeConName
import com.daml.lf.transaction.{Node, NodeId}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

private[keys] object KeyConsistencyValidation {
  def checkNodeKeyConsistency(
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      node: Node.GenNode[NodeId, ContractId],
      keyValidationState: KeyValidationState,
  ): KeyValidationStatus =
    node match {
      case exercise: Node.NodeExercises[NodeId, ContractId] =>
        checkKeyConsistency(
          contractKeysToContractIds,
          exercise.key,
          Some(exercise.targetCoid),
          exercise.templateId,
          keyValidationState,
        )

      case create: Node.NodeCreate[ContractId] =>
        checkKeyConsistency(
          contractKeysToContractIds,
          create.key,
          None,
          create.templateId,
          keyValidationState,
        )

      case fetch: Node.NodeFetch[ContractId] =>
        checkKeyConsistency(
          contractKeysToContractIds,
          fetch.key,
          Some(fetch.coid),
          fetch.templateId,
          keyValidationState,
        )

      case lookupByKey: Node.NodeLookupByKey[ContractId] =>
        checkKeyConsistency(
          contractKeysToContractIds,
          Some(lookupByKey.key),
          lookupByKey.result,
          lookupByKey.templateId,
          keyValidationState,
        )
    }

  private def checkKeyConsistency(
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      key: Option[Node.KeyWithMaintainers[Value[ContractId]]],
      targetContractId: Option[ContractId],
      templateId: TypeConName,
      keyValidationState: KeyValidationState,
  ): KeyValidationStatus =
    key match {
      case None => Right(keyValidationState)
      case Some(submittedKeyWithMaintainers) =>
        val submittedDamlContractKey =
          damlContractKey(templateId, submittedKeyWithMaintainers.key)
        val newKeyValidationState =
          keyValidationState + ConsistencyKeyValidationState(
            submittedContractKeysToContractIds = Map(
              submittedDamlContractKey ->
                targetContractId.map(_.coid)
            )
          )
        checkKeyConsistency(
          contractKeysToContractIds,
          submittedDamlContractKey,
          newKeyValidationState,
        )
    }

  private def checkKeyConsistency(
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      submittedDamlContractKey: DamlContractKey,
      keyValidationState: KeyValidationState,
  ): KeyValidationStatus = {
    val actual: Option[RawContractId] = keyValidationState.submittedContractKeysToContractIds
      .get(
        submittedDamlContractKey
      )
      .flatten
    val expected = contractKeysToContractIds.get(submittedDamlContractKey)
    if (actual != expected) {
      Left(Inconsistent(submittedDamlContractKey.toString, actual, expected))
    } else
      Right(keyValidationState)
  }

  private def ConsistencyKeyValidationState(
      submittedContractKeysToContractIds: Map[DamlContractKey, Option[
        RawContractId
      ]]
  ): KeyValidationState =
    KeyValidationState(submittedContractKeysToContractIds)
}
