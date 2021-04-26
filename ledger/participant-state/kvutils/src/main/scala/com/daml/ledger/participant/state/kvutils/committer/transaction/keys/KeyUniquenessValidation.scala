// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.keys

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.daml.ledger.participant.state.kvutils.committer.transaction.TransactionCommitter.globalKey
import com.daml.ledger.participant.state.kvutils.committer.transaction.keys.ContractKeysValidation.{
  Duplicate,
  KeyValidationError,
}
import com.daml.lf.transaction.{Node, NodeId}
import com.daml.lf.value.Value.ContractId

private[keys] object KeyUniquenessValidation {

  def checkNodeKeyUniqueness(
      node: Node.GenNode[NodeId, ContractId],
      activeStateKeys: Set[DamlStateKey],
  ): Either[KeyValidationError, Set[DamlStateKey]] =
    node match {
      case exercise: Node.NodeExercises[NodeId, ContractId]
          if exercise.key.isDefined && exercise.consuming =>
        val stateKey =
          Conversions.globalKeyToStateKey(globalKey(exercise.templateId, exercise.key.get.key))
        Right(
          activeStateKeys - stateKey
        )

      case create: Node.NodeCreate[ContractId] if create.key.isDefined =>
        val stateKey =
          Conversions.globalKeyToStateKey(globalKey(create.coinst.template, create.key.get.key))

        if (activeStateKeys.contains(stateKey))
          Left(Duplicate)
        else
          Right(activeStateKeys + stateKey)

      case _ => Right(activeStateKeys)
    }
}
