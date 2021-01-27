// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlContractKey, DamlStateKey}

private[transaction] object KeyValidation {

  sealed trait KeyValidationError
  case object Duplicate extends KeyValidationError
  case object Inconsistent extends KeyValidationError

  final case class KeyValidationState private[KeyValidation] (
      activeStateKeys: Set[DamlStateKey],
      submittedContractKeysToContractIds: Map[DamlContractKey, Option[
        RawContractId
      ]],
  ) {
    def +(state: KeyValidationState): KeyValidationState = {
      val newContractKeyMappings =
        state.submittedContractKeysToContractIds -- submittedContractKeysToContractIds.keySet
      KeyValidationState(
        activeStateKeys = state.activeStateKeys,
        submittedContractKeysToContractIds =
          submittedContractKeysToContractIds ++ newContractKeyMappings,
      )
    }
  }

  def UniquenessKeyValidationState(activeStateKeys: Set[DamlStateKey]): KeyValidationState =
    KeyValidationState(activeStateKeys, Map.empty)

  object UniquenessKeyValidationState {
    def unapply(state: KeyValidationState): Option[Set[DamlStateKey]] =
      Some(state.activeStateKeys)
  }

  def ConsistencyKeyValidationState(
      submittedContractKeysToContractIds: Map[DamlContractKey, Option[
        RawContractId
      ]]
  ): KeyValidationState =
    KeyValidationState(Set.empty, submittedContractKeysToContractIds)

  type KeyValidationStatus =
    Either[KeyValidationError, KeyValidationState]
}
