// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlContractKey, DamlStateKey}
import com.daml.lf.value.Value.ContractId

private[transaction] object KeyValidation {
  sealed trait KeyValidationError
  case object Duplicate extends KeyValidationError
  case object Inconsistent extends KeyValidationError

  case class KeyValidationState(
      activeDamlStateKeys: Set[DamlStateKey],
      submittedDamlContractKeysToContractIds: Map[DamlContractKey, Option[RawContractId]] =
        Map.empty,
  ) {
    def addSubmittedDamlContractKeyToContractIdIfEmpty(
        submittedDamlContractKey: DamlContractKey,
        contractId: Option[ContractId],
    ): KeyValidationState =
      copy(
        submittedDamlContractKeysToContractIds =
          if (!submittedDamlContractKeysToContractIds.contains(submittedDamlContractKey)) {
            submittedDamlContractKeysToContractIds + (submittedDamlContractKey -> contractId
              .map(
                _.coid
              ))
          } else {
            submittedDamlContractKeysToContractIds
          }
      )
  }

  type KeyValidationStatus =
    Either[KeyValidationError, KeyValidationState]
}
