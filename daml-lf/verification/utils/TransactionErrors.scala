// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

import Value.ContractId

object TransactionErrors {

  final case class DuplicateContractId(contractId: ContractId)
  final case class DuplicateContractKey(key: GlobalKey)
  final case class InconsistentContractKey(key: GlobalKey)

  sealed trait TransactionError
  final case class DuplicateContractIdTxError(duplicateContractId: DuplicateContractId)
      extends TransactionError
  final case class DuplicateContractKeyTxError(duplicateContractKey: DuplicateContractKey)
      extends TransactionError

  object TransactionError {
    def inject(error: DuplicateContractId): TransactionError =
      DuplicateContractIdTxError(error)

    def inject(error: DuplicateContractKey): TransactionError =
      DuplicateContractKeyTxError(error)

    def from(error: CreateError): TransactionError = error match {
      case DuplicateContractIdCreateError(e) => inject(e)
      case DuplicateContractKeyCreateError(e) => inject(e)
    }
  }

  sealed trait KeyInputError
  final case class DuplicateContractIdKIError(duplicateContractId: DuplicateContractId)
      extends KeyInputError
  final case class DuplicateContractKeyKIError(duplicateContractKey: DuplicateContractKey)
      extends KeyInputError
  final case class InconsistentContractKeyKIError(inconsistentContractKey: InconsistentContractKey)
      extends KeyInputError

  object KeyInputError {
    def inject(error: DuplicateContractId): KeyInputError =
      DuplicateContractIdKIError(error)

    def inject(error: DuplicateContractKey): KeyInputError =
      DuplicateContractKeyKIError(error)

    def inject(error: InconsistentContractKey): KeyInputError =
      InconsistentContractKeyKIError(error)

    def from(error: CreateError): KeyInputError = error match {
      case DuplicateContractIdCreateError(e) => inject(e)
      case DuplicateContractKeyCreateError(e) => inject(e)
    }
  }

  sealed trait CreateError
  final case class DuplicateContractIdCreateError(duplicateContractId: DuplicateContractId)
      extends CreateError
  final case class DuplicateContractKeyCreateError(duplicateContractKey: DuplicateContractKey)
      extends CreateError

  object CreateError {
    def inject(error: DuplicateContractId): CreateError =
      DuplicateContractIdCreateError(error)

    def inject(error: DuplicateContractKey): CreateError =
      DuplicateContractKeyCreateError(error)
  }
}
