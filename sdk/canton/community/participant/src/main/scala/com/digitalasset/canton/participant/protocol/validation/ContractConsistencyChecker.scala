// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.Validated
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{GenContractInstance, LfContractId}
import com.digitalasset.daml.lf.transaction.CreationTime

object ContractConsistencyChecker {

  /** Indicates that the given transaction uses a contract that has been created with a ledger time
    * after the ledger time of the transaction.
    */
  final case class ReferenceToFutureContractError(
      contractId: LfContractId,
      contractCreationTime: CantonTimestamp,
      transactionLedgerTime: CantonTimestamp,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[ReferenceToFutureContractError] = prettyOfString(self =>
      show"A request with ledger time ${self.transactionLedgerTime} uses a future contract (created at ${self.contractCreationTime}, id = ${self.contractId})"
    )
  }

  /** Checks that the provided contracts have a ledger time no later than `ledgerTime`.
    */
  def assertInputContractsInPast(
      inputContracts: List[(LfContractId, GenContractInstance)],
      ledgerTime: CantonTimestamp,
  ): Either[List[ReferenceToFutureContractError], Unit] =
    inputContracts
      .traverse_ { case (coid, contract) =>
        // The upcast to CreationTime works around https://github.com/scala/bug/issues/9837
        (contract.inst.createdAt: CreationTime) match {
          case CreationTime.CreatedAt(let) =>
            val createdAt = CantonTimestamp(let)
            Validated.condNec(
              createdAt <= ledgerTime,
              (),
              ReferenceToFutureContractError(coid, createdAt, ledgerTime),
            )
          case CreationTime.Now => Validated.validNec(())
        }
      }
      .toEither
      .leftMap(_.toList)
}
