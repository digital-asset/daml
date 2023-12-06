// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.Validated
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}

object ContractConsistencyChecker {

  /** Indicates that the given transaction uses a contract that has been created with a ledger time
    * after the ledger time of the transaction.
    */
  final case class ReferenceToFutureContractError(
      contractId: LfContractId,
      contractCreationTime: CantonTimestamp,
      transactionLedgerTime: CantonTimestamp,
  ) extends PrettyPrinting {
    override def pretty: Pretty[ReferenceToFutureContractError] = prettyOfString(self =>
      show"A request with ledger time ${self.transactionLedgerTime} uses a future contract (created at ${self.contractCreationTime}, id = ${self.contractId})"
    )
  }

  /** Checks that the provided contracts have a ledger time
    * no later than `ledgerTime`.
    */
  def assertInputContractsInPast(
      inputContracts: List[(LfContractId, SerializableContract)],
      ledgerTime: CantonTimestamp,
  ): Either[List[ReferenceToFutureContractError], Unit] =
    inputContracts
      .traverse_ { case (coid, contract) =>
        val let = contract.ledgerCreateTime.ts
        Validated.condNec(
          let <= ledgerTime,
          (),
          ReferenceToFutureContractError(coid, let, ledgerTime),
        )
      }
      .toEither
      .leftMap(_.toList)
}
