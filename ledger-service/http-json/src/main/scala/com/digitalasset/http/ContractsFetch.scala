// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{ContextShift, IO}
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.ledger.api.{v1 => lav1}

private class ContractsFetch(
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
) {

  import ContractsFetch._

  def fetchActiveContractsFromOffset(
      dao: Option[dbbackend.ContractDao],
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg)(
      implicit cs: ContextShift[IO]): IO[Seq[Contract]] =
    for {
      _ <- IO.shift(cs)
//      a <- dao
//      x = fetchActiveContractsFromOffset(???, party, templateId)
    } yield ???


  private def fetchActiveContractsFromOffset(
      jwt: Jwt,
      txFilter: TransactionFilter,
      offsetSource: Source[LedgerOffset, NotUsed]): Source[Transaction, NotUsed] =
    offsetSource.flatMapConcat(offset => getCreatesAndArchivesSince(jwt, txFilter, offset))

  private def ioToSource[Out](io: IO[Out]): Source[Out, NotUsed] =
    Source.fromFuture(io.unsafeToFuture())

  private def ledgerOffset(offset: String): LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Absolute(offset))
}

object ContractsFetch {
  type Contract = domain.Contract[lav1.value.Value]
}
