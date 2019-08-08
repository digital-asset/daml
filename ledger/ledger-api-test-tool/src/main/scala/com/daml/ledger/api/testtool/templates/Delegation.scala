// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.templates

import com.daml.ledger.api.testtool.infrastructure.LedgerTestContext
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.v1.value.Value.Sum.{ContractId, Optional, Party, Text}
import com.digitalasset.platform.participant.util.ValueConversions._

import scala.concurrent.Future

object Delegation {
  def apply(owner: String, delegate: String)(
      implicit context: LedgerTestContext): Future[Delegation] =
    context
      .create(owner, ids.delegation, Map("owner" -> Party(owner), "delegate" -> Party(delegate)))
      .map(new Delegation(_, owner, delegate) {})
}
sealed abstract case class Delegation(contractId: String, owner: String, delegate: String) {
  def fetchDelegated(delegatedId: String)(implicit context: LedgerTestContext): Future[Unit] =
    context.exercise(
      delegate,
      ids.delegation,
      contractId,
      "FetchDelegated",
      Map("delegated" -> ContractId(delegatedId)))

  def fetchByKeyDelegated(key: String, expected: Option[String])(
      implicit context: LedgerTestContext): Future[Unit] =
    context.exercise(
      delegate,
      ids.delegation,
      contractId,
      "FetchByKeyDelegated",
      Map[String, value.Value.Sum](
        "p" -> Party(owner),
        "k" -> Text(key),
        "expected" -> Optional(value.Optional(expected.map(_.asContractId))),
      )
    )

  def lookupByKeyDelegated(key: String, expected: Option[String])(
      implicit context: LedgerTestContext): Future[Unit] =
    context.exercise(
      delegate,
      ids.delegation,
      contractId,
      "FetchByKeyDelegated",
      Map[String, value.Value.Sum](
        "p" -> Party(owner),
        "k" -> Text(key),
        "expected" -> Optional(value.Optional(expected.map(_.asContractId))),
      )
    )

}
