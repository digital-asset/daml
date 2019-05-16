// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import anorm.ToStatement
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.{ApplicationId, CommandId, EventId, WorkflowId}

object Convertion {

  implicit def subStringToStatement[T <: String]: ToStatement[T] =
    ToStatement.stringToStatement.asInstanceOf[ToStatement[T]]

  @throws[IllegalArgumentException]
  def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  @throws[IllegalArgumentException]
  def toContractId(s: String): Ref.ContractId = Ref.LedgerString.assertFromString(s)

  @throws[IllegalArgumentException]
  def toTransactionId(s: String): Ref.TransactionId = Ref.LedgerString.assertFromString(s)

  @throws[IllegalArgumentException]
  def toLedgerId(s: String): Ref.LedgerId = Ref.LedgerString.assertFromString(s)

  @throws[IllegalArgumentException]
  def toEventId(s: String): EventId = Ref.LedgerString.assertFromString(s)

  @throws[IllegalArgumentException]
  def toApplicationId(s: String): ApplicationId = Ref.LedgerString.assertFromString(s)

  @throws[IllegalArgumentException]
  def toCommandId(s: String): CommandId = Ref.LedgerString.assertFromString(s)

  @throws[IllegalArgumentException]
  def toWorkflowId(s: String): Option[WorkflowId] =
    if (s.isEmpty) None
    else Some(Ref.LedgerString.assertFromString(s))

}

