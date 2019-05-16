package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import anorm.ToStatement
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry

object Convertion {

  implicit def subStringToStatement[T <: String]: ToStatement[T] =
    ToStatement.stringToStatement.asInstanceOf[ToStatement[T]]

  @throws[IllegalArgumentException]
  def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  @throws[IllegalArgumentException]
  def toContractId(s: String): Ref.ContractId = Ref.LedgerName.assertFromString(s)

  @throws[IllegalArgumentException]
  def toTransactionId(s: String): Ref.TransactionId = Ref.LedgerName.assertFromString(s)

  @throws[IllegalArgumentException]
  def toLedgerId(s: String): Ref.LedgerId = Ref.LedgerName.assertFromString(s)

  @throws[IllegalArgumentException]
  def toEventId(s: String): LedgerEntry.EventId = Ref.LedgerName.assertFromString(s)

}
