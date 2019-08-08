package com.daml.ledger.api.testtool.templates

import com.daml.ledger.api.testtool.infrastructure.LedgerTestContext
import com.digitalasset.ledger.api.v1.value.Value.Sum.{ContractId, Party}

import scala.concurrent.Future

object ShowDelegated {
  def apply(owner: String, delegate: String)(
      implicit context: LedgerTestContext): Future[ShowDelegated] =
    context
      .create(owner, ids.showDelegated, Map("owner" -> Party(owner), "delegate" -> Party(delegate)))
      .map(new ShowDelegated(_, owner, delegate) {})
}
sealed abstract case class ShowDelegated(contractId: String, owner: String, delegate: String) {
  def showIt(delegatedId: String)(implicit context: LedgerTestContext): Future[Unit] =
    context.exercise(
      owner,
      ids.showDelegated,
      contractId,
      "ShowIt",
      Map("delegatedId" -> ContractId(delegatedId)))
}
