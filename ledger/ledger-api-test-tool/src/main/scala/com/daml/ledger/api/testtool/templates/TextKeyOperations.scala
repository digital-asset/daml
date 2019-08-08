package com.daml.ledger.api.testtool.templates

import com.daml.ledger.api.testtool.infrastructure.LedgerTestContext
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.v1.value.Value.Sum.{ContractId, Optional, Party}
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.platform.participant.util.ValueConversions._

import scala.concurrent.Future

object TextKeyOperations {
  def apply(party: String)(implicit context: LedgerTestContext): Future[TextKeyOperations] =
    context
      .create(
        party,
        ids.textKeyOperations,
        Map[String, value.Value.Sum](
          "tkoParty" -> Party(party),
        ))
      .map(new TextKeyOperations(_, party) {})
}

sealed abstract case class TextKeyOperations(contractId: String, party: String) {
  def lookup(tk: (String, String), expected: Option[String])(
      implicit context: LedgerTestContext): Future[Unit] = {
    context.exercise(
      party,
      ids.textKeyOperations,
      contractId,
      "TKOLookup",
      Map[String, value.Value.Sum](
        "keyToLookup" -> toKeyValue(tk),
        "expected" -> Optional(value.Optional(expected.map(_.asContractId)))
      )
    )
  }

  def fetch(tk: (String, String), expectedCid: String)(
      implicit context: LedgerTestContext): Future[Unit] = {
    context.exercise(
      party,
      ids.textKeyOperations,
      contractId,
      "TKOFetch",
      Map[String, value.Value.Sum](
        "keyToFetch" -> toKeyValue(tk),
        "expectedCid" -> ContractId(expectedCid)
      ))
  }

  def consumeAndLookup(tk: (String, String), cidToConsume: String)(
      implicit context: LedgerTestContext): Future[Unit] = {
    context.exercise(
      party,
      ids.textKeyOperations,
      contractId,
      "TKOConsumeAndLookup",
      Map[String, value.Value.Sum](
        "keyToLookup" -> toKeyValue(tk),
        "cidToConsume" -> ContractId(cidToConsume)
      )
    )
  }

  private def toKeyValue(tk: (String, String)): Value.Sum =
    Value.Sum.Record(
      Record(fields = List(RecordField(value = tk._1.asParty), RecordField(value = tk._2.asText))))
}
