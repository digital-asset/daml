package com.daml.ledger.api.testtool.templates

import com.daml.ledger.api.testtool.infrastructure.LedgerTestContext
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.v1.value.Value.Sum.{Party, Text, List => DamlList}
import com.digitalasset.platform.participant.util.ValueConversions._

import scala.concurrent.Future

object TextKey {
  def apply(party: String, text: String, disclosedTo: Seq[String])(
      implicit context: LedgerTestContext): Future[TextKey] =
    context
      .create(
        party,
        ids.textKey,
        Map[String, value.Value.Sum](
          "tkParty" -> Party(party),
          "tkKey" -> Text(text),
          "tkDisclosedTo" -> DamlList(value.List(disclosedTo.map(_.asParty)))
        )
      )
      .map(new TextKey(_, party, text, disclosedTo) {})
}

sealed abstract case class TextKey(
    contractId: String,
    party: String,
    text: String,
    disclosedTo: Seq[String]) {
  def choice()(implicit context: LedgerTestContext): Future[Unit] =
    context.exercise(party, ids.textKey, contractId, "TextKeyChoice", Map.empty)
}
