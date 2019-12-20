package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.v1.{Configuration, LedgerId, Offset, TimeModel}
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
case class LedgerRecord(offset: Offset, entryId: DamlLogEntryId, envelope: Array[Byte])

trait LedgerReader {
  def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed]

  def retrieveLedgerId(): LedgerId

  def checkHealth(): HealthStatus = Healthy
}

object LedgerReader {
  val DefaultTimeModel = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault
  )
}
