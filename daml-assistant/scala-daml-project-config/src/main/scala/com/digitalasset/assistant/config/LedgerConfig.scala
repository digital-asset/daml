package com.digitalasset.assistant.config

import io.circe.{Decoder, HCursor}

final case class LedgerConfig(
    port: Option[Int],
    address: Option[String],
    wallClockTime: Option[Boolean],
    maxInboundMessageSize: Option[Int],
    ledgerId: Option[String],
    maxTtlSeconds: Option[Int]
) {
  def isEmpty: Boolean =
    port.isEmpty && address.isEmpty && wallClockTime.isEmpty && maxInboundMessageSize.isEmpty && ledgerId.isEmpty && maxTtlSeconds.isEmpty
}

object LedgerConfig {
  implicit val ledgerConfigDecoder: Decoder[LedgerConfig] = new Decoder[LedgerConfig] {
    override def apply(c: HCursor): Decoder.Result[LedgerConfig] =
      for {
        port <- c.downField("port").as[Option[Int]]
        address <- c.downField("address").as[Option[String]]
        wallClockTime <- c.downField("wall-clock-time").as[Option[Boolean]]
        maxInboundMessageSize <- c.downField("max-inbound-message-size-bytes").as[Option[Int]]
        ledgerId <- c.downField("ledger-id").as[Option[String]]
        maxTtlSeconds <- c.downField("max-ttl-seconds").as[Option[Int]]
      } yield
        LedgerConfig(
          port = port,
          address = address,
          wallClockTime = wallClockTime,
          maxInboundMessageSize = maxInboundMessageSize,
          ledgerId = ledgerId,
          maxTtlSeconds = maxTtlSeconds
        )
  }
}
