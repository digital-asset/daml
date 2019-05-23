// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.config

import java.util.UUID
import scalaz.OneAnd
import scalaz.syntax.foldable._
import scalaz.std.list._
import scalaz.std.string._

import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.tls.TlsConfiguration

sealed abstract class SnapshotEndSetting
object SnapshotEndSetting {
  case object Head extends SnapshotEndSetting
  case object Follow extends SnapshotEndSetting
  final case class Until(offset: String) extends SnapshotEndSetting
}

final case class ExtractorConfig(
    ledgerHost: String,
    ledgerPort: Int,
    from: LedgerOffset,
    to: SnapshotEndSetting,
    parties: ExtractorConfig.Parties,
    tlsConfig: TlsConfiguration,
    appId: String = s"Extractor-${UUID.randomUUID().toString}"
) {
  def partySpec: String = parties intercalate ","
}

object ExtractorConfig {
  type Parties = OneAnd[List, String]
  def parties(spec: String): Parties = {
    val Array(hd, tl @ _*) = spec split ','
    OneAnd(hd, tl.toList)
  }
}
