// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.config

import java.nio.file.Path
import java.util.UUID

import scalaz.{OneAnd, Order}
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import scalaz.std.list._
import scalaz.std.string._
import com.daml.lf.data.Ref.Party
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ports.Port

sealed abstract class SnapshotEndSetting
object SnapshotEndSetting {
  case object Head extends SnapshotEndSetting
  case object Follow extends SnapshotEndSetting
  final case class Until(offset: String) extends SnapshotEndSetting
}

final case class ExtractorConfig(
    ledgerHost: String,
    ledgerPort: Port,
    ledgerInboundMessageSizeMax: Int,
    from: LedgerOffset,
    to: SnapshotEndSetting,
    parties: ExtractorConfig.Parties,
    templateConfigs: Set[TemplateConfig],
    tlsConfig: TlsConfiguration,
    accessTokenFile: Option[Path],
    appId: String = s"Extractor-${UUID.randomUUID().toString}"
) {
  @SuppressWarnings(Array("org.wartremover.warts.Any")) // huh?
  def partySpec: String = parties.widen[String] intercalate ","
}

object ExtractorConfig {
  type Parties = OneAnd[List, Party]
}

final case class TemplateConfig(moduleName: String, entityName: String)

object TemplateConfig {
  implicit val templateConfigOrdering: Ordering[TemplateConfig] =
    Ordering.by(TemplateConfig.unapply)

  implicit val templateConfigOrder: Order[TemplateConfig] =
    Order.fromScalaOrdering
}
