// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import com.digitalasset.ledger.client.binding.Contract
import com.digitalasset.ods.slick.TemplateCompanionSlickTableQuery

import slick.jdbc.JdbcProfile

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object OdsIntegrationSpec {
  import com.digitalasset.sample.MyMain.CallablePayout

  /** What a user might write instead of e.g. `class IdCountSlickTable`. */
  class CallablePayoutSlickTable(val profile: JdbcProfile, val odsId: String)
      extends TemplateCompanionSlickTableQuery[CallablePayout, CallablePayout.type] { cpst =>
    import profile.api._
    def queryingExample(db: profile.api.Database): Future[Seq[Contract[CallablePayout]]] =
      db.run(cpst.all.filter(t => t.c.giver === t.c.receiver).result)
  }

  // you can avoid saying the name twice by not using `extends`
  def anotherCallablePayoutSlickTable = TemplateCompanionSlickTableQuery(CallablePayout) _
  anotherCallablePayoutSlickTable: (
      (
          _,
          _) => TemplateCompanionSlickTableQuery[CallablePayout, CallablePayout.type])
}
