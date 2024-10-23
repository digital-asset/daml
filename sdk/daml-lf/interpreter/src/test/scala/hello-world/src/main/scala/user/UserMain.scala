// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package user

import com.digitalasset.daml.lf.ledger.api.{logInfo, LfValueInt, LfValueParty}
import org.teavm.interop.Export

object UserMain {
  @Export(name = "main")
  def main(): Unit = {
//    implicit val companion: SimpleTemplate.type = SimpleTemplate

    logInfo("hello-world")
//    val contractId = new SimpleTemplate(LfValueParty("alice"), LfValueInt(42)).create[SimpleTemplate, SimpleTemplate.type]()
//    logInfo(s"created contract $contractId")
  }
}
