// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain._
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.api.v1.value.Value
import com.digitalasset.ledger.api.v1.value.Value.Sum

object DomainMocks {

  val party = Ref.Party.assertFromString("party")

  val identifier = Ref.Identifier(
    Ref.PackageId.assertFromString("package"),
    Ref.QualifiedName.assertFromString("module:entity"))

  val commandId = CommandId(Ref.LedgerString.assertFromString("commandId"))

  val transactionId = TransactionId(Ref.LedgerString.assertFromString("deadbeef"))

  val applicationId = ApplicationId(Ref.LedgerString.assertFromString("applicationId"))

  val workflowId = WorkflowId(Ref.LedgerString.assertFromString("workflowId"))

  val label = Ref.Name.assertFromString("label")

  object values {
    val int64 = Lf.ValueInt64(1)
    val constructor = Ref.Name.assertFromString("constructor")

    private val validPartyString = "party"
    val validApiParty = Value(Sum.Party(validPartyString))
    val validLfParty = Lf.ValueParty(Ref.Party.assertFromString(validPartyString))

    private val invalidPartyString = "p@rty"
    val invalidApiParty = Value(Sum.Party(invalidPartyString))
    val invalidPartyMsg =
      """Invalid argument: non expected character 0x40 in "p@rty""""
  }

}
