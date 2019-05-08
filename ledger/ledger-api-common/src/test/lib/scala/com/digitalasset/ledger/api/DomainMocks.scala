// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  val commandId = CommandId("commandId")

  val transactionId = TransactionId("deadbeef")

  val applicationId = ApplicationId("applicationId")

  val workflowId = WorkflowId("workflowId")

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
      """Invalid argument: string "p@rty" does not match regex "[a-zA-Z0-9\-_ ]+""""
  }

}
