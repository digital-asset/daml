// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.value.Value
import com.daml.ledger.api.v2.value.Value.Sum
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value as Lf

import domain.*

object DomainMocks {

  val party = Ref.Party.assertFromString("party")

  val identifier = Ref.Identifier(
    Ref.PackageId.assertFromString("package"),
    Ref.QualifiedName.assertFromString("module:entity"),
  )

  val commandId = CommandId(Ref.LedgerString.assertFromString("commandId"))

  val submissionId = SubmissionId(Ref.LedgerString.assertFromString("submissionId"))

  val updateId = UpdateId(Ref.LedgerString.assertFromString("deadbeef"))

  val applicationId = Ref.ApplicationId.assertFromString("applicationId")

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
      """Invalid argument: non expected character 0x40 in Daml-LF Party "p@rty""""
  }

}
