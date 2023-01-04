// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.lf.data.Ref
import com.daml.ledger.api.domain._
import com.daml.lf.value.{Value => Lf}
import com.daml.ledger.api.v1.value.Value
import com.daml.ledger.api.v1.value.Value.Sum

object DomainMocks {

  val party = Ref.Party.assertFromString("party")

  val identifier = Ref.Identifier(
    Ref.PackageId.assertFromString("package"),
    Ref.QualifiedName.assertFromString("module:entity"),
  )

  val commandId = CommandId(Ref.LedgerString.assertFromString("commandId"))

  val submissionId = SubmissionId(Ref.LedgerString.assertFromString("submissionId"))

  val transactionId = TransactionId(Ref.LedgerString.assertFromString("deadbeef"))

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
