// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.value.Value
import com.daml.ledger.api.v2.value.Value.Sum
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.IdString
import com.digitalasset.daml.lf.value.Value as Lf
import scalaz.@@

object ApiMocks {

  val party: IdString.Party = Ref.Party.assertFromString("party")

  val identifier: Ref.Identifier = Ref.Identifier(
    Ref.PackageId.assertFromString("package"),
    Ref.QualifiedName.assertFromString("module:entity"),
  )

  val commandId: IdString.LedgerString @@ CommandIdTag = CommandId(
    Ref.LedgerString.assertFromString("commandId")
  )

  val submissionId: IdString.LedgerString @@ SubmissionIdTag = SubmissionId(
    Ref.LedgerString.assertFromString("submissionId")
  )

  val updateId: IdString.LedgerString @@ UpdateIdTag = UpdateId(
    Ref.LedgerString.assertFromString("deadbeef")
  )

  val applicationId: IdString.ApplicationId = Ref.ApplicationId.assertFromString("applicationId")

  val workflowId: IdString.LedgerString @@ WorkflowIdTag = WorkflowId(
    Ref.LedgerString.assertFromString("workflowId")
  )

  val label: IdString.Name = Ref.Name.assertFromString("label")

  object values {
    val int64: Lf.ValueInt64 = Lf.ValueInt64(1)
    val constructor: IdString.Name = Ref.Name.assertFromString("constructor")

    private val validPartyString = "party"
    val validApiParty: Value = Value(Sum.Party(validPartyString))
    val validLfParty: Lf.ValueParty = Lf.ValueParty(Ref.Party.assertFromString(validPartyString))

    private val invalidPartyString = "p@rty"
    val invalidApiParty: Value = Value(Sum.Party(invalidPartyString))
    val invalidPartyMsg =
      """Invalid argument: non expected character 0x40 in Daml-LF Party "p@rty""""
  }

}
