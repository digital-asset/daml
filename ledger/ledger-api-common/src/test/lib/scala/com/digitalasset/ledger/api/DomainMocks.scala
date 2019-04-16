// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain._

object DomainMocks {

  val party = Party("party")

  val identifier = Ref.Identifier(
    Ref.PackageId.assertFromString("package"),
    Ref.QualifiedName.assertFromString("module:entity"))

  val commandId = CommandId("commandId")

  val transactionId = TransactionId("deadbeef")

  val applicationId = ApplicationId("applicationId")

  val workflowId = WorkflowId("workflowId")

  val label = Label("label")

  object values {
    val int64 = Value.Int64Value(1)
    val constructor = VariantConstructor("constructor")
  }

}
