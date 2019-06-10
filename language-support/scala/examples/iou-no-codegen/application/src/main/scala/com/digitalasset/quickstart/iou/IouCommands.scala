// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.quickstart.iou

import com.digitalasset.ledger.api.v1.commands.Command.Command
import com.digitalasset.ledger.api.v1.commands.{CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.value._

/**
  * Examples of how to construct ledger commands "manually".
  */
object IouCommands {
  // <doc-ref:iou-no-codegen-create-command>
  def iouCreateCommand(
      templateId: Identifier,
      issuer: String,
      owner: String,
      currency: String,
      amount: BigDecimal): Command.Create = {
    val fields = Seq(
      RecordField("issuer", Some(Value(Value.Sum.Party(issuer)))),
      RecordField("owner", Some(Value(Value.Sum.Party(owner)))),
      RecordField("currency", Some(Value(Value.Sum.Text(currency)))),
      RecordField("amount", Some(Value(Value.Sum.Decimal(amount.toString)))),
      RecordField("observers", Some(Value(Value.Sum.List(List())))),
    )
    Command.Create(
      CreateCommand(
        templateId = Some(templateId),
        createArguments = Some(Record(Some(templateId), fields))))
  }
  // </doc-ref:iou-no-codegen-create-command>

  // <doc-ref:iou-no-codegen-exercise-command>
  def iouTransferExerciseCommand(
      templateId: Identifier,
      contractId: String,
      newOwner: String): Command.Exercise = {
    val transferTemplateId = Identifier(
      packageId = templateId.packageId,
      moduleName = templateId.moduleName,
      entityName = "Iou_Transfer")
    val fields = Seq(RecordField("newOwner", Some(Value(Value.Sum.Party(newOwner)))))
    Command.Exercise(
      ExerciseCommand(
        templateId = Some(templateId),
        contractId = contractId,
        choice = "Iou_Transfer",
        choiceArgument = Some(Value(Value.Sum.Record(Record(Some(transferTemplateId), fields))))
      ))
  }
  // </doc-ref:iou-no-codegen-exercise-command>
}
