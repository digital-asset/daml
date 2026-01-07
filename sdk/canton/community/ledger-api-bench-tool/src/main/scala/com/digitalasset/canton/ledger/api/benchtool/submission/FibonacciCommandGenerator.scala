// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.commands.{Command, CreateAndExerciseCommand}
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FibonacciSubmissionConfig
import com.digitalasset.canton.ledger.api.benchtool.infrastructure.TestDars
import com.digitalasset.daml.lf.data.Ref

import scala.util.{Success, Try}

final class FibonacciCommandGenerator(
    config: FibonacciSubmissionConfig,
    signatory: Party,
    names: Names,
) extends SimpleCommandGenerator {

  private val packageRef: Ref.PackageRef = TestDars.benchtoolDarPackageRef

  override def nextUserId(): String = names.benchtoolUserId

  override def nextExtraCommandSubmitters(): List[Party] = List.empty

  def next(): Try[Seq[Command]] = {
    val createArguments: Option[Record] = Some(
      Record(
        None,
        Seq(
          RecordField(
            label = "owner",
            value = Some(Value(Value.Sum.Party(signatory.getValue))),
          )
        ),
      )
    )
    Success(
      Seq(
        Command(
          Command.Command.CreateAndExercise(
            CreateAndExerciseCommand(
              templateId =
                Some(FooTemplateDescriptor.inefficientFibonacciTemplateId(packageRef.toString)),
              createArguments = createArguments,
              choice = "InefficientFibonacci_Compute",
              choiceArgument = Some(
                Value(
                  Value.Sum.Record(
                    Record(
                      None,
                      Seq(
                        RecordField(
                          label = "value",
                          value = Some(Value(Value.Sum.Int64(config.value.toLong))),
                        )
                      ),
                    )
                  )
                )
              ),
            )
          )
        )
      )
    )
  }

}
