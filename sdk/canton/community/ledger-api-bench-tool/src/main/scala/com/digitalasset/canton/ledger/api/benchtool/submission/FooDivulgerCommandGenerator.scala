// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.commands.{Command, CreateCommand}
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ledger.api.benchtool.infrastructure.TestDars
import com.digitalasset.daml.lf.data.Ref

object FooDivulgerCommandGenerator {

  private val packageRef: Ref.PackageRef = TestDars.benchtoolDarPackageRef

  /** Builds a create Divulger command for each non-empty subset of divulgees such that the created
    * Divulger contract can be used to divulge (by immediate divulgence) Foo1, Foo2 or Foo3
    * contracts to the corresponding subset of divulgees.
    *
    * @param allDivulgees
    *   Small number of divulgees. At most 5.
    * @return
    *   A tuple of:
    *   - a sequence of create Divulger commands,
    *   - a map from sets of divulgees (all non-empty subsets of all divulgees) to corresponding
    *     contract keys,
    */
  def makeCreateDivulgerCommands(
      divulgingParty: Party,
      allDivulgees: List[Party],
  ): (List[Command], Map[Set[Party], Value]) = {
    require(
      allDivulgees.sizeIs <= 5,
      s"Number of divulgee parties must be at most 5, was: ${allDivulgees.size}.",
    )
    def allNonEmptySubsets(divulgees: List[Party]): List[List[Party]] = {
      def iter(remaining: List[Party]): List[List[Party]] =
        remaining match {
          case Nil => List(List.empty)
          case head :: tail =>
            val sub: List[List[Party]] = iter(tail)
            val sub2: List[List[Party]] = sub.map(xs => xs.prepended(head))
            sub ::: sub2
        }
      iter(divulgees)
        .collect {
          case parties if parties.nonEmpty => parties.sortBy(_.getValue)
        }
    }

    def createDivulgerFor(divulgees: List[Party]): (Command, Value) = {
      val keyId = "divulger-" + FooCommandGenerator.nextContractNumber.getAndIncrement()
      val createDivulgerCmd: Command = makeCreateDivulgerCommand(
        divulgees = divulgees,
        divulger = divulgingParty,
        keyId = keyId,
      )
      val divulgerKey: Value = FooCommandGenerator.makeContractKeyValue(divulgingParty, keyId)
      (createDivulgerCmd, divulgerKey)
    }

    val allSubsets = allNonEmptySubsets(allDivulgees)
    val (commands, keys, divulgeeSets) = allSubsets.map { divulgees =>
      val (cmd, key) = createDivulgerFor(divulgees)
      (cmd, key, divulgees.toSet)
    }.unzip3
    val divulgeesToContractKeysMap = divulgeeSets.zip(keys).toMap
    (commands, divulgeesToContractKeysMap)
  }

  private def makeCreateDivulgerCommand(
      keyId: String,
      divulger: Party,
      divulgees: List[Party],
  ) = {
    val createArguments: Option[Record] = Some(
      Record(
        None,
        Seq(
          RecordField(
            label = "divulger",
            value = Some(Value(Value.Sum.Party(divulger.getValue))),
          ),
          RecordField(
            label = "divulgees",
            value = Some(
              Value(
                Value.Sum.List(
                  com.daml.ledger.api.v2.value.List(
                    divulgees.map(d => Value(Value.Sum.Party(d.getValue)))
                  )
                )
              )
            ),
          ),
          RecordField(
            label = "keyId",
            value = Some(Value(Value.Sum.Text(keyId))),
          ),
        ),
      )
    )
    val c: Command = Command(
      command = Command.Command.Create(
        CreateCommand(
          templateId =
            Some(FooTemplateDescriptor.divulgerTemplateId(packageId = packageRef.toString)),
          createArguments = createArguments,
        )
      )
    )
    c
  }

}
