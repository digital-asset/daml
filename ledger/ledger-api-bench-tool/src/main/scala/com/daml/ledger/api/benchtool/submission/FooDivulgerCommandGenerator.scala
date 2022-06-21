// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.value.Value
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.benchtool.Foo.Divulger

object FooDivulgerCommandGenerator {

  /** Builds a create Divulger command for each non-empty subset of divulgees
    * such that the created Divulger contract can be used to divulge (by immediate divulgence) Foo1, Foo2 or Foo3 contracts
    * to the corresponding subset of divulgees.
    *
    * @param allDivulgees - Small number of divulgees. At most 5.
    * @return A tuple of:
    *          - a sequence of create Divulger commands,
    *          - a map from sets of divulgees (all non-empty subsets of all divulgees) to corresponding contract keys,
    */
  def makeCreateDivulgerCommands(
      divulgingParty: Primitive.Party,
      allDivulgees: List[Primitive.Party],
  ): (List[Command], Map[Set[Primitive.Party], Value]) = {
    require(
      allDivulgees.size <= 5,
      s"Number of divulgee parties must be at most 5, was: ${allDivulgees.size}.",
    )
    def allNonEmptySubsets(divulgees: List[Primitive.Party]): List[List[Primitive.Party]] = {
      def iter(remaining: List[Primitive.Party]): List[List[Primitive.Party]] = {
        remaining match {
          case Nil => List(List.empty)
          case head :: tail =>
            val sub: List[List[Primitive.Party]] = iter(tail)
            val sub2: List[List[Primitive.Party]] = sub.map(xs => xs.prepended(head))
            sub ::: sub2
        }
      }
      import scalaz.syntax.tag._
      iter(divulgees)
        .collect {
          case parties if parties.nonEmpty => parties.sortBy(_.unwrap)
        }
    }

    def createDivulgerFor(divulgees: List[Primitive.Party]): (Command, Value) = {
      val keyId = "divulger-" + FooCommandGenerator.nextContractNumber.getAndIncrement()
      val createDivulgerCmd = Divulger(
        divulgees = divulgees,
        divulger = divulgingParty,
        keyId = keyId,
      ).create.command
      val divulgerKey: Value = FooCommandGenerator.makeContractKeyValue(divulgingParty, keyId)
      (createDivulgerCmd, divulgerKey)
    }

    val allSubsets = allNonEmptySubsets(allDivulgees)
    val (commands, keys, divulgeeSets) = allSubsets.map { divulgees: List[Primitive.Party] =>
      val (cmd, key) = createDivulgerFor(divulgees)
      (cmd, key, divulgees.toSet)
    }.unzip3
    val divulgeesToContractKeysMap = divulgeeSets.zip(keys).toMap
    (commands, divulgeesToContractKeysMap)
  }

}
