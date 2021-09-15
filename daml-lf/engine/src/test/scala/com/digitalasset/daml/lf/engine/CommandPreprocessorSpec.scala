// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.command._
import com.daml.lf.data._
import com.daml.lf.value.Value._
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class CommandPreprocessorSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import com.daml.lf.testing.parser.Implicits._
  import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}
  private implicit val defaultPackageId = defaultParserParameters.defaultPackageId

  lazy val pkg =
    p"""
        module Mod {

          record @serializable Record = { field : Int64 };

          record @serializable RecordRef = { owner: Party, cid: (ContractId Mod:Record) };

          val @noPartyLiterals toParties: Mod:RecordRef -> List Party =
            \ (ref: Mod:RecordRef) -> Cons @Party [Mod:RecordRef {owner} ref] (Nil @Party);

          template (this : RecordRef) = {
            precondition True,
            signatories Mod:toParties this,
            observers Mod:toParties this,
            agreement "Agreement",
            choices {
              choice Change (self) (newCid: ContractId Mod:Record) : ContractId Mod:RecordRef,
                  controllers Mod:toParties this,
                  observers Nil @Party
                to create @Mod:RecordRef Mod:RecordRef { owner = Mod:RecordRef {owner} this, cid = newCid }
            },
            key @Party (Mod:RecordRef {owner} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party))
          };

        }
    """

  private[this] val compiledPackage = ConcurrentCompiledPackages()
  assert(compiledPackage.addPackage(defaultPackageId, pkg) == ResultDone.Unit)

  "preprocessCommand" should {

    def contractIdTestCases(culpritCid: ContractId, innocentCid: ContractId) = Table[ApiCommand](
      "command",
      CreateCommand(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> ValueParty("Alice"), "" -> ValueContractId(culpritCid))),
      ),
      ExerciseCommand(
        "Mod:RecordRef",
        innocentCid,
        "Change",
        ValueContractId(culpritCid),
      ),
      ExerciseCommand(
        "Mod:RecordRef",
        culpritCid,
        "Change",
        ValueContractId(innocentCid),
      ),
      CreateAndExerciseCommand(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> ValueParty("Alice"), "" -> ValueContractId(culpritCid))),
        "Change",
        ValueContractId(innocentCid),
      ),
      CreateAndExerciseCommand(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> ValueParty("Alice"), "" -> ValueContractId(innocentCid))),
        "Change",
        ValueContractId(culpritCid),
      ),
      ExerciseByKeyCommand(
        "Mod:RecordRef",
        ValueParty("Alice"),
        "Change",
        ValueContractId(culpritCid),
      ),
    )

    "accept all contract IDs when require flags are false" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.interface,
        forbidV0ContractId = false,
        requireV1ContractIdSuffix = false,
      )

      val cids = List(
        ContractId.V1
          .assertBuild(
            crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"),
            Bytes.assertFromString("00"),
          ),
        ContractId.V1
          .assertBuild(crypto.Hash.hashPrivateKey("a non-suffixed V1 Contract ID"), Bytes.Empty),
        ContractId.V0.assertFromString("#a V0 Contract ID"),
      )

      cids.foreach(cid =>
        forEvery(contractIdTestCases(cids.head, cid))(cmd =>
          Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe a[Success[_]]
        )
      )

    }

    "reject V0 Contract IDs when requireV1ContractId flag is true" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.interface,
        forbidV0ContractId = true,
        requireV1ContractIdSuffix = false,
      )

      val List(aLegalCid, anotherLegalCid) =
        List("a legal Contract ID", "another legal Contract ID").map(s =>
          ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))
        )
      val illegalCid = ContractId.V0.assertFromString("#illegal Contract ID")
      val failure = Failure(Error.Preprocessing.IllegalContractId.V0ContractId(illegalCid))

      forEvery(contractIdTestCases(aLegalCid, anotherLegalCid))(cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe a[Success[_]]
      )

      forEvery(contractIdTestCases(illegalCid, aLegalCid))(cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe failure
      )
    }

    "reject non suffixed V1 Contract IDs when requireV1ContractIdSuffix is true" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.interface,
        forbidV0ContractId = true,
        requireV1ContractIdSuffix = true,
      )
      val List(aLegalCid, anotherLegalCid) =
        List("a legal Contract ID", "another legal Contract ID").map(s =>
          ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))
        )
      val illegalCid =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("an illegal Contract ID"), Bytes.Empty)
      val failure = Failure(Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(illegalCid))

      forEvery(contractIdTestCases(aLegalCid, anotherLegalCid)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe a[Success[_]]
      }
      forEvery(contractIdTestCases(illegalCid, aLegalCid)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe failure
      }
    }

  }

}
