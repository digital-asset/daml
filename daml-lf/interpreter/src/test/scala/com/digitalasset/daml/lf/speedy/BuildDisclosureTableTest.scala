// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.{FrontStack, ImmArray}
import com.daml.lf.interpretation.Error.DisclosurePreprocessing
import com.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class BuildDisclosureTableTest extends AnyFreeSpec with Inside with Matchers {

  import BuildDisclosureTableTest._
  import ExplicitDisclosureLib._

  "buildDisclosureTable" - {
    "disclosure preprocessing" - {
      "template does not exist" in {
        val error = intercept[SError.SErrorDamlException] {
          Speedy.buildDiscTable(ImmArray(disclosedHouseContractInvalidTemplate), pkg.pkgInterface)
        }

        error shouldBe SError.SErrorDamlException(
          DisclosurePreprocessing(
            DisclosurePreprocessing.NonExistentTemplate(
              disclosedHouseContractInvalidTemplate.templateId
            )
          )
        )
      }

      "disclosed contract key has no hash" in {
        val error = intercept[SError.SErrorDamlException] {
          Speedy.buildDiscTable(ImmArray(disclosedHouseContractNoHash), pkg.pkgInterface)
        }

        error shouldBe SError.SErrorDamlException(
          DisclosurePreprocessing(
            DisclosurePreprocessing.NonExistentDisclosedContractKeyHash(
              contractId,
              disclosedHouseContractNoHash.templateId,
            )
          )
        )
      }

      "duplicate disclosed contract IDs" in {
        val error = intercept[SError.SErrorDamlException] {
          Speedy.buildDiscTable(
            ImmArray(disclosedHouseContract, disclosedHouseContract),
            pkg.pkgInterface,
          )
        }

        error shouldBe SError.SErrorDamlException(
          DisclosurePreprocessing(
            DisclosurePreprocessing.DuplicateContractIds(houseTemplateId)
          )
        )
      }

      "duplicate disclosed contract key hashes" in {
        val error = intercept[SError.SErrorDamlException] {
          Speedy.buildDiscTable(
            ImmArray(disclosedHouseContract, disclosedHouseContractWithDuplicateKey),
            pkg.pkgInterface,
          )
        }

        error shouldBe SError.SErrorDamlException(
          DisclosurePreprocessing(
            DisclosurePreprocessing.DuplicateContractKeys(houseTemplateId, collidingKeyHash)
          )
        )
      }
    }
  }
}

object BuildDisclosureTableTest {

  import ExplicitDisclosureLib._

  val disclosedHouseContractInvalidTemplate: DisclosedContract = buildDisclosedHouseContract(
    contractId,
    disclosureParty,
    maintainerParty,
    templateId = invalidTemplateId,
    withHash = false,
  )
  val disclosedHouseContractNoHash: DisclosedContract =
    buildDisclosedHouseContract(contractId, disclosureParty, maintainerParty, withHash = false)
  val disclosedHouseContractWithDuplicateKey: DisclosedContract =
    buildDisclosedHouseContract(altDisclosureContractId, disclosureParty, maintainerParty)
  val collidingKeyHash: crypto.Hash = {
    val key = Value.ValueRecord(
      None,
      ImmArray(
        None -> Value.ValueText(testKeyName),
        None -> Value.ValueList(FrontStack.from(ImmArray(Value.ValueParty(maintainerParty)))),
      ),
    )

    crypto.Hash.assertHashContractKey(houseTemplateType, key)
  }
}
