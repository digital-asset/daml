// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.ImmArray
import com.daml.lf.interpretation.Error.DisclosurePreprocessing
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class BuildDisclosureTableTest extends AnyFreeSpec with Inside with Matchers {

  import BuildDisclosureTableTest._
  import ExplicitDisclosureTest._

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
            DisclosurePreprocessing.DuplicateContractKeys(houseTemplateId)
          )
        )

      }
    }
  }
}

object BuildDisclosureTableTest {

  import ExplicitDisclosureTest._

  val disclosedHouseContractInvalidTemplate: DisclosedContract = buildDisclosedHouseContract(
    contractId,
    disclosureParty,
    maintainerParty,
    templateId = invalidTemplateId,
    withHash = false,
  )
  val disclosedCaveContractInvalidTemplate: DisclosedContract = buildDisclosedCaveContract(
    contractId,
    disclosureParty,
    templateId = invalidTemplateId,
  )
  val disclosedHouseContractNoHash: DisclosedContract =
    buildDisclosedHouseContract(contractId, disclosureParty, maintainerParty, withHash = false)
  val disclosedHouseContractWithDuplicateKey: DisclosedContract =
    buildDisclosedHouseContract(altDisclosureContractId, disclosureParty, maintainerParty)
}
