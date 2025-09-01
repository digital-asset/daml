// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.crypto.Hash.HashingMethod
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  Node,
  TransactionVersion,
}
import com.digitalasset.daml.lf.value.{Value => V}
import org.scalatest.EitherValues
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ContractValidationSpec extends AnyWordSpec with Matchers with EitherValues {

  private val underTest = ContractValidation(Engine.DevEngine(LanguageMajorVersion.V2))
  private val alice = Ref.Party.assertFromString("alice")
  private val create = Node.Create(
    coid = TransactionBuilder.newCid,
    packageName = Ref.PackageName.assertFromString("somePackageName"),
    templateId = Ref.Identifier.assertFromString("createPackageId:createModule:createName"),
    arg = V.ValueText("some-arg"),
    signatories = Set(alice),
    stakeholders = Set(alice),
    keyOpt = None,
    version = TransactionVersion.minVersion,
  )
  private val instance = FatContractInstance.fromCreateNode(
    create,
    CreationTime.CreatedAt(Time.Timestamp.now()),
    Bytes.Empty,
  )
  private val targetPackageId = Ref.PackageId.assertFromString("targetPackageId")
  private val validationSuccess = ResultDone(Right(()))

  "ContractValidation.hash" should {

    "hash contract using legacy hashing" in {

      val expected = Hash
        .hashContractInstance(
          templateId = instance.templateId,
          arg = instance.createArg,
          packageName = instance.packageName,
          upgradeFriendly = false,
        )
        .value

      inside(underTest.hash(instance, targetPackageId, hashingMethod = HashingMethod.Legacy)) {
        case ResultDone(actual) => actual shouldBe expected
      }
    }

    "hash contract using upgrade-friendly hashing" in {

      val expected = Hash
        .hashContractInstance(
          templateId = instance.templateId,
          arg = instance.createArg,
          packageName = instance.packageName,
          upgradeFriendly = true,
        )
        .value

      inside(
        underTest.hash(instance, targetPackageId, hashingMethod = HashingMethod.UpgradeFriendly)
      ) { case ResultDone(actual) =>
        actual shouldBe expected
      }
    }
  }

  "ContractValidation.validate" should {

    "accept valid contract" in {
      underTest.validate(
        instance = instance,
        targetPackageId = targetPackageId,
        hashingMethod = HashingMethod.UpgradeFriendly,
        idValidator = _ => true,
      ) shouldBe validationSuccess
    }

    "pass correct hash to validation method" in {

      val expected = Hash
        .hashContractInstance(
          templateId = instance.templateId,
          arg = instance.createArg,
          packageName = instance.packageName,
          upgradeFriendly = true,
        )
        .value

      underTest.validate(
        instance = instance,
        targetPackageId = targetPackageId,
        hashingMethod = HashingMethod.UpgradeFriendly,
        idValidator = actual => actual == expected,
      ) shouldBe validationSuccess
    }

    "fail if contract id validation fails" in {

      inside(
        underTest.validate(
          instance = instance,
          targetPackageId = targetPackageId,
          hashingMethod = HashingMethod.UpgradeFriendly,
          idValidator = _ => false,
        )
      ) { case ResultDone(Left(_)) =>
        succeed
      }
    }

  }

}
