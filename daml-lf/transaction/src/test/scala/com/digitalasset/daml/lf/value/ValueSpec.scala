// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, Unnatural}
import com.digitalasset.daml.lf.value.Value._

import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks}
import org.scalatest.{FreeSpec, Matchers}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ValueSpec extends FreeSpec with Matchers with Checkers with GeneratorDrivenPropertyChecks {
  "serialize" - {
    val emptyStruct = ValueStruct(ImmArray.empty)
    val emptyStructError = "contains struct ValueStruct(ImmArray())"
    val exceedsNesting = (1 to MAXIMUM_NESTING + 1).foldRight[Value[Nothing]](ValueInt64(42)) {
      case (_, v) => ValueVariant(None, Ref.Name.assertFromString("foo"), v)
    }
    val exceedsNestingError = s"exceeds maximum nesting value of $MAXIMUM_NESTING"
    val matchesNesting = (1 to MAXIMUM_NESTING).foldRight[Value[Nothing]](ValueInt64(42)) {
      case (_, v) => ValueVariant(None, Ref.Name.assertFromString("foo"), v)
    }

    "rejects struct" in {
      emptyStruct.serializable shouldBe ImmArray(emptyStructError)
    }

    "rejects nested struct" in {
      ValueList(FrontStack(emptyStruct)).serializable shouldBe ImmArray(emptyStructError)
    }

    "rejects excessive nesting" in {
      exceedsNesting.serializable shouldBe ImmArray(exceedsNestingError)
    }

    "accepts just right nesting" in {
      matchesNesting.serializable shouldBe ImmArray.empty
    }

    "outputs both error messages, without duplication" in {
      ValueList(FrontStack(exceedsNesting, ValueStruct(ImmArray.empty), exceedsNesting)).serializable shouldBe
        ImmArray(exceedsNestingError, emptyStructError)
    }
  }

  "VersionedValue" - {

    val pkgId = Ref.PackageId.assertFromString("pkgId")
    val tmplId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:Template"))

    "does not bump version when" - {

      "ensureNoCid is used " in {
        val value = VersionedValue[ContractId](ValueVersions.minVersion, ValueUnit)
        val contract = ContractInst(tmplId, value, "agreed")
        value.ensureNoCid.map(_.version) shouldBe Right(ValueVersions.minVersion)
        contract.ensureNoCid.map(_.arg.version) shouldBe Right(ValueVersions.minVersion)

      }

      "ensureNoRelCid is used " in {
        val value = VersionedValue(
          ValueVersions.minVersion,
          ValueContractId(AbsoluteContractId(Ref.ContractIdString.assertFromString("#0:0"))),
        )
        val contract = ContractInst(tmplId, value, "agreed")
        value.ensureNoRelCid.map(_.version) shouldBe Right(ValueVersions.minVersion)
        contract.ensureNoRelCid.map(_.arg.version) shouldBe Right(ValueVersions.minVersion)
      }

      "resolveRelCidV0 is used" in {
        val value = VersionedValue(
          ValueVersions.minVersion,
          ValueContractId(ValueContractId(RelativeContractId(NodeId(0), Some(randomHash())))),
        )
        val contract = ContractInst(tmplId, value, "agreed")
        val resolver: RelativeContractId => Ref.ContractIdString = {
          case RelativeContractId(NodeId(idx), _) =>
            Ref.ContractIdString.assertFromString(s"#0:$idx")
        }
        value.resolveRelCid(resolver).version shouldBe ValueVersions.minVersion
        contract.resolveRelCid(resolver).arg.version shouldBe ValueVersions.minVersion
      }

    }

  }

  "Equal" - {
    import com.digitalasset.daml.lf.value.ValueGenerators._
    import org.scalacheck.Arbitrary
    type T = VersionedValue[Unnatural[ContractId]]
    implicit val arbT: Arbitrary[T] =
      Arbitrary(versionedValueGen.map(VersionedValue.map1(Unnatural(_))))

    scalaz.scalacheck.ScalazProperties.equal.laws[T].properties foreach {
      case (s, p) => s in check(p)
    }

    "results preserve natural == results" in forAll { (a: T, b: T) =>
      scalaz.Equal[T].equal(a, b) shouldBe (a == b)
    }
  }

  private val randomHash = crypto.Hash.secureRandom
}
