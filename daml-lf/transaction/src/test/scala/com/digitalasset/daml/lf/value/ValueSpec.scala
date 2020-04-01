// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, Unnatural}
import com.digitalasset.daml.lf.value.Value._
import Ref.{Identifier, Name}
import ValueGenerators.{idGen, nameGen}
import TypedValueGenerators.{RNil, genAddend, ValueAddend => VA}

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalatest.{FreeSpec, Matchers}
import scalaz.{Order, Tag}
import scalaz.std.anyVal._
import scalaz.syntax.functor._
import scalaz.scalacheck.{ScalazProperties => SzP}
import scalaz.scalacheck.ScalaCheckBinding._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ValueSpec
    extends FreeSpec
    with Matchers
    with Checkers
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks {
  import ValueSpec._

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
          ValueContractId(AbsoluteContractId.assertFromString("#0:0")),
        )
        val contract = ContractInst(tmplId, value, "agreed")
        value.ensureNoRelCid.map(_.version) shouldBe Right(ValueVersions.minVersion)
        contract.ensureNoRelCid.map(_.arg.version) shouldBe Right(ValueVersions.minVersion)
      }

      "resolveRelCidV0 is used" in {
        val value = VersionedValue(
          ValueVersions.minVersion,
          ValueContractId(ValueContractId(RelativeContractId(NodeId(0)))),
        )
        val contract = ContractInst(tmplId, value, "agreed")
        val resolver: RelativeContractId => Ref.ContractIdString = {
          case RelativeContractId(NodeId(idx)) =>
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

  // XXX can factor like FlatSpecCheckLaws
  private def checkLaws(props: org.scalacheck.Properties) =
    forEvery(Table(("law", "property"), props.properties: _*)) { (_, p) =>
      check(p, minSuccessful(20))
    }

  "Order" - {
    type Cid = Int
    type T = Value[Cid]

    val FooScope: Value.LookupVariantEnum =
      Map(fooVariantId -> ImmArray("quux", "baz"), fooEnumId -> ImmArray("quux", "baz"))
        .transform((_, ns) => ns map Ref.Name.assertFromString)
        .lift

    "for primitive, matching types" - {
      val EmptyScope: Value.LookupVariantEnum = _ => None
      implicit val ord: Order[T] = Tag unsubst Value.orderInstance(EmptyScope)

      "obeys order laws" in forAll(genAddend, minSuccessful(100)) { va =>
        implicit val arb: Arbitrary[T] = va.injarb[Cid] map va.inj
        checkLaws(SzP.order.laws[T])
      }
    }

    "for record and variant types" - {
      implicit val ord: Order[T] = Tag unsubst Value.orderInstance(FooScope)
      "obeys order laws" in forEvery(Table("va", fooRecord, fooVariant)) { va =>
        implicit val arb: Arbitrary[T] = va.injarb[Cid] map va.inj
        checkLaws(SzP.order.laws[T])
      }
    }

    "for enum types" - {
      "obeys order laws" in forAll(enumDetailsAndScopeGen, minSuccessful(20)) {
        case (details, scope) =>
          implicit val ord: Order[T] = Tag unsubst Value.orderInstance(scope)
          forEvery(Table("va", details.values.toSeq: _*)) { ea =>
            implicit val arb: Arbitrary[T] = ea.injarb[Cid] map ea.inj
            checkLaws(SzP.order.laws[T])
          }
      }
    }
  }

}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object ValueSpec {
  private val fooSpec = {
    import shapeless.syntax.singleton._
    'quux ->> VA.int64 :: 'baz ->> VA.int64 :: RNil
  }
  private val (_, fooRecord) = VA.record(Identifier assertFromString "abc:Foo:FooRec", fooSpec)
  private val fooVariantId = Identifier assertFromString "abc:Foo:FooVar"
  private val (_, fooVariant) = VA.variant(fooVariantId, fooSpec)
  private val fooEnumId = Identifier assertFromString "abc:Foo:FooEnum"

  private[this] val scopeOfEnumsGen: Gen[Map[Identifier, Seq[Name]]] =
    Gen.mapOf(Gen.zip(idGen, Gen.nonEmptyContainerOf[Set, Name](nameGen) map (_.toSeq)))

  private val enumDetailsAndScopeGen
    : Gen[(Map[Identifier, VA.EnumAddend[Seq[Name]]], Value.LookupVariantEnum)] =
    scopeOfEnumsGen flatMap { details =>
      (
        details transform ((name, members) => VA.enum(name, members)._2),
        details.transform((_, members) => members.to[ImmArray]).lift)
    }
  /*
  private val genFoos: Gen[(ImmArray[Name], ValueAddend)] =
    Gen.nonEmptyContainerOf[ImmArray, Name](ValueGenerators.nameGen)
    Gen.oneOf(
    fooRecord,
    fooVariant,
     map ()
  )
 */
}
