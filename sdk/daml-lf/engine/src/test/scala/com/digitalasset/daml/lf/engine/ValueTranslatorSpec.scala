// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion, LookupError, Reference}
import com.daml.lf.speedy.ArrayList
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.value.Value
import com.daml.lf.value.Value._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import com.daml.lf.speedy.Compiler

import scala.util.{Failure, Success, Try}

class ValueTranslatorSpec
    extends AnyWordSpec
    with Inside
    with Matchers
    with TableDrivenPropertyChecks {

  import com.daml.lf.testing.parser.Implicits.SyntaxHelper
  import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}

  private[this] val aCid =
    ContractId.V1.assertBuild(
      crypto.Hash.hashPrivateKey("a Contract ID"),
      Bytes.assertFromString("00"),
    )

  val aInt = ValueInt64(42)
  val aText = ValueText("42")
  val aParty = ValueParty("42")
  val someText = ValueOptional(Some(aText))
  val someParty = ValueOptional(Some(aParty))
  val none = Value.ValueNone

  val nonUpgradablePkgId = Ref.PackageId.assertFromString("-non-upgradable-")

  implicit def pkgId(implicit
      parserParameter: ParserParameters[ValueTranslatorSpec.this.type]
  ): Ref.PackageId =
    parserParameter.defaultPackageId

  val (nonUpgradablePkg, nonUpgradableUserTestCases) = {
    implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
      ParserParameters(nonUpgradablePkgId, LanguageVersion.v1_15)

    val pkg =
      p"""metadata ( 'non-upgradable' : '1.0.0' )

        module Mod {
          record @serializable Record (a: *) (b: *) (c: *) (d: *) = { fieldA: a, fieldB: Option b };
          variant @serializable Variant (a: *) (b: *) (c: *) = ConsA: a | ConsB: b ;
          enum @serializable Enum = Cons1 | Cons2 ;
      }
    """
    val testCases = Table[Ast.Type, Value, List[Value], speedy.SValue](
      ("type", "normalized value", "non normalized value", "svalue"),
      (
        t"Mod:Record Int64 Text Unit Unit",
        ValueRecord("", ImmArray("" -> aInt, "" -> someText)),
        List(
          ValueRecord(
            "Mod:Record",
            ImmArray("fieldA" -> aInt, "fieldB" -> someText),
          ),
          ValueRecord(
            "Mod:Record",
            ImmArray("fieldB" -> someText, "fieldA" -> aInt),
          ),
          ValueRecord(
            "",
            ImmArray("fieldA" -> aInt, "fieldB" -> someText),
          ),
          ValueRecord(
            "Mod:Record",
            ImmArray("" -> aInt, "fieldB" -> someText),
          ),
          ValueRecord(
            "",
            ImmArray("fieldB" -> someText, "fieldA" -> aInt),
          ),
        ),
        SRecord(
          "Mod:Record",
          ImmArray("fieldA", "fieldB"),
          ArrayList(SInt64(42), SOptional(Some(SText("42")))),
        ),
      ),
      (
        t"Mod:Variant Int64 Text",
        ValueVariant("", "ConsB", ValueText("some test")),
        List(
          ValueVariant("Mod:Variant", "ConsB", ValueText("some test"))
        ),
        SVariant("Mod:Variant", "ConsB", 1, SText("some test")),
      ),
      (
        Ast.TTyCon("Mod:Enum"),
        ValueEnum("", "Cons1"),
        List(
          ValueEnum("Mod:Enum", "Cons1")
        ),
        SEnum("Mod:Enum", "Cons1", 0),
      ),
    )

    (pkg, testCases)
  }

  val upgradablePkgId = Ref.PackageId.assertFromString("-upgradable-v1-")

  val (upgradablePkg, upgradableUserTestCases) = {

    val dummyPackageId = Ref.PackageId.assertFromString("-dummy-")

    implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
      ParserParameters(upgradablePkgId, LanguageVersion.v1_17)
    val pkg = p"""metadata ( 'upgradable' : '1.0.0' )

        module Mod {
          record @serializable Record (a: *) (b: *) (c: *) (d: *) = { fieldA: a, fieldB: Option b, fieldC: Option c };
          variant @serializable Variant (a: *) (b: *) (c: *) = ConsA: a | ConsB: b ;
          enum @serializable Enum = Cons1 | Cons2 ;

          record MyCons = { head : Int64, tail: Mod:MyList };
          variant MyList = MyNil : Unit | MyCons: Mod:MyCons ;
      }
    """

    val testCases = Table[Ast.Type, Value, List[Value], speedy.SValue](
      ("type", "normalized value", "non normalized value", "svalue"),
      (
        t"Mod:Record Int64 Text Party Boolean Unit",
        ValueRecord("", ImmArray("" -> aInt, "" -> none, "" -> someParty)),
        List(
          ValueRecord(
            "Mod:Record",
            ImmArray("fieldA" -> aInt, "fieldB" -> none, "fieldC" -> someParty),
          ),
          ValueRecord(
            "Mod:Record",
            ImmArray("fieldB" -> none, "fieldC" -> someParty, "fieldA" -> aInt),
          ),
          ValueRecord("", ImmArray("fieldA" -> aInt, "fieldB" -> none, "fieldC" -> someParty)),
          ValueRecord("Mod:Record", ImmArray("" -> aInt, "fieldB" -> none, "fieldC" -> someParty)),
          ValueRecord("", ImmArray("fieldC" -> someParty, "fieldB" -> none, "fieldA" -> aInt)),
          ValueRecord("", ImmArray("" -> aInt, "" -> none, "" -> someParty)),
          ValueRecord("", ImmArray("" -> aInt, "" -> none, "" -> someParty, "" -> none)),
          ValueRecord("", ImmArray("fieldA" -> aInt, "fieldC" -> someParty)),
          ValueRecord("", ImmArray("fieldA" -> aInt, "fieldD" -> none, "fieldC" -> someParty)),
          ValueRecord(
            Some((i"Mod:Record").copy(packageId = dummyPackageId)),
            ImmArray("fieldA" -> aInt, "fieldB" -> none, "fieldC" -> someParty),
          ),
        ),
        SRecord(
          "Mod:Record",
          ImmArray("fieldA", "fieldB", "fieldC"),
          ArrayList(SInt64(42), SOptional(None), SOptional(Some(SParty("42")))),
        ),
      ),
      (
        t"Mod:Variant Int64 Text",
        ValueVariant("", "ConsB", ValueText("some test")),
        List(
          ValueVariant("Mod:Variant", "ConsB", ValueText("some test")),
          ValueVariant(
            Some(i"Mod:Variant".copy(packageId = dummyPackageId)),
            "ConsB",
            ValueText("some test"),
          ),
        ),
        SVariant("Mod:Variant", "ConsB", 1, SText("some test")),
      ),
      (
        Ast.TTyCon("Mod:Enum"),
        ValueEnum("", "Cons1"),
        List(
          ValueEnum("Mod:Enum", "Cons1"),
          ValueEnum(Some(i"Mod:Enum".copy(packageId = dummyPackageId)), "Cons1"),
        ),
        SEnum("Mod:Enum", "Cons1", 0),
      ),
    )

    (pkg -> testCases)
  }

  val upgradableV2PkgId = Ref.PackageId.assertFromString("-upgradable-v2-")

  val upgradableV2Pkg = {
    implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
      ParserParameters(upgradableV2PkgId, LanguageVersion.v1_17)
    p"""metadata ( 'upgradable' : '2.0.0' )

          module Mod {
          record @serializable Record (a: *) (b: *) (c: *) (d: *) = { fieldA: a, fieldB: Option b, fieldC: Option c, fieldD: Option d };
          variant @serializable Variant (a: *) (b: *) (c: *) = ConsA: a | ConsB: b | ConsC: b;
          enum @serializable Enum = Cons1 | Cons2 | Cons3 ;
      }
    """
  }

  implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
    ParserParameters(upgradablePkgId, LanguageVersion.v1_17)

  private[this] val compiledPackage =
    ConcurrentCompiledPackages(Compiler.Config.Dev(LanguageMajorVersion.V1))
  assert(compiledPackage.addPackage(nonUpgradablePkgId, nonUpgradablePkg) == ResultDone.Unit)
  assert(compiledPackage.addPackage(upgradablePkgId, upgradablePkg) == ResultDone.Unit)
  assert(compiledPackage.addPackage(upgradableV2PkgId, upgradableV2Pkg) == ResultDone.Unit)

  val valueTranslator = new ValueTranslator(
    compiledPackage.pkgInterface,
    checkV1ContractIdSuffixes = false,
  )
  import valueTranslator.unsafeTranslateValue

  val nonEmptyBuiltinTestCases = Table[Ast.Type, Value, List[Value], speedy.SValue](
    ("type", "normalized value", "non normalized value", "sValue"),
    (
      t"Unit",
      ValueUnit,
      List.empty,
      SValue.Unit,
    ),
    (
      t"Bool",
      ValueTrue,
      List.empty,
      SValue.True,
    ),
    (
      t"Int64",
      ValueInt64(42),
      List.empty,
      SInt64(42),
    ),
    (
      t"Timestamp",
      ValueTimestamp(Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")),
      List.empty,
      STimestamp(Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")),
    ),
    (
      t"Date",
      ValueDate(Time.Date.assertFromString("1879-03-14")),
      List.empty,
      SDate(Time.Date.assertFromString("1879-03-14")),
    ),
    (
      t"Text",
      ValueText("daml"),
      List.empty,
      SText("daml"),
    ),
    (
      t"Numeric 10",
      ValueNumeric(Numeric.assertFromString("10.0000000000")),
      List(
        ValueNumeric(Numeric.assertFromString("10.")),
        ValueNumeric(Numeric.assertFromString("10.0")),
        ValueNumeric(Numeric.assertFromString("10.00000000000000000000")),
      ),
      SNumeric(Numeric.assertFromString("10.0000000000")),
    ),
    (
      t"Party",
      ValueParty("42"),
      List.empty,
      SParty("42"),
    ),
    (
      t"ContractId Unit",
      ValueContractId(aCid),
      List.empty,
      SContractId(aCid),
    ),
    (
      t"List Text",
      ValueList(FrontStack(ValueText("a"), ValueText("b"))),
      List.empty,
      SList(FrontStack(SText("a"), SText("b"))),
    ),
    (
      t"TextMap Bool",
      ValueTextMap(SortedLookupList(Map("0" -> ValueTrue, "1" -> ValueFalse))),
      List.empty,
      SMap(true, SText("0") -> SValue.True, SText("1") -> SValue.False),
    ),
    (
      t"GenMap Int64 Text",
      ValueGenMap(ImmArray(ValueInt64(1) -> ValueText("1"), ValueInt64(42) -> ValueText("42"))),
      List(
        ValueGenMap(ImmArray(ValueInt64(42) -> ValueText("42"), ValueInt64(1) -> ValueText("1"))),
        ValueGenMap(
          ImmArray(
            ValueInt64(1) -> ValueText("0"),
            ValueInt64(42) -> ValueText("42"),
            ValueInt64(1) -> ValueText("1"),
          )
        ),
      ),
      SMap(false, SInt64(1) -> SText("1"), SInt64(42) -> SText("42")),
    ),
    (
      t"Option Text",
      ValueOptional(Some(ValueText("text"))),
      List.empty,
      SOptional(Some(SText("text"))),
    ),
  )

  val emptyBuiltinTestCase = Table[Ast.Type, Value, List[Nothing], speedy.SValue](
    ("type", "normalized value", "non normalized value", "svalue"),
    (
      t"List Text",
      ValueList(FrontStack.empty),
      List.empty,
      SList(FrontStack.empty),
    ),
    (
      t"Option Text",
      ValueOptional(None),
      List.empty,
      SOptional(None),
    ),
    (
      t"TextMap Text",
      ValueTextMap(SortedLookupList.Empty),
      List.empty,
      SMap(true),
    ),
    (
      t"GenMap Int64 Text",
      ValueGenMap(ImmArray.empty),
      List.empty,
      SMap(false),
    ),
  )

  "translateValue" should {

    val TRecordNonUpgradable = {
      implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
        ParserParameters(nonUpgradablePkgId, LanguageVersion.v1_15)
      t"Mod:Record Int64 Text Party Unit"
    }

    val TRecordUpgradable =
      t"Mod:Record Int64 Text Party Unit"

    val TVariantNonUpgradable = {
      implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
        ParserParameters(nonUpgradablePkgId, LanguageVersion.v1_15)
      t"Mod:Variant Int64 Text"
    }

    val TVariantUpgradable =
      t"Mod:Variant Int64 Text"

    val TEnumNonUpgradable = {
      implicit val parserParameters: ParserParameters[ValueTranslatorSpec.this.type] =
        ParserParameters(nonUpgradablePkgId, LanguageVersion.v1_15)
      t"Mod:Enum"
    }

    val TEnumUpgradable =
      t"Mod:Enum"

    "succeeds on any well-typed values" in {
      forEvery(
        nonUpgradableUserTestCases ++ upgradableUserTestCases ++ emptyBuiltinTestCase ++ nonEmptyBuiltinTestCases
      ) { (typ, normal, nonNormal, svalue) =>
        (normal +: nonNormal).takeRight(1).foreach { v =>
          Try(unsafeTranslateValue(typ, v)) shouldBe Success(svalue)
        }
      }
    }

    "return proper mismatch error" in {
      val testCases = Table[Ast.Type, Value, PartialFunction[Error.Preprocessing.Error, _]](
        ("type", "value", "error"),
        (
          TRecordNonUpgradable,
          ValueRecord(
            "",
            ImmArray(
              "fieldA" -> aInt
            ),
          ),
          { case Error.Preprocessing.TypeMismatch(typ, _, msg) =>
            typ shouldBe TRecordNonUpgradable
            msg should include regex "Expecting 2 field for record .*, but got 1"
          },
        ),
        (
          TRecordNonUpgradable,
          ValueRecord(
            "",
            ImmArray(
              "fieldA" -> aInt,
              "fieldB" -> someText,
              "fieldC" -> none,
            ),
          ),
          { case Error.Preprocessing.TypeMismatch(typ, _, msg) =>
            typ shouldBe TRecordNonUpgradable
            msg should include regex "Expecting 2 field for record .*, but got 3"
          },
        ),
        (
          TRecordNonUpgradable,
          ValueRecord(
            "",
            ImmArray(
              "fieldA" -> aInt,
              "fieldB" -> someText,
              "fieldC" -> someParty,
              "fieldD" -> aInt, // extra non-optional field
            ),
          ),
          { case Error.Preprocessing.TypeMismatch(typ, _, _) =>
            typ shouldBe TRecordNonUpgradable
          },
        ),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray(
              "fieldA" -> aInt,
              "fieldB" -> someParty, // Here the field has type Party instead of Text
              "fieldC" -> none,
            ),
          ),
          { case Error.Preprocessing.TypeMismatch(typ, value, _) =>
            typ shouldBe t"Text"
            value shouldBe aParty
          },
        ),
        (
          TVariantNonUpgradable,
          ValueVariant("", "ConsB", aInt), // Here the variant has type Text instead of Int64
          { case Error.Preprocessing.TypeMismatch(typ, value, _) =>
            typ shouldBe t"Text"
            value shouldBe aInt
          },
        ),
        (
          TVariantNonUpgradable,
          ValueVariant("", "ConsC", aInt), // ConsC is not a constructor of Mod:Variant
          {
            case Error.Preprocessing.Lookup(
                  LookupError.NotFound(
                    Reference.DataVariantConstructor(_, consName),
                    Reference.DataVariantConstructor(_, _),
                  )
                ) =>
              consName shouldBe "ConsC"
          },
        ),
        (
          TEnumNonUpgradable,
          ValueEnum("", "Cons3"), // Cons3 is not a constructor of Mod:Enum
          {
            case Error.Preprocessing.Lookup(
                  LookupError.NotFound(
                    Reference.DataEnumConstructor(_, consName),
                    _,
                  )
                ) =>
              consName shouldBe "Cons3"
          },
        ),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray( // fields non-order and non fully labelled.
              "fieldA" -> aInt,
              "" -> none,
              "fieldB" -> someText,
            ),
          ),
          { case Error.Preprocessing.TypeMismatch(typ, _, _) =>
            typ shouldBe TRecordUpgradable
          },
        ),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray(), // missing a non-optional field
          ),
          { case Error.Preprocessing.TypeMismatch(typ, _, _) =>
            typ shouldBe TRecordUpgradable
          },
        ),
        (
          TRecordUpgradable,
          ValueRecord(
            "",
            ImmArray(
              "fieldA" -> aInt,
              "fieldB" -> someText,
              "fieldC" -> someParty,
              "fieldD" -> aInt, // extra non-optional field
            ),
          ),
          { case Error.Preprocessing.TypeMismatch(typ, _, _) =>
            typ shouldBe TRecordUpgradable
          },
        ),
        (
          TVariantUpgradable,
          ValueVariant("", "ConsB", aInt), // Here the variant has type Text instead of Int64
          { case Error.Preprocessing.TypeMismatch(typ, value, _) =>
            typ shouldBe t"Text"
            value shouldBe aInt
          },
        ),
        (
          TVariantUpgradable,
          ValueVariant("", "ConsC", aInt), // ConsC is not a constructor of Mod:Variant
          {
            case Error.Preprocessing.Lookup(
                  LookupError.NotFound(
                    Reference.DataVariantConstructor(_, consName),
                    Reference.DataVariantConstructor(_, _),
                  )
                ) =>
              consName shouldBe "ConsC"
          },
        ),
        (
          TEnumUpgradable,
          ValueEnum("", "Cons3"), // Cons3 is not a constructor of Mod:Enum
          {
            case Error.Preprocessing.Lookup(
                  LookupError.NotFound(
                    Reference.DataEnumConstructor(_, consName),
                    Reference.DataEnumConstructor(_, _),
                  )
                ) =>
              consName shouldBe "Cons3"
          },
        ),
      )
      forEvery(testCases)((typ, value, checkError) =>
        inside(Try(unsafeTranslateValue(typ, value))) {
          case Failure(error: Error.Preprocessing.Error) =>
            checkError(error)
        }
      )
    }

    "fails on non-well type values" in {
      forAll(nonEmptyBuiltinTestCases) { (typ1, value1, _, _) =>
        forAll(nonEmptyBuiltinTestCases) { (_, value2, _, _) =>
          if (value1 != value2) {
            a[Error.Preprocessing.Error] shouldBe thrownBy(
              unsafeTranslateValue(typ1, value2)
            )
          }
        }
      }
    }

    "fails on too deep values" in {

      def mkMyList(n: Int) =
        Iterator.range(0, n).foldLeft[Value](ValueVariant("", "MyNil", ValueUnit)) { case (v, n) =>
          ValueVariant(
            "",
            "MyCons",
            ValueRecord("", ImmArray("" -> ValueInt64(n.toLong), "" -> v)),
          )
        }
      val notTooBig = mkMyList(49)
      val tooBig = mkMyList(50)
      val failure = Failure(Error.Preprocessing.ValueNesting(tooBig))

      Try(unsafeTranslateValue(t"Mod:MyList", notTooBig)) shouldBe a[Success[_]]
      Try(unsafeTranslateValue(t"Mod:MyList", tooBig)) shouldBe failure
    }

    def testCasesForCid(culprit: ContractId) = {
      val cid = ValueContractId(culprit)
      Table[Ast.Type, Value](
        ("type" -> "value"),
        t"ContractId Unit" -> cid,
        t"List (ContractId Unit)" -> ValueList(FrontStack(cid)),
        t"TextMap (ContractId Unit)" -> ValueTextMap(SortedLookupList(Map("0" -> cid))),
        t"GenMap Int64 (ContractId Unit)" -> ValueGenMap(ImmArray(ValueInt64(1) -> cid)),
        t"GenMap (ContractId Unit) Int64" -> ValueGenMap(ImmArray(cid -> ValueInt64(0))),
        t"Option (ContractId Unit)" -> ValueOptional(Some(cid)),
        t"Mod:Record (ContractId Unit) Unit Unit" -> ValueRecord(
          None,
          ImmArray("" -> cid, "" -> none),
        ),
        t"Mod:Variant (ContractId Unit) Unit Unit" -> ValueVariant("", "ConsA", cid),
      )
    }

    "accept all contract IDs when require flags are false" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.pkgInterface,
        checkV1ContractIdSuffixes = false,
      )
      val cids = List(
        ContractId.V1
          .assertBuild(
            crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"),
            Bytes.assertFromString("00"),
          ),
        ContractId.V1
          .assertBuild(crypto.Hash.hashPrivateKey("a non-suffixed V1 Contract ID"), Bytes.Empty),
      )

      cids.foreach(cid =>
        forEvery(testCasesForCid(cid))((typ, value) =>
          Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe a[Success[
            _
          ]]
        )
      )
    }

    "reject non suffixed V1 Contract IDs when requireV1ContractIdSuffix is true" in {

      val valueTranslator = new ValueTranslator(
        compiledPackage.pkgInterface,
        checkV1ContractIdSuffixes = true,
      )
      val legalCid =
        ContractId.V1.assertBuild(
          crypto.Hash.hashPrivateKey("a legal Contract ID"),
          Bytes.assertFromString("00"),
        )
      val illegalCid =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("an illegal Contract ID"), Bytes.Empty)
      val failure = Failure(Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(illegalCid))

      forEvery(testCasesForCid(legalCid))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe a[Success[_]]
      )
      forEvery(testCasesForCid(illegalCid))((typ, value) =>
        Try(valueTranslator.unsafeTranslateValue(typ, value)) shouldBe failure
      )
    }

  }

}
