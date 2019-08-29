// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf
package engine

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast.{TNat, TTyCon}
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.ValueVersion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.language.implicitConversions

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CommandPreprocessorSpec extends WordSpec with Matchers with TableDrivenPropertyChecks {

  import defaultParserParameters.{defaultPackageId => pkgId}

  private implicit def toName(s: String): Ref.Name = Ref.Name.assertFromString(s)

  val recordCon = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Record"))
  val variantCon = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Variant"))
  val enumCon = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Enum"))

  val pkg =
    p"""
        module Module {

          record Record = { field : Int64 };
          variant Variant = variant1 : Text | variant2 : Int64 ;
          enum Enum = value1 | value2;

        }

    """

  "translateValue" should {

    val testCases = Table(
      "type" -> "value",
      TUnit ->
        ValueUnit,
      TBool ->
        ValueBool(true),
      TInt64 ->
        ValueInt64(42),
      TTimestamp ->
        ValueTimestamp(Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")),
      TDate ->
        ValueDate(Time.Date.assertFromString("1879-03-14")),
      TText ->
        ValueText("daml"),
      TNumeric(TNat(10)) ->
        ValueNumeric(Numeric.assertFromString("10.0000000000")),
//      TNumeric(TNat(9)) ->
//        ValueNumeric(Numeric.assertFromString("9.000000000")),
      TParty ->
        ValueParty(Ref.Party.assertFromString("Alice")),
      TContractId(TTyCon(recordCon)) ->
        ValueContractId(AbsoluteContractId(Ref.ContractIdString.assertFromString("contractId"))),
      TList(TText) ->
        ValueList(FrontStack(ValueText("a"), ValueText("b"))),
      TMap(TBool) ->
        ValueMap(SortedLookupList(Map("0" -> ValueBool(true), "1" -> ValueBool(false)))),
      TOptional(TText) ->
        ValueOptional(Some(ValueText("text"))),
      TTyCon(recordCon) ->
        ValueRecord(None, ImmArray(Some[Ref.Name]("field") -> ValueInt64(33))),
      TTyCon(variantCon) ->
        ValueVariant(None, "variant1", ValueText("some test")),
      TTyCon(enumCon) ->
        ValueEnum(None, "value1"),
    )

    val compiledPackage = ConcurrentCompiledPackages()
    assert(compiledPackage.addPackage(pkgId, pkg) == ResultDone(()))
    val preprocessor = new CommandPreprocessor(compiledPackage)
    import preprocessor.translateValue
    val valueVersion = ValueVersion("last")

    "succeeds on well type values" in {
      forAll(testCases) { (typ, value) =>
        translateValue(typ, VersionedValue(valueVersion, value)) shouldBe a[ResultDone[_]]
      }
    }

    "fails on non-well type values" in {
      forAll(testCases) { (typ1, value1) =>
        forAll(testCases) { (_, value2) =>
          if (value1 != value2)
            translateValue(typ1, VersionedValue(valueVersion, value2)) shouldBe a[ResultError]
        }
      }
    }
  }

}
