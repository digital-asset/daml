// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

import com.daml.lf.data.Ref.Identifier
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.PackageInterface.{DataEnumInfo, DataRecordInfo, DataVariantInfo}
import org.scalatest.{Assertion, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop._
import org.scalatest.wordspec.AnyWordSpec

class PackageInterfaceSpec
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import com.daml.lf.testing.parser.Implicits._
  import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}

  // TODO https://github.com/digital-asset/daml/issues/12051
  //  test interfaces

  private[this] implicit val defaultPkgId: Ref.PackageId =
    defaultParserParameters.defaultPackageId

  private[this] val pkg =
    p"""
        module Mod {

          val unit: Unit = ();

          record @serializable Tuple X Y = { fst: X, snd: Y };
          variant @serializable Either a b = Left : a | Right : b ;
          enum @serializable Color = Red | Green | Blue;

          record @serializable Contract = {};
          template (this : Contract) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
            };

       }
    """

  private[this] val pkgInterface = PackageInterface(Map(defaultPkgId -> pkg))

  private[this] def test[X, Y](
      description: String,
      lookup: X => Either[LookupError, Y],
      toContext: X => Reference,
  )(nonErrorCase: (X, PartialFunction[Y, Assertion]), errorCases: (X, Reference)*) =
    s"Lookup$description" should {

      s"succeed on known ${description.toLowerCase()}" in {
        val (successfulInput, validateSuccess) = nonErrorCase
        inside(lookup(successfulInput)) { case Right(success) => inside(success)(validateSuccess) }
      }

      s"fail on unknown ${description.toLowerCase()}" in {
        val table = Table("input" -> "reference", errorCases: _*)

        forEvery(table) { (input, expectedNotFound) =>
          inside(lookup(input)) { case Left(LookupError(notFound, context)) =>
            notFound shouldBe expectedNotFound
            context shouldBe toContext(input)
          }
        }
      }
    }

  test(
    description = "DataRecord",
    lookup = pkgInterface.lookupDataRecord,
    toContext = Reference.DataRecord,
  )(
    nonErrorCase =
      ("Mod:Tuple": Identifier) -> { case DataRecordInfo(dataType, DataRecord(variants)) =>
        dataType.params shouldBe ImmArray("X" -> KStar, "Y" -> KStar)
        variants shouldBe ImmArray("fst" -> TVar("X"), "snd" -> TVar("Y"))
      },
    errorCases = Identifier("another package", "Mod:Tuple") ->
      Reference.Package("another package"),
    ("AnotherModule:Tuple": Identifier) ->
      Reference.Module(defaultPkgId, "AnotherModule"),
    ("Mod:MyTuple": Identifier) ->
      Reference.Definition("Mod:MyTuple"),
    ("Mod:unit": Identifier) ->
      Reference.DataType("Mod:unit"),
    ("Mod:Either": Identifier) ->
      Reference.DataRecord("Mod:Either"),
  )

  test(
    description = "DataVariant",
    lookup = pkgInterface.lookupDataVariant,
    toContext = Reference.DataVariant,
  )(
    nonErrorCase =
      ("Mod:Either": Identifier) -> { case DataVariantInfo(dataType, DataVariant(fields)) =>
        dataType.params shouldBe ImmArray("a" -> KStar, "b" -> KStar)
        fields shouldBe ImmArray("Left" -> TVar("a"), "Right" -> TVar("b"))
      },
    errorCases = Identifier("another package", "Mod:Either") ->
      Reference.Package("another package"),
    ("AnotherModule:Either": Identifier) ->
      Reference.Module(defaultPkgId, "AnotherModule"),
    ("Mod:MyEither": Identifier) ->
      Reference.Definition("Mod:MyEither"),
    ("Mod:unit": Identifier) ->
      Reference.DataType("Mod:unit"),
    ("Mod:Tuple": Identifier) ->
      Reference.DataVariant("Mod:Tuple"),
  )

  test(
    description = "DataEnum",
    lookup = pkgInterface.lookupDataEnum,
    toContext = Reference.DataEnum,
  )(
    nonErrorCase =
      ("Mod:Color": Identifier) -> { case DataEnumInfo(dataType, DataEnum(constructors)) =>
        dataType.params shouldBe ImmArray.empty
        constructors shouldBe ImmArray("Red", "Green", "Blue")
      },
    errorCases = Identifier("another package", "Mod:Color") ->
      Reference.Package("another package"),
    ("AnotherModule:Color": Identifier) ->
      Reference.Module(defaultPkgId, "AnotherModule"),
    ("Mod:MyColor": Identifier) ->
      Reference.Definition("Mod:MyColor"),
    ("Mod:unit": Identifier) ->
      Reference.DataType("Mod:unit"),
    ("Mod:Tuple": Identifier) ->
      Reference.DataEnum("Mod:Tuple"),
  )

  test(
    description = "Template",
    lookup = pkgInterface.lookupTemplate,
    toContext = Reference.Template,
  )(
    nonErrorCase = ("Mod:Contract": Identifier) -> { case template =>
      template.param shouldBe "this"
    },
    errorCases = Identifier("another package", "Mod:Contract") ->
      Reference.Package("another package"),
    ("AnotherModule:Contract": Identifier) ->
      Reference.Module(defaultPkgId, "AnotherModule"),
    ("Mod:unit": Identifier) ->
      Reference.Template("Mod:unit"),
  )

  "interfaceImplementations" should {

    import Identifier.{assertFromString => str2Id}

    def test(
        description: String,
        pkg: => Package,
        expectedResult: Map[Identifier, Set[Identifier]],
    ) =
      description in {
        PackageInterface(Map(defaultPkgId -> pkg))
          .interfaceImplementations(defaultPkgId) shouldBe Right(expectedResult)
      }

    test("return nothing when given package that do not talk about interface", pkg, Map.empty)

    test(
      "return interface implemented",
      p"""
        module Mod1 {
          record @serializable T1 = {};
          template (this : T1) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              implements 'pkg3':Mod3:I3 {};
              implements 'pkg4':Mod4:I4 {};
            };
        }

        module Mod2 {
          record @serializable T2 = {};
          template (this : T2) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              implements 'pkg3':Mod3:I3 {};
              implements 'pkg5':Mod5:I5 {};
            };
        }
        """,
      Map(
        str2Id("pkg3:Mod3:I3") -> Set(
          str2Id(s"$defaultPkgId:Mod1:T1"),
          str2Id(s"$defaultPkgId:Mod2:T2"),
        ),
        str2Id("pkg4:Mod4:I4") -> Set(str2Id(s"$defaultPkgId:Mod1:T1")),
        str2Id("pkg5:Mod5:I5") -> Set(str2Id(s"$defaultPkgId:Mod2:T2")),
      ),
    )

    test(
      "return template coimplemented",
      p"""
         module Mod1 {
           interface (this: I1) = {
             precondition True;
             coimplements 'pkg3':Mod3:T3 {};
             coimplements 'pkg4':Mod4:T4 {};
           }; 
         }

         module Mod2 {
           interface (this: I2) = {
             precondition True;
             coimplements 'pkg3':Mod3:T3 {};
             coimplements 'pkg5':Mod5:T5 {};
           };
         }
     """,
      Map(
        str2Id(s"$defaultPkgId:Mod1:I1") -> Set(str2Id("pkg3:Mod3:T3"), str2Id("pkg4:Mod4:T4")),
        str2Id(s"$defaultPkgId:Mod2:I2") -> Set(str2Id("pkg3:Mod3:T3"), str2Id("pkg5:Mod5:T5")),
      ),
    )

    test(
      "return interface complex implementation",
      p"""
        module Mod1 {
          record @serializable T1 = {};
          template (this : T1) =  {
            precondition True;
            signatories Nil @Party;
            observers Nil @Party;
            agreement "Agreement";
            implements Mod1:I1 {};
            implements 'pkg3':Mod3:I3 {};
          };
          interface (this: I1) = {
            precondition True;
          };   
        }

        module Mod2 {
          record @serializable T2 = {};
          template (this : T2) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              implements Mod2:I2 {};
              implements 'pkg3':Mod3:I3 {};
            };
            interface (this: I2) = {
             precondition True;
             coimplements Mod1:T1 {};
             coimplements 'pkg4':Mod4:T4 {};
           };   
            
        }
        """,
      Map(
        str2Id(s"$defaultPkgId:Mod1:I1") -> Set(
          str2Id(s"$defaultPkgId:Mod1:T1")
        ),
        str2Id(s"$defaultPkgId:Mod2:I2") -> Set(
          str2Id(s"$defaultPkgId:Mod1:T1"),
          str2Id(s"$defaultPkgId:Mod2:T2"),
          str2Id("pkg4:Mod4:T4"),
        ),
        str2Id("pkg3:Mod3:I3") -> Set(
          str2Id(s"$defaultPkgId:Mod1:T1"),
          str2Id(s"$defaultPkgId:Mod2:T2"),
        ),
      ),
    )

  }
}
