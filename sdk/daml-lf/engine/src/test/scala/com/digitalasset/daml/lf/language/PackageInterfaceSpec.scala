// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.PackageInterface.{
  DataEnumInfo,
  DataRecordInfo,
  DataVariantInfo,
}
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import org.scalatest.{Assertion, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop._
import org.scalatest.wordspec.AnyWordSpec

class PackageInterfaceSpecV2 extends PackageInterfaceSpec(LanguageMajorVersion.V2)

class PackageInterfaceSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
  import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.{
    defaultPackageId => _,
    _,
  }

  // TODO https://github.com/digital-asset/daml/issues/12051
  //  test interfaces

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

  private[this] implicit val defaultPackageId: Ref.PackageId =
    parserParameters.defaultPackageId

  private[this] val pkg =
    p"""metadata ( 'pkg' : '1.0.0' )

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
            };

       }
    """

  private[this] val pkgInterface = PackageInterface(Map(defaultPackageId -> pkg))

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
          inside(lookup(input)) { case Left(LookupError.NotFound(notFound, context)) =>
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
      Reference.Module(defaultPackageId, "AnotherModule"),
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
      Reference.Module(defaultPackageId, "AnotherModule"),
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
      Reference.Module(defaultPackageId, "AnotherModule"),
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
    toContext = Reference.Template(_: Ref.TypeConName),
  )(
    nonErrorCase = ("Mod:Contract": Identifier) -> { case template =>
      template.param shouldBe "this"
    },
    errorCases = Identifier("another package", "Mod:Contract") ->
      Reference.Package("another package"),
    ("AnotherModule:Contract": Identifier) ->
      Reference.Module(defaultPackageId, "AnotherModule"),
    ("Mod:unit": Identifier) ->
      Reference.Template("Mod:unit": Ref.TypeConName),
  )

}
