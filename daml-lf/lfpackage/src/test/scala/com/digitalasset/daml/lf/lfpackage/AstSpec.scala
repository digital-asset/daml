// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.lfpackage

import com.digitalasset.daml.lf.archive.LanguageVersion
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, DottedName, Name}
import com.digitalasset.daml.lf.data.Ref.Name.{assertFromString => id}
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.lfpackage.Decode.ParseError
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

class AstSpec extends WordSpec with TableDrivenPropertyChecks with Matchers {

  private def defaultVersion = LanguageVersion.defaultV1

  "Package.apply" should {

    "catch module name collisions" in {

      Package(
        List(
          Module(modName1, List.empty, List.empty, defaultVersion, FeatureFlags.default),
          Module(modName2, List.empty, List.empty, defaultVersion, FeatureFlags.default)))
      an[ParseError] shouldBe thrownBy(
        Package(
          List(
            Module(modName1, List.empty, List.empty, defaultVersion, FeatureFlags.default),
            Module(modName1, List.empty, List.empty, defaultVersion, FeatureFlags.default))))

    }

  }

  "Module.apply" should {

    val template = Template(
      param = id("x"),
      precond = eTrue,
      signatories = eParties,
      agreementText = eText,
      choices = Map.empty,
      observers = eParties,
      key = None
    )
    val recordDefWithoutTemplate =
      DDataType(true, ImmArray.empty, DataRecord(ImmArray.empty, None))
    val recordDefWithTemplate =
      DDataType(true, ImmArray.empty, DataRecord(ImmArray.empty, Some(template)))
    val variantDef = DDataType(true, ImmArray.empty, DataVariant(ImmArray.empty))
    val valDef = DValue(tUnit, false, eUnit, false)

    def defName(s: String) = DottedName.assertFromSegments(Iterable(s))

    "catch definition name collisions" in {

      Module.apply(
        name = modName1,
        definitions = List(
          defName("def1") -> recordDefWithoutTemplate,
          defName("def2") -> recordDefWithTemplate,
          defName("def3") -> variantDef,
          defName("def4") -> valDef
        ),
        templates = List.empty,
        languageVersion = defaultVersion,
        featureFlags = FeatureFlags.default,
      )

      an[ParseError] shouldBe thrownBy(
        Module.apply(
          name = modName1,
          definitions = List(
            defName("def1") -> recordDefWithoutTemplate,
            defName("def2") -> recordDefWithTemplate,
            defName("def3") -> variantDef,
            defName("def1") -> valDef
          ),
          templates = List.empty,
          languageVersion = defaultVersion,
          featureFlags = FeatureFlags.default,
        ))

    }

    "catch template collisions" in {

      Module.apply(
        name = modName1,
        definitions = List(
          defName("defName1") -> recordDefWithoutTemplate,
          defName("defName2") -> recordDefWithTemplate,
        ),
        templates = List(
          defName("defName1") -> template,
        ),
        languageVersion = defaultVersion,
        featureFlags = FeatureFlags.default,
      )

      an[ParseError] shouldBe thrownBy(
        Module.apply(
          name = modName1,
          definitions = List(
            defName("defName1") -> recordDefWithoutTemplate,
            defName("defName2") -> recordDefWithTemplate,
          ),
          templates = List(
            defName("defName1") -> template,
            defName("defName1") -> template,
          ),
          languageVersion = defaultVersion,
          featureFlags = FeatureFlags.default,
        ))
    }

    "catch templates without record definition" in {

      Module.apply(
        name = modName1,
        definitions = List(
          defName("defName1") -> recordDefWithoutTemplate,
          defName("defName2") -> recordDefWithTemplate,
        ),
        templates = List(
          defName("defName1") -> template,
        ),
        languageVersion = defaultVersion,
        featureFlags = FeatureFlags.default,
      )

      an[ParseError] shouldBe thrownBy(
        Module.apply(
          name = modName1,
          definitions = List(
            defName("defName1") -> recordDefWithoutTemplate,
            defName("defName2") -> recordDefWithTemplate,
          ),
          templates = List(
            defName("defName3") -> template,
          ),
          languageVersion = defaultVersion,
          featureFlags = FeatureFlags.default,
        ))
    }

  }

  "Template.apply" should {

    def builder(name: ChoiceName, typ: Type, expr: Expr) = TemplateChoice(
      name = name,
      consuming = true,
      controllers = eParties,
      selfBinder = id("self"),
      argBinder = (None, tUnit),
      returnType = tUnit,
      update = EUpdate(UpdatePure(typ, expr)),
    )

    val List(choice1, choice2, choice3) =
      List("choice1", "choice2", "choice3").map(Name.assertFromString)

    "catch choice name collisions" in {

      Template(
        param = id("x"),
        precond = eTrue,
        signatories = eParties,
        agreementText = eText,
        choices = List(
          choice1 -> builder(choice1, tUnit, eUnit),
          choice2 -> builder(choice2, tBool, eTrue),
          choice3 -> builder(choice3, tText, eText)
        ),
        observers = eParties,
        key = None
      )

      an[ParseError] shouldBe thrownBy(
        Template(
          param = id("x"),
          precond = eTrue,
          signatories = eParties,
          agreementText = eText,
          choices = List(
            choice1 -> builder(choice1, tUnit, eUnit),
            choice2 -> builder(choice2, tBool, eTrue),
            choice1 -> builder(choice1, tText, eText)
          ),
          observers = eParties,
          key = None
        ))
    }
  }

  private val modName1 = DottedName.assertFromString("Mod1")
  private val modName2 = DottedName.assertFromString("Mod2")

  private val tUnit = TBuiltin(BTUnit)
  private val tBool = TBuiltin(BTUnit)
  private val tText = TBuiltin(BTUnit)
  private val eParties = ENil(TBuiltin(BTParty))
  private val eText = EPrimLit(PLText("some text"))
  private val eUnit = EPrimCon(PCUnit)
  private val eTrue = EPrimCon(PCTrue)

}
