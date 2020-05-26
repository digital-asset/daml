// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{ChoiceName, DottedName, Name}
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

class AstSpec extends WordSpec with TableDrivenPropertyChecks with Matchers {

  private def defaultVersion = LanguageVersion.defaultV1

  "Package.apply" should {

    "catch module name collisions" in {

      Package(
        List(
          Module(modName1, List.empty, List.empty, defaultVersion, FeatureFlags.default),
          Module(modName2, List.empty, List.empty, defaultVersion, FeatureFlags.default)),
        Set.empty,
        None
      )
      a[PackageError] shouldBe thrownBy(
        Package(
          List(
            Module(modName1, List.empty, List.empty, defaultVersion, FeatureFlags.default),
            Module(modName1, List.empty, List.empty, defaultVersion, FeatureFlags.default)),
          Set.empty,
          None
        ))

    }

  }

  "Module.apply" should {

    val template = Template(
      param = Name.assertFromString("x"),
      precond = ETrue,
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
    val valDef = DValue(TUnit, false, EUnit, false)

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

      a[PackageError] shouldBe thrownBy(
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

      a[PackageError] shouldBe thrownBy(
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

      a[PackageError] shouldBe thrownBy(
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
      selfBinder = Name.assertFromString("self"),
      argBinder = Name.assertFromString("arg") -> TUnit,
      returnType = TUnit,
      update = EUpdate(UpdatePure(typ, expr)),
    )

    val List(choice1, choice2, choice3) =
      List("choice1", "choice2", "choice3").map(Name.assertFromString)

    "catch choice name collisions" in {

      Template(
        param = Name.assertFromString("x"),
        precond = ETrue,
        signatories = eParties,
        agreementText = eText,
        choices = List(
          choice1 -> builder(choice1, TUnit, EUnit),
          choice2 -> builder(choice2, TBool, ETrue),
          choice3 -> builder(choice3, TText, eText)
        ),
        observers = eParties,
        key = None
      )

      a[PackageError] shouldBe thrownBy(
        Template(
          param = Name.assertFromString("x"),
          precond = ETrue,
          signatories = eParties,
          agreementText = eText,
          choices = List(
            choice1 -> builder(choice1, TUnit, EUnit),
            choice2 -> builder(choice2, TBool, ETrue),
            choice1 -> builder(choice1, TText, eText)
          ),
          observers = eParties,
          key = None
        ))
    }
  }

  private val modName1 = DottedName.assertFromString("Mod1")
  private val modName2 = DottedName.assertFromString("Mod2")

  private val eParties = ENil(TBuiltin(BTParty))
  private val eText = EPrimLit(PLText("some text"))

}
