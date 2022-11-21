// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{ChoiceName, DottedName, Name, TypeConName}
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import org.scalatest.Inside.inside
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.VectorMap

// TODO https://github.com/digital-asset/daml/issues/12051
//  Test Interface logic

class AstSpec extends AnyWordSpec with TableDrivenPropertyChecks with Matchers {

  private def defaultVersion = LanguageVersion.defaultV1

  "Package.build" should {

    "catch module name collisions" in {

      Package.build(
        List(
          Module(modName1, Map.empty, Map.empty, Map.empty, Map.empty, FeatureFlags.default),
          Module(modName2, Map.empty, Map.empty, Map.empty, Map.empty, FeatureFlags.default),
        ),
        Set.empty,
        defaultVersion,
        None,
      )
      a[PackageError] shouldBe thrownBy(
        Package.build(
          List(
            Module(modName1, Map.empty, Map.empty, Map.empty, Map.empty, FeatureFlags.default),
            Module(modName1, Map.empty, Map.empty, Map.empty, Map.empty, FeatureFlags.default),
          ),
          Set.empty,
          defaultVersion,
          None,
        )
      )

    }

  }

  "Module.build" should {

    val template = Template(
      param = Name.assertFromString("x"),
      precond = ETrue,
      signatories = eParties,
      agreementText = eText,
      choices = Map.empty,
      observers = eParties,
      key = None,
      implements = VectorMap.empty,
    )
    def interface = DefInterface(
      param = Name.assertFromString("x"),
      choices = Map.empty,
      methods = Map.empty,
      requires = Set.empty,
      coImplements = Map.empty,
      view = TUnit,
    )

    def exception = DefException(
      message = eText
    )
    val recordDef = DDataType(true, ImmArray.Empty, DataRecord(ImmArray.Empty))
    val variantDef = DDataType(true, ImmArray.Empty, DataVariant(ImmArray.Empty))
    val valDef = DValue(TUnit, EUnit, false)

    def defName(s: String) = DottedName.assertFromSegments(Iterable(s))

    "catch definition name collisions" in {

      Module.build(
        name = modName1,
        definitions = List(
          defName("def1") -> recordDef,
          defName("def2") -> recordDef,
          defName("def3") -> variantDef,
          defName("def4") -> valDef,
        ),
        templates = List(defName("def3") -> template),
        exceptions = List.empty,
        interfaces = List.empty,
        featureFlags = FeatureFlags.default,
      )

      a[PackageError] shouldBe thrownBy(
        Module.build(
          name = modName1,
          definitions = List(
            defName("def1") -> recordDef,
            defName("def2") -> recordDef,
            defName("def3") -> variantDef,
            defName("def1") -> valDef,
          ),
          templates = List(defName("def3") -> template),
          exceptions = List.empty,
          interfaces = List.empty,
          featureFlags = FeatureFlags.default,
        )
      )

    }

    "catch template collisions" in {

      Module.build(
        name = modName1,
        definitions = List(
          defName("defName1") -> recordDef,
          defName("defName2") -> recordDef,
        ),
        templates = List(
          defName("defName1") -> template
        ),
        exceptions = List.empty,
        interfaces = List.empty,
        featureFlags = FeatureFlags.default,
      )

      a[PackageError] shouldBe thrownBy(
        Module.build(
          name = modName1,
          definitions = List(
            defName("defName1") -> recordDef,
            defName("defName2") -> recordDef,
          ),
          templates = List(
            defName("defName1") -> template,
            defName("defName1") -> template,
          ),
          exceptions = List.empty,
          interfaces = List.empty,
          featureFlags = FeatureFlags.default,
        )
      )
    }

    "catch interface collisions" in {

      Module.build(
        name = modName1,
        definitions = List(
          defName("defName1") -> recordDef,
          defName("defName2") -> recordDef,
        ),
        interfaces = List(
          defName("defName1") -> interface
        ),
        exceptions = List.empty,
        templates = List.empty,
        featureFlags = FeatureFlags.default,
      )

      a[PackageError] shouldBe thrownBy(
        Module.build(
          name = modName1,
          definitions = List(
            defName("defName1") -> recordDef,
            defName("defName2") -> recordDef,
          ),
          interfaces = List(
            defName("defName1") -> interface,
            defName("defName1") -> interface,
          ),
          templates = List.empty,
          exceptions = List.empty,
          featureFlags = FeatureFlags.default,
        )
      )
    }

    "catch exception collisions" in {
      Module.build(
        name = modName1,
        definitions = List(
          defName("defName1") -> recordDef,
          defName("defName2") -> recordDef,
        ),
        templates = List.empty,
        exceptions = List(
          defName("defName1") -> exception
        ),
        interfaces = List.empty,
        featureFlags = FeatureFlags.default,
      )

      a[PackageError] shouldBe thrownBy(
        Module.build(
          name = modName1,
          definitions = List(
            defName("defName1") -> recordDef,
            defName("defName2") -> recordDef,
          ),
          templates = List.empty,
          exceptions = List(
            defName("defName1") -> exception,
            defName("defName1") -> exception,
          ),
          interfaces = List.empty,
          featureFlags = FeatureFlags.default,
        )
      )
    }

    "catch collisions between exceptions, templates and interfaces" in {
      Module.build(
        name = modName1,
        definitions = List(
          defName("defName1") -> recordDef,
          defName("defName2") -> recordDef,
          defName("defName3") -> recordDef,
        ),
        templates = List(
          defName("defName1") -> template
        ),
        exceptions = List(
          defName("defName2") -> exception
        ),
        interfaces = List(
          defName("defName3") -> interface
        ),
        featureFlags = FeatureFlags.default,
      )

      a[PackageError] shouldBe thrownBy(
        Module.build(
          name = modName1,
          definitions = List(
            defName("defName1") -> recordDef,
            defName("defName2") -> recordDef,
          ),
          templates = List(
            defName("defName1") -> template
          ),
          exceptions = List(
            defName("defName1") -> exception
          ),
          interfaces = List(
            defName("defName3") -> interface
          ),
          featureFlags = FeatureFlags.default,
        )
      )

      a[PackageError] shouldBe thrownBy(
        Module.build(
          name = modName1,
          definitions = List(
            defName("defName1") -> recordDef,
            defName("defName2") -> recordDef,
            defName("defName3") -> recordDef,
          ),
          templates = List(
            defName("defName1") -> template
          ),
          interfaces = List(
            defName("defName1") -> interface
          ),
          exceptions = List(
            defName("defName2") -> exception
          ),
          featureFlags = FeatureFlags.default,
        )
      )

      a[PackageError] shouldBe thrownBy(
        Module.build(
          name = modName1,
          definitions = List(
            defName("defName1") -> recordDef,
            defName("defName2") -> recordDef,
            defName("defName3") -> recordDef,
          ),
          templates = List(
            defName("defName1") -> template
          ),
          interfaces = List(
            defName("defName2") -> interface
          ),
          exceptions = List(
            defName("defName2") -> exception
          ),
          featureFlags = FeatureFlags.default,
        )
      )
    }

  }

  "Template.build" should {

    inside(List("choice1", "choice2", "choice3").map(Name.assertFromString)) {
      case List(choice1, choice2, choice3) =>
        "catch implements interface repetition " in {
          Template.build(
            param = Name.assertFromString("x"),
            precond = ETrue,
            signatories = eParties,
            agreementText = eText,
            choices = List.empty,
            observers = eParties,
            key = None,
            implements = List(ifaceImpl1, ifaceImpl2),
          )

          a[PackageError] shouldBe thrownBy(
            Template.build(
              param = Name.assertFromString("x"),
              precond = ETrue,
              signatories = eParties,
              agreementText = eText,
              choices = List.empty,
              observers = eParties,
              key = None,
              implements = List(ifaceImpl1, ifaceImpl1),
            )
          )
        }

        "catch choice name collisions" in {

          Template.build(
            param = Name.assertFromString("x"),
            precond = ETrue,
            signatories = eParties,
            agreementText = eText,
            choices = List(
              choiceBuilder(choice1, TUnit, EUnit),
              choiceBuilder(choice2, TBool, ETrue),
              choiceBuilder(choice3, TText, eText),
            ),
            observers = eParties,
            key = None,
            implements = List.empty,
          )

          a[PackageError] shouldBe thrownBy(
            Template.build(
              param = Name.assertFromString("x"),
              precond = ETrue,
              signatories = eParties,
              agreementText = eText,
              choices = List(
                choiceBuilder(choice1, TUnit, EUnit),
                choiceBuilder(choice2, TBool, ETrue),
                choiceBuilder(choice1, TText, eText),
              ),
              observers = eParties,
              key = None,
              implements = List.empty,
            )
          )
        }
    }
  }

  "GenDefInterface.build " should {

    inside(List("choice1", "choice2", "choice3").map(Name.assertFromString)) {
      case List(choice1, choice2, choice3) =>
        "build" in {
          DefInterface.build(
            requires = List.empty,
            param = Name.assertFromString("x"),
            choices = List(
              choiceBuilder(choice1, TUnit, EUnit),
              choiceBuilder(choice2, TBool, ETrue),
              choiceBuilder(choice3, TText, eText),
            ),
            methods = List(ifaceMethod1, ifaceMethod2),
            coImplements = List.empty,
            view = TUnit,
          )
        }

        "catch duplicate choices" in {
          a[PackageError] shouldBe thrownBy(
            DefInterface.build(
              requires = List.empty,
              param = Name.assertFromString("x"),
              choices = List(
                choiceBuilder(choice1, TUnit, EUnit),
                choiceBuilder(choice2, TBool, ETrue),
                choiceBuilder(choice1, TText, eText),
              ),
              methods = List.empty,
              coImplements = List.empty,
              view = TUnit,
            )
          )
        }

        "catch duplicate method" in {
          a[PackageError] shouldBe thrownBy(
            DefInterface.build(
              requires = List.empty,
              param = Name.assertFromString("x"),
              choices = List.empty,
              methods = List(ifaceMethod1, ifaceMethod1),
              coImplements = List.empty,
              view = TUnit,
            )
          )
        }

        "catch duplicate co-implementation" in {
          DefInterface.build(
            requires = List.empty,
            param = Name.assertFromString("x"),
            choices = List.empty,
            methods = List(ifaceMethod1, ifaceMethod2),
            coImplements = List(ifaceCoImpl1, ifaceCoImpl2),
            view = TUnit,
          )

          a[PackageError] shouldBe thrownBy(
            DefInterface.build(
              requires = List.empty,
              param = Name.assertFromString("x"),
              choices = List.empty,
              methods = List(ifaceMethod1, ifaceMethod2),
              coImplements = List(ifaceCoImpl1, ifaceCoImpl1),
              view = TUnit,
            )
          )
        }
    }
  }

  private val modName1 = DottedName.assertFromString("Mod1")
  private val modName2 = DottedName.assertFromString("Mod2")

  private val eParties = ENil(TBuiltin(BTParty))
  private val eText = EPrimLit(PLText("some text"))
  private val ifaceImpl1 = TemplateImplements(
    interfaceId = TypeConName.assertFromString("pkgId:Mod:I1"),
    InterfaceInstanceBody(
      methods = Map.empty,
      view = EAbs(
        (Name.assertFromString("this"), TUnit),
        EPrimCon(PCUnit),
        None,
      ),
    ),
  )
  private val ifaceImpl2 = TemplateImplements(
    interfaceId = TypeConName.assertFromString("pkgId:Mod:I2"),
    InterfaceInstanceBody(
      methods = Map.empty,
      view = EAbs(
        (Name.assertFromString("this"), TUnit),
        EPrimCon(PCUnit),
        None,
      ),
    ),
  )
  private val ifaceCoImpl1 = InterfaceCoImplements(
    templateId = TypeConName.assertFromString("pkgId:Mod:T1"),
    InterfaceInstanceBody(
      methods = Map.empty,
      view = EAbs(
        (Name.assertFromString("this"), TUnit),
        EPrimCon(PCUnit),
        None,
      ),
    ),
  )
  private val ifaceCoImpl2 = InterfaceCoImplements(
    templateId = TypeConName.assertFromString("pkgId:Mod:T2"),
    InterfaceInstanceBody(
      methods = Map.empty,
      view = EAbs(
        (Name.assertFromString("this"), TUnit),
        EPrimCon(PCUnit),
        None,
      ),
    ),
  )
  private val ifaceMethod1 = InterfaceMethod(name = Name.assertFromString("x"), returnType = TUnit)
  private val ifaceMethod2 = InterfaceMethod(name = Name.assertFromString("y"), returnType = TUnit)
  private def choiceBuilder(name: ChoiceName, typ: Type, expr: Expr) = TemplateChoice(
    name = name,
    consuming = true,
    controllers = eParties,
    choiceObservers = None,
    selfBinder = Name.assertFromString("self"),
    argBinder = Name.assertFromString("arg") -> TUnit,
    returnType = TUnit,
    update = EUpdate(UpdatePure(typ, expr)),
  )

}
