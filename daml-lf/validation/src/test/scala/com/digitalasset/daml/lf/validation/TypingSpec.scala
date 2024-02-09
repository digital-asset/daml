// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref.{DottedName, PackageName, PackageVersion}
import com.daml.lf.language.Ast._
import com.daml.lf.language.{
  Ast,
  LanguageMajorVersion,
  LookupError,
  PackageInterface,
  Reference,
  LanguageVersion => LV,
}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.ParserParameters
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TypingSpecV2 extends TypingSpec(LanguageMajorVersion.V2)

class TypingSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with TableDrivenPropertyChecks
    with Matchers {

  private[this] implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor(majorLanguageVersion)
  private[this] val defaultPackageId = parserParameters.defaultPackageId
  private[this] val defaultLanguageVersion = parserParameters.languageVersion
  private[this] val packageMetadata = Ast.PackageMetadata(
    PackageName.assertFromString("pkg"),
    PackageVersion.assertFromString("0.0.0"),
    None,
  )

  import SpecUtil._

  "checkKind" should {
    // TEST_EVIDENCE: Integrity: ill-formed kinds are rejected
    "reject invalid kinds" in {

      val negativeTestCases = Table(
        "kinds",
        k"*",
        k"* -> *",
        k"* -> * -> *",
        k"(* -> *) -> *",
        k"(nat -> *)",
        k"nat -> * -> *",
        k"* -> nat -> *",
      )

      val positiveTestCases = Table(
        "kinds",
        k"* -> nat",
        k"* -> * -> nat",
        k"(* -> nat) -> *",
      )

      forEvery(negativeTestCases)(env.checkKind)
      forEvery(positiveTestCases)(k => an[ENatKindRightOfArrow] shouldBe thrownBy(env.checkKind(k)))
    }
  }

  "kindOf" should {

    // TEST_EVIDENCE: Integrity: ensure builtin operators have the correct type
    "infers the proper kind for builtin types (but ContractId)" in {
      val testCases = Table(
        "builtin type" -> "expected kind",
        BTInt64 -> k"*",
        BTNumeric -> k"nat -> *",
        BTText -> k"*",
        BTTimestamp -> k"*",
        BTParty -> k"*",
        BTUnit -> k"*",
        BTBool -> k"*",
        BTList -> k"* -> *",
        BTTextMap -> k"* -> *",
        BTGenMap -> k"* -> * -> *",
        BTUpdate -> k"* -> *",
        BTScenario -> k"* -> *",
        BTDate -> k"*",
        BTContractId -> k"* -> *",
        BTArrow -> k"* -> * -> *",
        BTAny -> k"*",
        BTRoundingMode -> k"*",
        BTBigNumeric -> k"*",
        BTAnyException -> k"*",
      )

      forEvery(testCases) { (bType: BuiltinType, expectedKind: Kind) =>
        env.kindOf(TBuiltin(bType)) shouldBe expectedKind
      }
    }

    "infers the proper kind for complex types" in {

      val testCases = Table(
        "type" -> "expected kind",
        t"Mod:T" -> k"*",
        t"Mod:R" -> k"* -> *",
        t"List Bool" -> k"*",
        t"Mod:R" -> k"* -> *",
        t"Mod:R Mod:T" -> k"*",
        t"ContractId Mod:T" -> k"*",
        t"Unit -> Unit" -> k"*",
        t"forall (a:*) . a -> Bool -> a" -> k"*",
        t"forall (a:*) (b:*) . a -> b -> a" -> k"*",
        t"< f1 : Int64, f2 : Bool >" -> k"*",
        t"Arrow Int64" -> k"* -> *",
      )

      forEvery(testCases) { (typ: Type, expectedKind: Kind) =>
        env.kindOf(typ) shouldBe expectedKind
      }
    }

    // TEST_EVIDENCE: Integrity: ill-formed types are rejected
    "reject ill-formed types" in {
      an[ENatKindRightOfArrow] shouldBe thrownBy(env.kindOf(T"""∀ (τ: ⋆ → nat). Unit """))
    }
  }

  "typeOf" should {

    // TEST_EVIDENCE: Integrity: ensure expression forms have the correct type

    "infers the proper type for expression" in {
      // The part of the expression that corresponds to the expression
      // defined by the given rule should be wrapped in double
      // parentheses.
      val testCases = Table(
        "expression" ->
          "expected type",
        // ExpDefVar
        E"Λ (τ : ⋆) (σ: ⋆). λ (x: σ) → λ (x: τ) → (( x ))" ->
          T"∀ (τ: ⋆) (σ:⋆). σ → τ → (( τ ))",
        // ExpApp
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₁ → τ₂) (e₂ : τ₁) → (( e₁ e₂ ))" ->
          T"∀ (τ₁: ⋆) (τ₂ : ⋆) . (τ₁ → τ₂) → τ₁ → (( τ₂ ))",
        // ExpTyApp
        E"Λ (τ : ⋆) (σ: ⋆ → ⋆). λ (e : ∀ (α : ⋆). σ α) → (( e @τ ))" ->
          T"∀ (τ: ⋆) (σ: ⋆ → ⋆). (∀ (α : ⋆). σ α) → (( σ τ ))",
        // ExpAbs
        E"Λ  (τ : ⋆) (σ: ⋆) . λ (e: τ → σ) → λ (x : τ) → (( e x ))" ->
          T"∀ (τ : ⋆) (σ: ⋆) . (τ → σ) → τ → (( σ ))",
        // ExpTyAbs
        E"Λ (τ : ⋆). λ (e: τ )  →  (( Λ (α : ⋆ → ⋆) . e ))" ->
          T"∀ (τ : ⋆). τ  →  (( ∀ (α : ⋆ → ⋆) . τ ))",
        // ExpLet
        E"Λ (τ : ⋆) (σ: ⋆). λ (e₁ : τ) (e₂ : τ → σ) → (( let x : τ = e₁ in e₂ x ))" ->
          T"∀ (τ : ⋆) (σ: ⋆). τ → (τ → σ) → (( σ ))",
        // ExpUnit
        E"(( () ))" -> T"(( Unit ))",
        // ExpTrue
        E"(( True ))" -> T"(( Bool ))",
        // ExpFalse
        E"(( False ))" -> T"(( Bool ))",
        // ExpListNil
        E"Λ (τ : ⋆). (( Nil @τ ))" -> T"∀ (τ : ⋆). (( List τ ))",
        // ExpListCons
        E"Λ (τ : ⋆). λ (e₁ : τ) (e₂ : τ) (e : List τ) → (( Cons @τ [e₁, e₂] e ))" ->
          T"∀ (τ : ⋆). τ → τ → List τ → (( List τ ))",
        // ExpOptionNone
        E"Λ (τ : ⋆) . (( None @τ ))" ->
          T"∀ (τ : ⋆) . (( Option τ ))",
        // ExpOptionSome
        E"Λ (τ : ⋆). λ (e : τ) → (( Some @τ e ))" ->
          T"∀ (τ : ⋆). τ → (( Option τ ))",
        // ExpLitInt64
        E"(( 42 ))" -> T"Int64",
        // ExpLitNumeric
        E"(( 3.1415926536 ))" -> T"(( Numeric 10 ))",
        // ExpLitText
        E"""(( "text" ))""" -> T"(( Text ))",
        // ExpLitDate
        E"(( 1879-03-14 ))" -> T"(( Date ))",
        // ExpLitTimestamp
        E"(( 1969-07-20T20:17:00.000000Z ))" -> T"(( Timestamp ))",
        // TextMap
        E"Λ (τ : ⋆) . (( TEXTMAP_EMPTY @τ ))" -> T"∀ (τ : ⋆) . (( TextMap τ ))",
        // GenMap
        E"Λ (τ : ⋆) (σ : ⋆). (( GENMAP_EMPTY @τ @σ ))" -> T"∀ (τ : ⋆) (σ : ⋆) . (( GenMap τ σ ))",
        // ExpVal
        E"(( Mod:f ))" -> T"(( Int64 →  Bool ))",
        // ExpRecCon
        E"Λ (σ : ⋆). λ (e₁ : Int64) (e₂ : List σ) → (( Mod:R @σ { f1 = e₁, f2 =e₂ } )) " ->
          T"∀ (σ : ⋆) . Int64 → List σ → (( Mod:R σ ))",
        // ExpRecProj
        E"Λ (σ : ⋆). λ (e : Mod:R σ) → (( Mod:R @σ {f2} e ))" ->
          T"∀ (σ : ⋆).  Mod:R σ →(( List σ  ))",
        // ExpRecUpdate
        E"Λ (σ : ⋆). λ (e : Mod:R σ) (e₂ : List σ) → (( Mod:R @σ { e  with f2 = e₂ } ))" ->
          T"∀ (σ : ⋆). Mod:R σ → List σ → (( Mod:R σ ))",
        // ExpVarCon
        E"Λ (σ : ⋆). λ (e : σ) → (( Mod:Tree:Leaf @σ e ))" ->
          T"∀ (σ : ⋆). σ  → (( Mod:Tree σ ))",
        // ExpEnumCon
        E"(( Mod:Color:Blue ))" ->
          T"Mod:Color",
        // ExpStructCon
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e₁ : τ₁) (e₂ : τ₂)  →  (( ⟨ f₁ = e₁, f₂ = e₂ ⟩ ))" ->
          T"∀ (τ₁ : ⋆) (τ₂ : ⋆). τ₁ → τ₂ → (( ⟨ f₁: τ₁, f₂: τ₂ ⟩ ))",
        // ExpStructProj
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) → (( (e).f₂ ))" ->
          T"∀ (τ₁ : ⋆) (τ₂ : ⋆) . ⟨ f₁: τ₁, f₂: τ₂ ⟩ → (( τ₂ ))",
        // ExpStructUpdate
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) (e₂ : τ₂)  → (( ⟨ e with f₂ = e₂ ⟩ ))" ->
          T"∀ (τ₁ : ⋆) (τ₂ : ⋆) . ⟨ f₁: τ₁, f₂: τ₂ ⟩ → τ₂ → (( ⟨ f₁: τ₁, f₂: τ₂ ⟩ ))",
        // ExpCaseVariant
        E"Λ (τ : ⋆). λ (e : Mod:Tree τ) → (( case e of Mod:Tree:Node x → (x).left | Mod:Tree:Leaf x -> e ))" ->
          T"∀ (τ : ⋆). Mod:Tree τ →  ((  Mod:Tree τ  ))",
        // ExpCaseEnum
        E"λ (e : Mod:Color) → (( case e of Mod:Color:Red → True | Mod:Color:Green → False | Mod:Color:Blue → False  ))" ->
          T"Mod:Color → (( Bool ))",
        // ExpCaseNil & ExpCaseCons
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : List τ) (c: σ) (f: τ → List τ → σ) → (( case e of Nil → c | Cons x y → f x y ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). List τ → σ → (τ → List τ → σ) → (( σ ))",
        // ExpCaseFalse & ExpCaseTrue
        E"Λ (σ : ⋆). λ (e : Bool) (e₁: σ) (e₂: σ) → (( case e of True → e₁ | False → e₂ ))" ->
          T"∀ (σ : ⋆). Bool → σ →  σ → (( σ ))",
        // ExpCaseUnit
        E"Λ (σ : ⋆). λ (e₁ : Unit) (e₂: σ) → (( case e₁ of () → e₂ ))" ->
          T"∀ (σ : ⋆). Unit → σ → (( σ ))",
        // ExpDefault
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁ : τ) (e₂: σ) → (( case e₁ of _ → e₂ ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). τ → σ → (( σ ))",
        // ExpToAny
        E"""λ (t : Mod:T) → (( to_any @Mod:T t ))""" ->
          T"Mod:T → Any",
        E"""λ (t : Mod:R Text) → (( to_any @(Mod:R Text) t ))""" ->
          T"Mod:R Text → Any",
        E"""λ (t : Text) → (( to_any @Text t ))""" ->
          T"Text → Any",
        E"""λ (t : Int64) → (( to_any @Int64 t ))""" ->
          T"Int64 -> Any",
        // ExpFromAny
        E"""λ (t: Any) → (( from_any @Mod:T t ))""" ->
          T"Any → Option Mod:T",
        E"""λ (t: Any) → (( from_any @(Mod:R Text) t ))""" ->
          T"Any → Option (Mod:R Text)",
        E"""λ (t: Any) → (( from_any @Text t ))""" ->
          T"Any → Option Text",
        E"""λ (t: Any) → (( from_any @Int64 t ))""" ->
          T"Any → Option Int64",
        // ExpTypeRep
        E"""(( type_rep @Mod:T ))""" ->
          T"TypeRep",
        E"""(( type_rep @Int64 ))""" ->
          T"TypeRep",
        E"""(( type_rep @(Mod:Tree (List Text)) ))""" ->
          T"TypeRep",
        E"""(( type_rep @((ContractId Mod:T) → Mod:Color) ))""" ->
          T"TypeRep",
        // ExpRoundingMode,
        E"""ROUNDING_UP""" ->
          T"RoundingMode",
        // ExpThrow
        E"Λ (σ : ⋆). λ (e : Mod:E) →  (( throw @σ @Mod:E e ))" ->
          T"∀ (σ : ⋆). Mod:E → (( σ ))",
        // ExpToAnyException
        E"λ (e : Mod:E) → (( to_any_exception @Mod:E e ))" ->
          T"Mod:E → (( AnyException ))",
        // ExpFromAnyException
        E"λ (e : AnyException) → (( from_any_exception @Mod:E e ))" ->
          T"AnyException → (( Option Mod:E ))",
        // AnyException built-ins
        E"ANY_EXCEPTION_MESSAGE" -> T"AnyException → Text",
        // UpdTryCatch
        E"Λ (σ : ⋆). λ (e₁ : Update σ) (e₂: AnyException → Option (Update σ)) → (( try @σ e₁ catch x → e₂ x ))" ->
          T"∀ (σ : ⋆). Update σ → (AnyException → Option (Update σ)) → Update σ",
        // EExperimental
        E"experimental ANSWER (Unit -> Int64)" ->
          T"Unit -> Int64",
        // Forall shadowing
        E"(Λ (τ : ⋆) (τ : ⋆). λ (e₁ : τ)  → 0) @Int64 @Text" ->
          T"Text -> Int64",
        // EToInterface
        E"λ (t: Mod:Ti) → (( to_interface @Mod:I @Mod:Ti t ))" ->
          T"Mod:Ti → Mod:I",
        // EFromInterface
        E"λ (i: Mod:I) → (( from_interface @Mod:I @Mod:Ti i ))" ->
          T"Mod:I → Option Mod:Ti",
        // EUnsafeFromInterface
        E"λ (cid: ContractId Mod:I) (i: Mod:I) → (( unsafe_from_interface @Mod:I @Mod:Ti cid i ))" ->
          T"ContractId Mod:I → Mod:I → Mod:Ti",
        // EToRequiredInterface
        E"λ (sub: Mod:SubI) → (( to_required_interface @Mod:I @Mod:SubI sub ))" ->
          T"Mod:SubI → Mod:I",
        // EFromRequiredInterface
        E"λ (i: Mod:I) → (( from_required_interface @Mod:I @Mod:SubI i ))" ->
          T"Mod:I → Option Mod:SubI",
        // EUnsafeFromRequiredInterface
        E"λ (cid: ContractId Mod:I) (i: Mod:I) → (( unsafe_from_required_interface @Mod:I @Mod:SubI cid i ))" ->
          T"ContractId Mod:I → Mod:I → Mod:SubI",
        // ECallInterface
        E"λ (i: Mod:I) → (( call_method @Mod:I getParties i ))" ->
          T"Mod:I → List Party",
        // EInterfaceTemplateTypeRep
        E"λ (i: Mod:I) → (( interface_template_type_rep @Mod:I i ))" ->
          T"Mod:I → TypeRep",
        // ESignatoryInterface
        E"λ (i: Mod:I) → (( signatory_interface @Mod:I i ))" ->
          T"Mod:I → List Party",
        // EObserverInterface
        E"λ (i: Mod:I) → (( observer_interface @Mod:I i ))" ->
          T"Mod:I → List Party",
        // EChoiceController
        E"λ (e₁: Mod:T) (e₂: Int64) → (( choice_controller @Mod:T Ch e₁ e₂ ))" ->
          T"Mod:T → Int64 → (( List Party ))",
        E"λ (e₁: Mod:I) (e₂: Int64) → (( choice_controller @Mod:I ChIface e₁ e₂ ))" ->
          T"Mod:I → Int64 → (( List Party ))",
        // EChoiceObserver
        E"λ (e₁: Mod:T) (e₂: Int64) → (( choice_observer @Mod:T Ch e₁ e₂ ))" ->
          T"Mod:T → Int64 → (( List Party ))",
        E"λ (e₁: Mod:I) (e₂: Int64) → (( choice_observer @Mod:I ChIface e₁ e₂ ))" ->
          T"Mod:I → Int64 → (( List Party ))",
      )

      forEvery(testCases) { (exp: Expr, expectedType: Type) =>
        env.typeOfTopExpr(exp) shouldBe expectedType
      }
    }

    "not reject exhaustive patterns" in {

      val testCases = Table(
        "expression",
        E"Λ (τ : ⋆). λ (e : Mod:Tree τ) → (( case e of Mod:Tree:Node x → () | Mod:Tree:Leaf x -> () ))",
        E"Λ (τ : ⋆). λ (e : Mod:Tree τ) → (( case e of Mod:Tree:Node x → () | Mod:Tree:Leaf x -> () |  Mod:Tree:Leaf x -> () | Mod:Tree:Node x → () ))",
        E"Λ (τ : ⋆). λ (e : Mod:Tree τ) → (( case e of Mod:Tree:Node x → () | _ -> () ))",
        E"Λ (τ : ⋆). λ (e : Mod:Tree τ) → (( case e of _ -> () ))",
        E"Λ (τ : ⋆). λ (e : Mod:Tree τ) → (( case e of _ -> () | Mod:Tree:Node x → () ))",
        E"λ (e : Mod:Color) → (( case e of Mod:Color:Red → () | Mod:Color:Green → () | Mod:Color:Blue → () ))",
        E"λ (e : Mod:Color) → (( case e of Mod:Color:Blue → () | Mod:Color:Green → () | Mod:Color:Red → () ))",
        E"λ (e : Mod:Color) → (( case e of Mod:Color:Red → () | Mod:Color:Blue → () | _ -> () ))",
        E"λ (e : Mod:Color) → (( case e of Mod:Color:Green → () | _ -> () | Mod:Color:Red → () ))",
        E"λ (e : Mod:Color) → (( case e of _ -> () ))",
        E"Λ (τ : ⋆). λ (e : List τ) → (( case e of Cons x y → () | Nil -> () ))",
        E"Λ (τ : ⋆). λ (e : List τ) → (( case e of Nil -> () | Cons x y → () ))",
        E"Λ (τ : ⋆). λ (e : List τ) → (( case e of Nil → () | _ -> () ))",
        E"Λ (τ : ⋆). λ (e : List τ) → (( case e of Cons x y → () | _ -> () ))",
        E"Λ (τ : ⋆). λ (e : List τ) → (( case e of _ -> () ))",
        E"Λ (τ : ⋆). λ (e : Option τ) → (( case e of None → () | Some x -> () ))",
        E"Λ (τ : ⋆). λ (e : Option τ) → (( case e of Some x -> () | None → () ))",
        E"Λ (τ : ⋆). λ (e : Option τ) → (( case e of None -> () | _ -> () ))",
        E"Λ (τ : ⋆). λ (e : Option τ) → (( case e of Some x → () | _ -> () ))",
        E"Λ (τ : ⋆). λ (e : Option τ) → (( case e of _ -> () ))",
        E"λ (e : Bool) → (( case e of True → () | False → () ))",
        E"λ (e : Bool) → (( case e of False → () | True → () ))",
        E"λ (e : Bool) → (( case e of True → () | _ → () ))",
        E"λ (e : Bool) → (( case e of False  → () | _ → () ))",
        E"λ (e : Bool) → (( case e of _ → () ))",
        E"(( case () of () → () ))",
        E"(( case () of _ → () ))",
        E"Λ (τ : ⋆). λ (e : Mod:R τ) → (( case e of _ -> () ))",
        E"Λ (τ : ⋆). λ (e : τ) → (( case e of _ -> () ))",
      )

      forEvery(testCases)(env.typeOfTopExpr(_))
    }

    "infer proper type for Scenarios" in {
      val testCases = Table(
        "expression" ->
          "expected type",
        E"Λ (τ : ⋆). λ (e: τ) → (( spure @τ e ))" ->
          T"∀ (τ: ⋆). τ → (( Scenario τ ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (f: τ₁ → τ₂ → Scenario τ) → (( sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in f x₁ x₂ ))" ->
          T"∀ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). Scenario τ₁ → Scenario τ₂ → (τ₁ → τ₂ → Scenario τ) → (( Scenario τ ))",
        E"Λ (τ : ⋆). λ (e₁: Party) (e₂: Update τ) → (( commit @τ e₁ e₂ ))" ->
          T"∀ (τ : ⋆). Party → Update τ → (( Scenario τ ))",
        E"Λ (τ : ⋆). λ (e₁: Party) (e₂: Update τ) → (( must_fail_at @τ e₁ e₂ ))" ->
          T"∀ (τ : ⋆). Party → Update τ → (( Scenario Unit ))",
        E"λ (e: Int64) → (( pass e ))" ->
          T"Int64 → (( Scenario Timestamp ))",
        E"(( sget_time ))" ->
          T"(( Scenario Timestamp ))",
        E"λ (e: Text) → (( sget_party e ))" ->
          T"Text → (( Scenario Party ))",
        E"Λ (τ : ⋆). λ (e : Scenario τ) → (( sembed_expr @τ e ))" ->
          T"∀ (τ : ⋆). Scenario τ → (( Scenario τ ))",
      )

      forEvery(testCases) { (exp: Expr, expectedType: Type) =>
        env.typeOfTopExpr(exp) shouldBe expectedType
      }
    }

    "infers proper type for Update" in {
      val testCases = Table(
        "expression" ->
          "expected type",
        // ScenarioPure
        E"Λ (τ : ⋆). λ (e: τ) → (( upure @τ e ))" ->
          T"∀ (τ: ⋆). τ → (( Update τ ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: Update τ₁) (e₂: Update τ₂) (f: τ₁ → τ₂ → Update τ) → (( ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in f x₁ x₂ ))" ->
          T"∀ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). Update τ₁ → Update τ₂ → (τ₁ → τ₂ → Update τ) → (( Update τ ))",
        E"λ (e: Mod:T) → (( create @Mod:T e))" ->
          T"Mod:T → (( Update (ContractId Mod:T) ))",
        E"λ (e: Mod:I) → (( create_by_interface @Mod:I e))" ->
          T"Mod:I → (( Update (ContractId Mod:I) ))",
        E"λ (e₁: ContractId Mod:T) (e₂: Int64) → (( exercise @Mod:T Ch e₁ e₂ ))" ->
          T"ContractId Mod:T → Int64 → (( Update Numeric 10 ))",
        E"λ (e₁: ContractId Mod:I) (e₂: Int64) → (( exercise_interface @Mod:I ChIface e₁ e₂ ))" ->
          T"ContractId Mod:I → Int64 → (( Update Numeric 10 ))",
        E"λ (e₁: ContractId Mod:I) (e₂: Int64) (e₃: Mod:I → Bool) → (( exercise_interface_with_guard @Mod:I ChIface e₁ e₂ e₃ ))" ->
          T"ContractId Mod:I → Int64 → (Mod:I → Bool) → (( Update Numeric 10 ))",
        E"λ (e₁: Party) (e₂: Int64) → (( exercise_by_key @Mod:T Ch e₁ e₂ ))" ->
          T"Party → Int64 → (( Update Numeric 10 ))",
        E"λ (e: ContractId Mod:T) → (( fetch_template @Mod:T e ))" ->
          T"ContractId Mod:T → (( Update Mod:T ))",
        E"λ (e: ContractId Mod:I) → (( fetch_interface @Mod:I e ))" ->
          T"ContractId Mod:I → (( Update Mod:I ))",
        E"λ (e: Party) → (( fetch_by_key @Mod:T e ))" ->
          T"Party → (( Update (⟨ contract: Mod:T, contractId: ContractId Mod:T ⟩) ))",
        E"λ (e: Party) →  (( lookup_by_key @Mod:T e ))" ->
          T"Party → (( Update (Option (ContractId Mod:T)) ))",
        E"(( uget_time ))" ->
          T"(( Update Timestamp ))",
        E"Λ (τ : ⋆). λ (e: Update τ) →(( uembed_expr @τ e ))" ->
          T"∀ (τ : ⋆). Update τ -> (( Update τ ))",
      )

      forEvery(testCases) { (exp: Expr, expectedType: Type) =>
        env.typeOfTopExpr(exp) shouldBe expectedType
      }
    }

    "handle variable scope properly" in {

      val testCases = Table(
        "expression" ->
          "expected type",
        E"Λ (τ : ⋆) (σ : ⋆). (( λ (x : τ)  → λ (x: σ)  → x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). (( τ  → σ  → σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : σ) → (( λ (x : τ)  → let x: σ = e in x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). σ → (( τ  → σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : σ) → ((  let x: σ = e in λ (x : τ)  → x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). σ → (( τ → τ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁ : τ) (e₂: σ) → (( let x : τ = e₁ in let x : σ = e₂ in x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). τ → σ → (( σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (f : σ → τ) (x: σ) → (( let x : τ = f x in x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). (σ → τ) → σ → (( τ ))",
        E"""Λ (τ : ⋆) (σ : ⋆). λ (e: List σ) → (( λ (x : τ) → case e of Cons x t → x | Nil -> ERROR @σ "error" ))""" ->
          T"∀ (τ : ⋆) (σ : ⋆). List σ → (( τ → σ ))",
        E"""Λ (τ : ⋆) (σ : ⋆). λ (e: List σ) → (( case e of Cons x t → λ (x : τ) → x | _ -> ERROR @(τ  → τ) "error" ))""" ->
          T"∀ (τ : ⋆) (σ : ⋆). List σ → (( τ  → τ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: Scenario σ) → (( sbind x: σ ← e in spure @(τ → τ) (λ (x : τ) → x) ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Scenario σ → (( Scenario (τ  → τ) ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: Scenario σ) → (( λ (x : τ) → sbind x: σ ← e in spure @σ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Scenario σ → (( τ → Scenario σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Scenario τ) (e₂: Scenario σ)  → (( sbind x: τ ← e₁ ; x: σ ← e₂ in spure @σ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Scenario τ → Scenario σ → (( Scenario σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (f : σ → τ) (x: σ) → (( sbind x : τ ← spure @τ (f x) in spure @τ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). (σ → τ) → σ → (( Scenario τ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: Update σ) → (( ubind x: σ ← e in upure @(τ → τ) (λ (x : τ) → x) ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Update σ → (( Update (τ  → τ) ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: Update σ) → (( λ (x : τ) → ubind x: σ ← e in upure @σ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Update σ → (( τ → Update σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Update τ) (e₂: Update σ)  → (( ubind x: τ ← e₁ ; x: σ ← e₂ in upure @σ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Update τ → Update σ → (( Update σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (f : σ → τ) (x: σ) → (( ubind x : τ ← upure @τ (f x) in upure @τ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). (σ → τ) → σ → (( Update τ ))",
      )

      forEvery(testCases) { (exp: Expr, expectedType: Type) =>
        env.typeOfTopExpr(exp) shouldBe expectedType
      }
    }

    // TEST_EVIDENCE: Integrity: ill-formed expressions are rejected
    "reject ill formed terms" in {

      // In the following test cases we use the variable `nothing` when we
      // cannot built an expression of the expected type. In those cases we
      // expect the type checker to fail with the error we are testing before
      // it tries to type check the variable `nothing`.
      // Expressions of type τ, where τ has kind ⋆ → ⋆, are examples of
      // such expressions that cannot be constructed.

      val testCases = Table[Expr, PartialFunction[ValidationError, _]](
        "non-well formed expression" -> "error",
        // ExpDefVar
        E"⸨ x ⸩" -> //
          { case _: EUnknownExprVar => },
        // ExpApp
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₂) (e₂ : τ₁) → ⸨ e₁ e₂ ⸩" -> //
          { case _: EExpectedFunctionType => },
        E"Λ (τ₁: ⋆) (τ₂ : ⋆) (τ₃ : ⋆). λ (e₁ : τ₂ → τ₃) (e₂ : τ₁) → ⸨ e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ₁: ⋆) (τ₂ : ⋆) (τ₃ : ⋆). λ (e₁ : (τ₁ → τ₂) → τ₃) (e₂ : τ₁) → ⸨ e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ₁: ⋆) (τ₂ : ⋆) (τ₃ : ⋆). λ (e₁ : Bool) (e₂ : τ₃) → ⸨ e₁ e₂ ⸩" -> //
          { case _: EExpectedFunctionType => },
        // ExpTyAbs
        E"Λ (τ : ⋆) . λ (e: τ) → ⸨ Λ (σ: ⋆ → nat) . e ⸩" -> //
          { case _: ENatKindRightOfArrow => },
        // ExpTyApp
        E"Λ (τ : ⋆ → ⋆) (σ: ⋆ → ⋆). λ (e : ∀ (α : ⋆). σ α) → ⸨ e @τ ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (σ: ⋆). λ (e : σ) → ⸨ e @τ ⸩" -> //
          { case _: EExpectedUniversalType => },
        // ExpAbs
        E"⸨ λ (x : List) → () ⸩" -> //
          { case _: EKindMismatch => },
        // ExpLet
        E"Λ  (τ₁: ⋆) (τ₂ : ⋆) (σ: ⋆). λ (e₁ : τ₁) (e₂ : σ) → ⸨ let x : τ₂ = e₁ in e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆ → ⋆) (σ: ⋆). λ(e : σ) → ⸨ let x : τ = nothing in e ⸩" -> //
          { case _: EKindMismatch => },
        // ExpLet (rhs)
        E"Λ  (τ₁: ⋆). λ (e₁ : τ₁) → ⸨ let x : τ₁ = e₁ e₁ in unbound ⸩" -> //
          { case _: EExpectedFunctionType => },
        E"Λ  (τ₁: ⋆). λ (e₁ : τ₁) → ⸨ let _ : τ₁ = e₁ e₁ in unbound ⸩" -> //
          { case _: EExpectedFunctionType => },
        // ExpListNil
        E"Λ (τ : ⋆ → ⋆). ⸨ Nil @τ ⸩" -> //
          { case _: EKindMismatch => },
        // ExpListCons
        E"Λ (τ : ⋆ → ⋆). ⸨ Cons @τ [nothing] nothing ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₂) (e₂ : τ₁) (e : List τ₁) → ⸨ Cons @τ₁ [e₁, e₂] e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₂) (e : List τ₁) → ⸨ Cons @τ₁ [e₁] e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₁) (e : List τ₂) → ⸨ Cons @τ₁ [e₁] e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ₁: ⋆) (τ₂: ⋆). λ (e₁: τ₁) (e: τ₂) → ⸨ Cons @τ₁ [e₁] e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₁) (e : List τ₁) → ⸨ Cons @τ₂ [e₁] e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆). λ (e : List τ) → ⸨ Cons @τ [] e ⸩" -> //
          { case _: EEmptyConsFront => },
        // ExpVal
        E"⸨ Mod:g ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Definition(_), Reference.Value(_)),
                ) =>
          },
        E"⸨ Mod:R ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Value(_), Reference.Value(_)),
                ) =>
          },
        // ExpRecCon
        E"Λ (σ : ⋆). λ (e₁ : Bool) (e₂ : List σ) → ⸨ Mod:R @σ { f1 = e₁, f2 = e₂ } ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (σ : ⋆ → ⋆). λ (e₁ : Int64) → ⸨ Mod:R @σ { f1 = e₁, f2 = nothing } ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (σ : ⋆). λ (e₁ : Int64) (e₂ : List σ) → ⸨ Mod:R @σ { f1 = e₁, f3 = e₂ } ⸩" -> //
          { case _: EFieldMismatch => },
        E"Λ (σ : ⋆). λ (e₁ : Int64) (e₂ : List σ) → ⸨ Mod:R @σ { f1 = e₁ } ⸩" -> //
          { case _: EFieldMismatch => },
        E"Λ (σ : ⋆) (τ: ⋆). λ (e₁ : Int64) (e₂ : List σ) (e₃:τ) → ⸨ Mod:R @σ { f1 = e₁, f2 = e₂, f3 = e₃} ⸩" -> //
          { case _: EFieldMismatch => },
        E"Λ (σ : ⋆). λ (e₁ : Bool) (e₂ : List σ) → ⸨ Mod:g @σ { f1 = e₁, f2 = e₂ } ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Definition(_), Reference.DataType(_)),
                ) =>
          },
        E"Λ (σ : ⋆). λ (e₁ : Bool) (e₂ : List σ) → ⸨ Mod:S @σ { f1 = e₁, f2 = e₂ } ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.DataType(_), Reference.DataType(_)),
                ) =>
          },
        // ExpRecProj
        E"Λ (σ : ⋆ → ⋆). ⸨ Mod:R @σ {f2} nothing⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (σ : ⋆). λ (e : Mod:R σ) → ⸨ Mod:R @σ {f3} e ⸩" -> //
          { case _: EUnknownField => },
        E"Λ (τ: ⋆) (σ: ⋆). λ (e: Mod:R τ) → ⸨ Mod:R @σ {f1} e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ: ⋆) (σ: ⋆). λ (e: τ) → ⸨ Mod:R @σ {f1} e ⸩" -> //
          { case _: ETypeMismatch => },
        // ExpRecUpdate
        E"Λ (σ: ⋆ → ⋆). λ (e: Int64) → ⸨ Mod:R @σ { nothing with f1 = e₁ } ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (σ : ⋆). λ (e : Mod:R σ) (e₂ : List σ) → ⸨ Mod:R @σ { e  with f3 = e₂ } ⸩" -> //
          { case _: EUnknownField => },
        E"Λ (σ : ⋆). λ (e : Mod:R σ) (e₂ : Bool) → ⸨ Mod:R @σ { e  with f2 = e₂ } ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ: ⋆) (σ: ⋆). λ (e: Mod:R τ) (e₂: List σ) → ⸨ Mod:R @τ { e  with f2 = e₂ } ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ: ⋆) (σ: ⋆). λ (e: Mod:R τ) (e₂: List σ) → ⸨ Mod:R @σ { e  with f2 = e₂ } ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (σ: ⋆). λ  (e: List σ) (e₂: List σ) → ⸨ Mod:R @σ { e with f2 = e₂ } ⸩" -> //
          { case _: ETypeMismatch => },
        // ExpVarCon
        E"Λ (σ : ⋆ → ⋆). ⸨ Mod:Tree:Leaf @σ nothing ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : σ) → ⸨ Mod:Tree:Leaf @τ e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : σ) → ⸨ Mod:g:Leaf @τ e ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Definition(_), Reference.DataType(_)),
                ) =>
          },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : σ) → ⸨ Mod:S:Leaf @τ e ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.DataType(_), Reference.DataType(_)),
                ) =>
          },
        // ExpStructCon
        E"Λ (τ₁: ⋆) (τ₂: ⋆). λ (e₁: τ₁) (e₂: τ₂) → ⸨ ⟨ f₁ = e₁, f₁ = e₂ ⟩ ⸩" -> //
          { case _: EDuplicateField => },
        // ExpStructProj
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) → ⸨ (e).f3 ⸩" -> //
          { case _: EUnknownField => },
        // ExpStructUpdate
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) (e₂ : τ₂)  → ⸨ ⟨ e with f₃ = e₂ ⟩ ⸩" -> //
          { case _: EUnknownField => },
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆) (τ₃: ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) (e₃: τ₃)  → ⸨ ⟨ e with f₂ = e₃ ⟩ ⸩" -> //
          { case _: ETypeMismatch => },
        // ExpCaseVariant
        E"Λ (τ : ⋆). λ (e : τ) → ⸨ case e of Mod:Tree:Node x -> () | _ -> () ⸩" -> //
          { case _: EPatternTypeMismatch => },
        E"Λ (τ : ⋆). λ (e : Mod:Tree τ) → ⸨ case e of Mod:Tree:Node x -> () ⸩" -> //
          { case _: ENonExhaustivePatterns => },
        // ExpCaseEnum
        E"Λ (τ : ⋆). λ (e : τ) → ⸨ case e of Mod:Color:Red -> () | _ -> () ⸩" -> //
          { case _: EPatternTypeMismatch => },
        E"λ (e : Mod:Color) → ⸨ case e of Mod:Color:Red -> () | Mod:Color:Green -> () ⸩" -> //
          { case _: ENonExhaustivePatterns => },
        // ExpCaseNil
        E"Λ (τ : ⋆). λ (e : τ) → ⸨ case e of Nil → () | _ -> () ⸩" -> //
          { case _: EPatternTypeMismatch => },
        E"Λ (τ : ⋆). λ (e : List τ) → ⸨ case e of Nil → () ⸩" -> //
          { case _: ENonExhaustivePatterns => },
        // ExpCaseCons
        E"Λ (τ : ⋆). λ (e : τ) → ⸨ case e of Cons x y → () | _ -> () ⸩" -> //
          { case _: EPatternTypeMismatch => },
        E"Λ (τ : ⋆). λ (e: List τ) → ⸨ case e of Cons x x → () | _ -> () ⸩" -> //
          { case _: EClashingPatternVariables => },
        E"Λ (τ : ⋆). λ (e : List τ) → ⸨ case e of Cons x y → () ⸩" -> //
          { case _: ENonExhaustivePatterns => },
        // ExpCaseFalse & ExpCaseTrue
        E"Λ (τ : ⋆). λ (e : τ) → ⸨ case e of True → () | _ -> () ⸩" -> //
          { case _: EPatternTypeMismatch => },
        E"Λ (τ : ⋆). λ (e : τ) → ⸨ case e of False → () | _ -> () ⸩" -> //
          { case _: EPatternTypeMismatch => },
        E"λ (e : Bool) → ⸨ case e of True → () ⸩" -> //
          { case _: ENonExhaustivePatterns => },
        E"λ (e : Bool) → ⸨ case e of False → () ⸩" -> //
          { case _: ENonExhaustivePatterns => },
        // ExpCaseUnit
        E"Λ (τ : ⋆). λ (e : τ) → ⸨ case e of () → () ⸩" -> //
          { case _: EPatternTypeMismatch => },
        // ExpCaseOr
        E"Λ (τ : ⋆). λ (e : τ) → ⸨ case e of ⸩" -> //
          { case _: EEmptyCase => },
        // ExpToAny
        E"⸨ to_any @Mod:R nothing ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ :⋆). λ (r: Mod:R τ) → ⸨ to_any @(Mod:R τ) r ⸩" -> //
          { case _: EExpectedAnyType => },
        E"Λ (τ :⋆). λ (t: Mod:Tree τ) → ⸨ to_any @(Mod:Tree τ) t ⸩" -> //
          { case _: EExpectedAnyType => },
        E"Λ (τ :⋆). λ (t: ∀ (α : ⋆). Int64) → ⸨ to_any @(∀ (α : ⋆). Int64) t ⸩" -> //
          { case _: EExpectedAnyType => },
        E"Λ (τ :⋆). λ (t: List (Option (∀ (α: ⋆). Int64))) → ⸨ to_any @(List (Option (∀ (α: ⋆). Int64))) t ⸩" -> //
          { case _: EExpectedAnyType => },
        E"λ (e: |Mod:S|) → ⸨ to_any @|Mod:S| e ⸩" -> //
          { case _: EExpectedAnyType => },
        E"⸨ to_any @Int64 (Nil @Int64) ⸩" -> //
          { case _: ETypeMismatch => },
        // ExpFromAny
        E"λ (t: Any) → ⸨ from_any @Mod:R t ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ :⋆). λ (t: Any) → ⸨ from_any @(Mod:R τ) t ⸩" -> //
          { case _: EExpectedAnyType => },
        E"Λ (τ :⋆). λ (t: Any) → ⸨ from_any @(Mod:Tree τ) t ⸩" -> //
          { case _: EExpectedAnyType => },
        E"λ (t: Mod:T) → ⸨ from_any @Mod:T t ⸩" -> //
          { case _: ETypeMismatch => },
        E"λ (t: Any) → ⸨ from_any @(∀ (α: ⋆). Int64) t ⸩" -> //
          { case _: EExpectedAnyType => },
        E"Λ (τ :⋆). λ (t: Any) → ⸨ from_any @(List (Option (∀ (α: ⋆). Int64))) t ⸩" -> //
          { case _: EExpectedAnyType => },
        E"λ (e: Any) → ⸨ from_any @|Mod:S| e ⸩" -> //
          { case _: EExpectedAnyType => },
        // ExpTypeRep
        E"⸨ type_rep @Mod:R ⸩" -> //
          { case _: EKindMismatch => },
        E"⸨ type_rep @Mod:NoSuchType ⸩" -> //
          { case _: EUnknownDefinition => },
        E"Λ (τ : ⋆). ⸨ type_rep @τ ⸩" -> //
          { case _: EExpectedAnyType => },
        E"⸨ type_rep @(∀(τ :⋆) . Int64) ⸩" -> //
          { case _: EExpectedAnyType => },
        E"⸨ type_rep @|Mod:S| ⸩" -> //
          { case _: EExpectedAnyType => },
        // ExpToAnyException
        E"λ (r: Mod:T) → ⸨ to_any_exception @(Mod:T) r ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Exception(_), Reference.Exception(_)),
                ) =>
          },
        E"λ (t: Bool) → ⸨ to_any_exception @Bool t ⸩" -> //
          { case _: EExpectedExceptionType => },
        E"Λ (τ :⋆). λ (t: ∀ (α : ⋆). Int64) → ⸨ to_any_exception @(∀ (α : ⋆). Int64) t ⸩" -> //
          { case _: EExpectedExceptionType => },
        E"λ (e: |Mod:S|) → ⸨ to_any_exception  @|Mod:S| e ⸩" -> //
          { case _: EExpectedExceptionType => },
        E"λ (e: Mod:T) → ⸨ to_any_exception @(Mod:E) e ⸩" -> //
          { case _: ETypeMismatch => },
        // ExpFromAnyException
        E"λ (t: AnyException) → ⸨ from_any_exception @Mod:T t ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Exception(_), Reference.Exception(_)),
                ) =>
          },
        E"λ (t: Any) → ⸨ from_any_exception @Bool t ⸩" -> //
          { case _: EExpectedExceptionType => },
        E"λ (t: ∀ (α : ⋆). Int64) → ⸨ from_any_exception @(∀ (α : ⋆). Int64) t ⸩" -> //
          { case _: EExpectedExceptionType => },
        E"λ (e: Any) → ⸨ from_any_exception  @|Mod:S| e ⸩" -> //
          { case _: EExpectedExceptionType => },
        // ExpThrow
        E"⸨ throw @Mod:R @Mod:E nothing ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ :⋆). λ (e : Mod:U) →  ⸨ throw @τ @Mod:U e ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Exception(_), Reference.Exception(_)),
                ) =>
          },
        E"Λ (τ :⋆). λ (e : Mod:U) →  ⸨ throw @τ @Mod:U e ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Exception(_), Reference.Exception(_)),
                ) =>
          },
        E"Λ (τ :⋆). λ (e : Bool) →  ⸨ throw @τ @Bool e ⸩" -> //
          { case _: EExpectedExceptionType => },
        E"Λ (τ :⋆). λ (e: ∀ (α : ⋆). Int64) →  ⸨ throw @τ @(∀ (α : ⋆). Int64) e ⸩" -> //
          { case _: EExpectedExceptionType => },
        E"Λ (τ :⋆). λ (e: |Mod:S|) →  ⸨ throw @τ @(∀ (α : ⋆). Int64) e ⸩" -> //
          { case _: EExpectedExceptionType => },
        E"Λ (τ: ⋆) (σ: ⋆). λ (e: σ) →  ⸨ throw @τ @Mod:E e ⸩" -> //
          { case _: ETypeMismatch => },
        // ScnPure
        E"Λ (τ : ⋆ → ⋆). ⸨ spure @τ nothing ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: τ) → ⸨ spure @σ e ⸩" -> //
          { case _: ETypeMismatch => },
        // ScnBlock
        E"Λ (τ : ⋆) (τ₂ : ⋆ → ⋆) (τ₁ : ⋆). λ (e₁: Scenario τ₁) (e: Scenario τ) → ⸨ sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← nothing in e ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆ → ⋆). λ (e₂: Scenario τ₂) (e: Scenario τ) → ⸨ sbind x₁: τ₁ ← nothing ;  x₂: τ₂ ← e₂ in e ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁:  τ₁) (e₂: Scenario τ₂) (e: Scenario τ) → ⸨ sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: Scenario τ₁) (e₂:τ₂) (e: Scenario τ) → ⸨ sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (f: τ) → ⸨ sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in f ⸩" -> //
          { case _: EExpectedScenarioType => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆) (σ : ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (e: Scenario τ) → ⸨ sbind x₁: σ  ← e₁ ;  x₂: τ₂ ← e₂ in e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆) (σ : ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (e: Scenario τ) → ⸨ sbind x₁: τ₁ ← e₁ ;  x₂: σ ← e₂ in e ⸩" -> //
          { case _: ETypeMismatch => },
        // ScnCommit
        E"Λ (τ : ⋆ → ⋆). λ (e₁: Party) → ⸨ commit @τ e₁ nothing ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: σ) (e₂: Update τ) → ⸨ commit @τ e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: Update σ) → ⸨ commit @τ e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: σ) → ⸨ commit @τ e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        // ScnMustFail
        E"Λ (τ : ⋆ → ⋆). λ (e₁: Party) → ⸨ must_fail_at @τ e₁ nothing ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: σ) (e₂: Update τ) → ⸨ must_fail_at @τ e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: Update σ) → ⸨ must_fail_at @τ e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: σ) → ⸨ must_fail_at @τ e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        // ScnPass
        E"Λ (σ : ⋆). λ (e: σ) → ⸨ pass e ⸩" -> //
          { case _: ETypeMismatch => },
        // ScnGetParty
        E"Λ (σ : ⋆). λ (e: σ) → ⸨ sget_party e ⸩" -> //
          { case _: ETypeMismatch => },
        // ScnEmbedExpr
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : σ) → ⸨ sembed_expr @τ e ⸩" -> //
          { case _: ETypeMismatch => },
        //  UpdPure
        E"Λ (τ : ⋆ → ⋆). ⸨ upure @τ nothing ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: τ) → ⸨ upure @σ e ⸩" -> //
          { case _: ETypeMismatch => },
        // UpdBlock
        E"Λ (τ : ⋆) (τ₂ : ⋆ → ⋆) (τ₁ : ⋆). λ (e₁: Update τ₁) (e: Update τ) → ⸨ ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← nothing in e ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆ → ⋆). λ (e₂: Update τ₂) (e: Update τ) → ⸨ ubind x₁: τ₁ ← nothing ;  x₂: τ₂ ← e₂ in e ⸩" -> //
          { case _: EKindMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁:  τ₁) (e₂: Update τ₂) (e: Update τ) → ⸨ ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: Update τ₁) (e₂:τ₂) (e: Update τ) → ⸨ ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: Update τ₁) (e₂: Update τ₂) (f: τ) → ⸨ ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in f ⸩" -> //
          { case _: EExpectedUpdateType => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆) (σ : ⋆). λ (e₁: Update τ₁) (e₂: Update τ₂) (e: Update τ) → ⸨ ubind x₁: σ  ← e₁ ;  x₂: τ₂ ← e₂ in e ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆) (σ : ⋆). λ (e₁: Update τ₁) (e₂: Update τ₂) (e: Update τ) → ⸨ ubind x₁: τ₁ ← e₁ ;  x₂: σ ← e₂ in e ⸩" -> //
          { case _: ETypeMismatch => },
        // UpdCreate
        E"λ (e: Mod:U) → ⸨ create @Mod:U nothing ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Template(_), Reference.Template(_)),
                ) =>
          },
        E"Λ (σ : ⋆). λ (e: σ) → ⸨ create @Mod:T e ⸩" -> //
          { case _: ETypeMismatch => },
        // UpdCreateInterface
        E"λ (e: Mod:U) → ⸨ create_by_interface @Mod:U nothing ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Interface(_), Reference.Interface(_)),
                ) =>
          },
        E"Λ (σ : ⋆). λ (e: σ) → ⸨ create_by_interface @Mod:I e ⸩" -> //
          { case _: ETypeMismatch => },
        // UpdExercise
        E"λ (e₁: ContractId Mod:U) (e₂: Int64) → ⸨ exercise @Mod:U Ch e₁ e₂ ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Template(_), Reference.TemplateChoice(_, _)),
                ) =>
          },
        E"λ (e₁: ContractId Mod:T) (e₂: Int64) → ⸨ exercise @Mod:T ChTmpl e₁ e₂⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError
                    .NotFound(Reference.TemplateChoice(_, _), Reference.TemplateChoice(_, _)),
                ) =>
          },
        E"Λ (σ : ⋆).λ (e₁: ContractId Mod:T) (e₂: σ) → ⸨ exercise @Mod:T Ch e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (σ : ⋆).λ (e₁: ContractId σ) (e₂: Int64) → ⸨ exercise @Mod:T Ch e₁ e₂ ⸩" -> //
          { case _: ETypeMismatch => },
        // This verifies that template choice cannot be exercised by interface
        E"λ (e₁: ContractId Mod:I) (e₂: List Party) (e₃: Int64) → ⸨ exercise @Mod:I ChTmpl e₁ e₂ ⸩" -> {
          case EUnknownDefinition(
                _,
                LookupError.NotFound(Reference.Template(_), Reference.TemplateChoice(_, _)),
              ) =>
            // We double check that Ti implements I and Ti has a choice ChTmpl
            val TTyCon(conI) = t"Mod:I"
            val TTyCon(conTi) = t"Mod:Ti"
            env.pkgInterface.lookupInterfaceInstance(conI, conTi) should matchPattern {
              case Right(TemplateImplementsSignature(`conI`, _)) =>
            }
            assert(env.pkgInterface.lookupTemplateChoice(conTi, n"ChTmpl").isRight)
        },
        // UpdExerciseInterface
        E"λ (e₁: ContractId Mod:U) (e₂: Int64) (e₃: Mod:U → Bool) → ⸨ exercise_interface_with_guard @Mod:U ChIface e₁ e₂ e₃ ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Interface(_), Reference.InterfaceChoice(_, _)),
                ) =>
          },
        E"λ (e₁: ContractId Mod:I) (e₂: Int64) (e₃: Mod:I → Bool)  → ⸨ exercise_interface_with_guard @Mod:I Not e₁ e₂ e₃ ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError
                    .NotFound(Reference.InterfaceChoice(_, _), Reference.InterfaceChoice(_, _)),
                ) =>
          },
        E"Λ (σ : ⋆).λ (e₁: ContractId Mod:I) (e₂: σ) (e₃: Mod:I → Bool) → ⸨ exercise_interface_with_guard @Mod:T ChIface e₁ e₂ e₃ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (σ : ⋆).λ (e₁: ContractId Mod:I) (e₂: Int64) (e₃: Mod:T → Bool) → ⸨ exercise_interface_with_guard @Mod:T ChIface e₁ e₂ e₃ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (σ : ⋆).λ (e₁: ContractId σ) (e₂: Int64) (e₃: Mod:I → Bool) → ⸨ exercise_interface_with_guard @Mod:I ChIface e₁ e₂ e₃ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (σ : ⋆).λ (e₁: ContractId Mod:I) (e₂: Int64) (e₃: σ) → ⸨ exercise_interface_with_guard @Mod:I ChIface e₁ e₂ e₃ ⸩" -> //
          { case _: ETypeMismatch => },
        E"Λ (σ : ⋆).λ (e₁: ContractId Mod:I) (e₂: Int64) (e₃: σ → Bool) → ⸨ exercise_interface_with_guard @Mod:I ChIface e₁ e₂ e₃ ⸩" -> //
          { case _: ETypeMismatch => },
        // This verifies that interface choice cannot be exercise by template
        E"""λ (e₁: ContractId Mod:Ti) (e: Mod:Ti) (e₂: Int64) (e₃: Mod:Ti → Bool) → ⸨ exercise_interface_with_guard @Mod:Ti ChIface e₁ e₂ e₃ ⸩""" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Interface(_), Reference.InterfaceChoice(_, _)),
                ) =>
              // We double check that Ti implements I and Ti has a choice ChTmpl
              val TTyCon(conI) = t"Mod:I"
              val TTyCon(conTi) = t"Mod:Ti"
              env.pkgInterface.lookupInterfaceInstance(conI, conTi) should matchPattern {
                case Right(TemplateImplementsSignature(`conI`, _)) =>
              }
              assert(env.pkgInterface.lookupInterfaceChoice(conI, n"ChIface").isRight)
          },
        // UpdFetch
        E"λ (e: ContractId Mod:U) → ⸨ fetch_template @Mod:U e ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Template(_), Reference.Template(_)),
                ) =>
          },
        E"Λ (σ : ⋆). λ (e: σ) → ⸨ fetch_template @Mod:T e ⸩" -> //
          { case _: ETypeMismatch => },
        // UpdFetchInterface
        E"λ (e: ContractId Mod:U) → ⸨ fetch_interface @Mod:U e ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Interface(_), Reference.Interface(_)),
                ) =>
          },
        E"Λ (σ : ⋆). λ (e: σ) → ⸨ fetch_interface @Mod:I e ⸩" -> //
          { case _: ETypeMismatch => },
        // UpFecthByKey & lookupByKey
        E"""⸨ fetch_by_key @Mod:U "Bob" ⸩""" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Template(_), Reference.TemplateKey(_)),
                ) =>
          },
        E"""⸨ fetch_by_key @Mod:T "Bob" ⸩""" -> //
          { case _: ETypeMismatch => },
        E"""⸨ lookup_by_key @Mod:T "Bob" ⸩""" -> //
          { case _: ETypeMismatch => },
        // ScenarioEmbedExpr
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : σ) → ⸨ uembed_expr @τ e ⸩" -> //
          { case _: ETypeMismatch => },
        // EToInterface
        E"""λ (t: Mod:Ti) → ⸨ to_interface @Mod:T @Mod:Ti t  ⸩""" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Interface(_), _),
                ) =>
          },
        E"""λ (t: Mod:Ti) → ⸨ to_interface @Mod:I @Mod:T t  ⸩""" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.InterfaceInstance(_, _), _),
                ) =>
          },
        E"""λ (t: Mod:T) → ⸨ to_interface @Mod:I @Mod:Ti t  ⸩""" -> //
          { case _: ETypeMismatch => },
        // EFromInterface
        E"""λ (i: Mod:I) → ⸨ from_interface @Mod:I @Mod:I i ⸩""" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Template(_), _),
                ) =>
          },
        E"λ (i: Mod:I) → ⸨ from_interface @Mod:I @Mod:T i ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.InterfaceInstance(_, _), _),
                ) =>
          },
        E"λ (i: Mod:J) → ⸨ from_interface @Mod:I @Mod:Ti i ⸩" -> //
          { case _: ETypeMismatch => },
        // ECallInterface
        E"λ (i: Mod:U) → ⸨  call_method @Mod:U getParties i ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Interface(_), Reference.Method(_, _)),
                ) =>
          },
        E"λ (i: Mod:I) → ⸨  call_method @Mod:I getParty i ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Method(_, _), Reference.Method(_, _)),
                ) =>
          },
        E"Λ (σ : ⋆). λ (e: σ) → ⸨ call_method @Mod:I getParties e ⸩" -> //
          { case _: ETypeMismatch => },
        // EInterfaceTemplateTypeRep - argument must be an interface
        E"λ (t: Mod:T) → ⸨ interface_template_type_rep @Mod:T t ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Interface(_), Reference.Interface(_)),
                ) =>
          },
        // ESignatoryInterface - argument must be an interface
        E"λ (t: Mod:T) → ⸨ signatory_interface @Mod:T t ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Interface(_), Reference.Interface(_)),
                ) =>
          },
        // EObserverInterface - argument must be an interface
        E"λ (t: Mod:T) → ⸨ observer_interface @Mod:T t ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError.NotFound(Reference.Interface(_), Reference.Interface(_)),
                ) =>
          },
        // EChoiceController - template/interface must have choice
        E"λ (e₁: Mod:T) (e₂: Int64) → ⸨ choice_controller @Mod:T FakeChoice e₁ e₂ ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError
                    .NotFound(Reference.TemplateChoice(_, _), Reference.TemplateChoice(_, _)),
                ) =>
          },
        E"λ (e₁: Mod:I) (e₂: Int64) → ⸨ choice_controller @Mod:I FakeChoice e₁ e₂ ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError
                    .NotFound(Reference.InterfaceChoice(_, _), Reference.InterfaceChoice(_, _)),
                ) =>
          },
        // EChoiceObserver - template/interface must have choice
        E"λ (e₁: Mod:T) (e₂: Int64) → ⸨ choice_observer @Mod:T FakeChoice e₁ e₂ ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError
                    .NotFound(Reference.TemplateChoice(_, _), Reference.TemplateChoice(_, _)),
                ) =>
          },
        E"λ (e₁: Mod:I) (e₂: Int64) → ⸨ choice_observer @Mod:I FakeChoice e₁ e₂ ⸩" -> //
          {
            case EUnknownDefinition(
                  _,
                  LookupError
                    .NotFound(Reference.InterfaceChoice(_, _), Reference.InterfaceChoice(_, _)),
                ) =>
          },
      )

      val ELocation(expectedLocation, EVar("something")) = E"⸨ something ⸩"
      val expectedContext = Context.Location(expectedLocation)

      forAll(testCases) { (exp, checkError) =>
        import scala.util.{Failure, Try}

        val x = Try(env.typeOfTopExpr(exp))
        x should matchPattern {
          case Failure(exception: ValidationError)
              if exception.context == expectedContext // check the error happened between ⸨ ⸩
                && checkError.isDefinedAt(exception) =>
        }
      }
    }

    // TEST_EVIDENCE: Integrity: ill-formed templates are rejected
    "reject ill formed template definition" in {

      val pkg =
        p"""
          metadata ( 'pkg' : '1.0.0' )

          module Mod {
            record @serializable MyUnit = {};

            record @serializable Key = {person: Text, party: Party};

            interface (this: I) = {
              viewtype Mod:MyUnit;
              method getParties: List Party;
            };
          }

          module NegativeTestCase {
            record @serializable T = {person: Party, name: Text};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch1 (self) (i : Unit) : Unit
                  , controllers Nil @Party
                  to upure @Unit ();
              choice Ch2 (self) (i : Unit) : Unit
                  , controllers Nil @Party
                  , observers Nil @Party
                  to upure @Unit ();
              choice Ch3 (self) (i : Unit) : Unit
                  , controllers Nil @Party
                  , observers Nil @Party
                  to upure @Unit ();
              implements Mod:I {
                view = Mod:MyUnit {};
                method getParties = Cons @Party [NegativeTestCase:T {person} this] (Nil @Party);
              };
              key @Mod:Key
                  (Mod:Key { person = (NegativeTestCase:T {name} this), party = (NegativeTestCase:T {person} this) })
                  (\ (key: Mod:Key) -> Cons @Party [(Mod:Key {party} key)] (Nil @Party)  );
            } ;
          }


          module PositiveTestCase_TemplateTypeShouldBeStar {
            record @serializable T (a: *) = {x: a};

            // in the next line, T should be of type *.
            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
            };
          }

          module PositiveTestCase_TemplateTypeShouldBeRecord {
            variant @serializable V = P : Party;

            // in the next line, V should be of record.
            template (this : V) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
            };
          }

           module PositiveTestCase_TemplateTypeShouldExists{
             // template without data type
             template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
            } ;
          }

          module PositiveTestCase_PreconditionShouldBeBoolean{
             record @serializable T = {person: Party, name: Text};

             template (this : T) =  {
              precondition ();                               // precondition should be a boolean
              signatories Nil @Party;
              observers Nil @Party;
            } ;
          }

          module PositiveTestCase_SignatoriesShouldBeListParty {
            record @serializable T = {};

            template (this : T) =  {
              precondition True;
              signatories ();                                 // should be of (type List Party)
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit, controllers Nil @Party to upure @Unit ();
            } ;
          }

          module PositiveTestCase_ObserversShouldBeListParty {
            record @serializable T = {};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers ();                                  // should be of type (List Party)
              choice Ch (self) (i : Unit) : Unit, controllers Nil @Party to upure @Unit ();
            } ;
          }

          module PositiveTestCase_ControllersMustBeListParty {
            record @serializable T = {};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit
                , controllers ()                                  // should be of type (List Party)
                to upure @Unit ();
            } ;
          }

          module PositiveTestCase_ChoiceObserversMustBeListParty {
            record @serializable T = {};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit
                , controllers Nil @Party
                , observers ()                                  // should be of type (List Party)
                to upure @Unit ();
            } ;
          }

          module PositiveTestCase4 {
            record @serializable T = {};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : List) : Unit   // the type of i (here List) should be of kind * (here it is * -> *)
                , controllers Nil @Party to upure @Unit ();
            } ;
          }

          module PositiveTestCase5 {
            record @serializable T = {};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : List   // the return type (here List) should be of kind * (here it is * -> *)
                , controllers Nil @Party to upure @(List) (/\ (tau : *). Nil @tau);
            } ;
          }

          module PositiveTestCase_KeyBodyShouldBeProperType {
            record @serializable T = {person: Party, name: Text};
            record @serializable Key = {person: Text, party: Party};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit, controllers Nil @Party to upure @Unit ();
              key @Mod:Key
                // In the next line, the declared type do not match body
                (PositiveTestCase_KeyBodyShouldBeProperType:Key {
                  person = (PositiveTestCase_KeyBodyShouldBeProperType:T {name} this),
                  party = (PositiveTestCase_KeyBodyShouldBeProperType:T {person} this)
                })
                (\ (key: Mod:Key) -> Cons @Party [(Mod:Key {party} key)] (Nil @Party)  );
              } ;
          }


           module PositiveTestCase_MaintainersShouldBeProperType {
            record @serializable T = {person: Party, name: Text};
            record @serializable Key = {person: Text, party: Party};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit, controllers Nil @Party to upure @Unit ();
              key @Mod:Key
                // In the next line, the declared type do not match body
                (Mod:Key {
                  person = (PositiveTestCase_MaintainersShouldBeProperType:T {name} this),
                  party = (PositiveTestCase_MaintainersShouldBeProperType:T {person} this)
                })
                (\ (key: PositiveTestCase_MaintainersShouldBeProperType:Key) ->
                  Cons @Party [(PositiveTestCase_MaintainersShouldBeProperType:Key {party} key)] (Nil @Party)  );
              } ;
          }

           module PositiveTestCase_MaintainersShouldBeListParty {
            record @serializable T = {person: Party, name: Text};
            record @serializable Key = {person: Text, party: Party};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit, controllers Nil @Party to upure @Unit ();
              key @Mod:Key
                // In the next line, the declared type do not match body
                (Mod:Key {
                  person = (PositiveTestCase_MaintainersShouldBeListParty:T {name} this),
                  party = (PositiveTestCase_MaintainersShouldBeListParty:T {person} this)
                })
                (\ (key: Mod:Key) -> ()  );
              } ;
          }

          module PositiveTestCase_MaintainersShouldNotUseThis {
            record @serializable T = {person: Party, name: Text};
            record @serializable TBis = {person: Text, party: Party};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit, controllers Nil @Party to upure @Unit ();
              key @PositiveTestCase_MaintainersShouldNotUseThis:TBis
                (PositiveTestCase_MaintainersShouldNotUseThis:TBis {
                  person = (PositiveTestCase_MaintainersShouldNotUseThis:T {name} this),
                  party = (PositiveTestCase_MaintainersShouldNotUseThis:T {person} this)
                })
                // In the next line, cannot use `this`
                (\ (key: PositiveTestCase_MaintainersShouldNotUseThis:TBis) ->
                  Cons @Party [(PositiveTestCase_MaintainersShouldNotUseThis:T {person} this)] (Nil @Party)  );
            };
          }

          module PositiveCase_InterfaceMethodShouldBeProperType {
            record @serializable T = {person: Party, name: Text};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit
                  , controllers Nil @Party
                  to upure @Unit ();
              implements Mod:I {
                view = Mod:MyUnit {};
                method getParties = (); // should be of type List Party
              };
            };
          }

          module PositiveCase_ImplementsShouldOverrideAllMethods {
            record @serializable T = {person: Party, name: Text};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit
                  , controllers Nil @Party
                  to upure @Unit ();
              implements Mod:I {
                view = Mod:MyUnit {};
              };
            };
          }

          module PositiveCase_ImplementsShouldOverrideOnlyMethods {
            record @serializable T = {person: Party, name: Text};

            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              choice Ch (self) (i : Unit) : Unit
                  , controllers Nil @Party
                  to upure @Unit ();
              implements Mod:I {
                view = Mod:MyUnit {};
                method getParties =
                  Cons @Party [(PositiveCase_ImplementsShouldOverrideOnlyMethods:T {person} this)] (Nil @Party);
                method getName =
                  PositiveCase_ImplementsShouldOverrideOnlyMethods:T {name} this;
              };
            };
          }
      """

      val nonTemplateTypeCases = Table(
        "moduleName",
        "PositiveTestCase_TemplateTypeShouldBeStar",
        "PositiveTestCase_TemplateTypeShouldBeRecord",
      )

      val typeMismatchCases = Table(
        "moduleName",
        "PositiveTestCase_PreconditionShouldBeBoolean",
        "PositiveTestCase_SignatoriesShouldBeListParty",
        "PositiveTestCase_ObserversShouldBeListParty",
        "PositiveTestCase_ControllersMustBeListParty",
        "PositiveTestCase_ChoiceObserversMustBeListParty",
        "PositiveTestCase_KeyBodyShouldBeProperType",
        "PositiveTestCase_MaintainersShouldBeProperType",
        "PositiveTestCase_MaintainersShouldBeListParty",
      )

      val methodTypeMismatch = Table(
        "moduleName",
        "PositiveCase_InterfaceMethodShouldBeProperType",
      )

      val kindMismatchCases = Table(
        "moduleName",
        "PositiveTestCase4",
        "PositiveTestCase5",
      )

      val pkgIface = PackageInterface(Map(defaultPackageId -> pkg))

      def checkModule(modName: String): Unit = Typing.checkModule(
        pkgIface,
        defaultPackageId,
        pkg.modules(DottedName.assertFromString(modName)),
      )

      checkModule("NegativeTestCase") shouldBe ()
      forEvery(nonTemplateTypeCases)(mod =>
        an[EExpectedTemplatableType] shouldBe thrownBy(checkModule(mod))
      )
      an[EUnknownDefinition] shouldBe thrownBy(
        checkModule("PositiveTestCase_TemplateTypeShouldExists")
      )
      forEvery(typeMismatchCases)(mod => an[ETypeMismatch] shouldBe thrownBy(checkModule(mod)))
      forEvery(methodTypeMismatch)(mod =>
        an[EMethodTypeMismatch] shouldBe thrownBy(checkModule(mod))
      )
      forEvery(kindMismatchCases)(mod => an[EKindMismatch] shouldBe thrownBy(checkModule(mod)))
      an[EUnknownExprVar] shouldBe thrownBy(
        checkModule("PositiveTestCase_MaintainersShouldNotUseThis")
      )

      an[EMissingMethodInInterfaceInstance] shouldBe thrownBy(
        checkModule("PositiveCase_ImplementsShouldOverrideAllMethods")
      )
      an[EUnknownMethodInInterfaceInstance] shouldBe thrownBy(
        checkModule("PositiveCase_ImplementsShouldOverrideOnlyMethods")
      )
    }

    // TEST_EVIDENCE: Integrity: ill-formed interfaces are rejected
    "reject ill formed interface definition" in {

      val pkg =
        p"""
          metadata ( 'pkg' : '1.0.0' )

          module Mod {
            record @serializable MyUnit = {};
            record @serializable Box a = {x: a};
            enum Color = Red | Green | Blue;
          }

          module NegativeTestCase {
            interface (this : X) = {
              viewtype Mod:MyUnit;
              method getX: List Party;
            };

            interface (this : I) =  {
              viewtype Mod:MyUnit;
              method getParties: List Party;
              choice Ch1 (self) (i : Unit) : Unit,
                  controllers Nil @Party
                to upure @Unit ();
              choice Ch2 (self) (i : Unit) : Unit,
                 controllers call_method @NegativeTestCase:I getParties this,
                 observers Nil @Party
               to upure @Unit ();
              choice Ch3 (self) (i : Unit) : Unit,
                  controllers call_method @NegativeTestCase:I getParties this,
                  observers call_method @NegativeTestCase:I getParties this
                to upure @Unit ();
            } ;
          }

          module PositiveTestCase_ControllersShouldBeListParty {
            interface (this : I) =  {
              viewtype Mod:MyUnit;
              choice Ch (self) (i : Unit) : Unit,
                  controllers ()                                  // should be of type (List Party)
                to upure @Unit ();
            } ;
          }

          module PositiveTestCase_ChoiceObserversShouldBeListParty {
             interface (this : I) =  {
              viewtype Mod:MyUnit;
              method getParties: List Party;
              choice Ch (self) (i : Unit) : Unit,
                  controllers (call_method @NegativeTestCase:I getParties this),
                  observers ()                                  // should be of type (List Party)
                to upure @Unit ();
            } ;
          }

          module PositiveTestCase_ChoiceArgumentTypeShouldBeStar {
             interface (this : I) =  {
              viewtype Mod:MyUnit;
              method getParties: List Party;
              choice Ch (self) (i : List) : Unit,   // the type of i (here List) should be of kind * (here it is * -> *)
                  controllers (call_method @NegativeTestCase:I getParties this)
                to upure @Unit ();
            } ;
          }

          module PositiveTestCase_ChoiceResultTypeShouldBeStar {
             interface (this : I) =  {
              viewtype Mod:MyUnit;
              method getParties: List Party;
              choice Ch (self) (i : Unit) : List,   // the return type (here List) should be of kind * (here it is * -> *)
                  controllers (call_method @NegativeTestCase:I getParties this)
                to upure @(List) (/\ (tau : *). Nil @tau);
            } ;
          }

          module PositiveTestCase_UnknownDefinition {
              interface (this : I) =  {
              viewtype Mod:MyUnit;
              requires NegativeTestCase:J;
            } ;
          }

          module PositiveTestCase_MissingRequiredInterface {
            interface (this : Y) =  {
              viewtype Mod:MyUnit;
              requires NegativeTestCase:X;
            } ;

            record @serializable T = { person: Party, name: Text };
            template (this : T) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              implements PositiveTestCase_MissingRequiredInterface:Y {
                view = Mod:MyUnit {};
              };
           };
          }

          module PositiveTestCase_WrongInterfaceRequirement1 {
            interface (this : Y) = { viewtype Mod:MyUnit; };
            interface (this : Z) = { viewtype Mod:MyUnit; };
            interface (this : X) =  {
              viewtype Mod:MyUnit;
              requires PositiveTestCase_WrongInterfaceRequirement1:Y;
              choice ToY (self) (arg : Unit) : PositiveTestCase_WrongInterfaceRequirement1:Z,
                  controllers Nil@Party
                to upure @PositiveTestCase_WrongInterfaceRequirement1:Z
                          (to_required_interface @PositiveTestCase_WrongInterfaceRequirement1:Z
                                                 @PositiveTestCase_WrongInterfaceRequirement1:X
                                                 this);
            } ;
          }

          module PositiveTestCase_WrongInterfaceRequirement2 {
            interface (this : Y) = { viewtype Mod:MyUnit; };
            interface (this : Z) = { viewtype Mod:MyUnit; };
            interface (this : X) =  {
              viewtype Mod:MyUnit;
              requires PositiveTestCase_WrongInterfaceRequirement2:Y;
              choice ToY (self) (arg : PositiveTestCase_WrongInterfaceRequirement2:Z) : Option PositiveTestCase_WrongInterfaceRequirement2:X,
                  controllers Nil@Party
                to upure @(Option PositiveTestCase_WrongInterfaceRequirement2:X)
                          (from_required_interface @PositiveTestCase_WrongInterfaceRequirement2:Z
                                                   @PositiveTestCase_WrongInterfaceRequirement2:X
                                                   arg);
            } ;
          }

          module NegativeTestCase_WrongInterfaceRequirement3 {
            interface (this : Y) = { viewtype Mod:MyUnit; };
            interface (this : Z) = { viewtype Mod:MyUnit; };
            interface (this : X) =  {
              viewtype Mod:MyUnit;
              requires NegativeTestCase_WrongInterfaceRequirement3:Y;
              choice ToY (self) (arg : Unit) : NegativeTestCase_WrongInterfaceRequirement3:Y,
                  controllers Nil@Party
                to upure @NegativeTestCase_WrongInterfaceRequirement3:Y
                          (to_required_interface @NegativeTestCase_WrongInterfaceRequirement3:Y
                                                 @NegativeTestCase_WrongInterfaceRequirement3:X
                                                 this);
            } ;
          }

          module NegativeTestCase_WrongInterfaceRequirement4 {
            interface (this : Y) = { viewtype Mod:MyUnit; };
            interface (this : Z) = { viewtype Mod:MyUnit; };
            interface (this : X) = {
              viewtype Mod:MyUnit;
              requires NegativeTestCase_WrongInterfaceRequirement4:Y;
              choice ToY (self) (arg : NegativeTestCase_WrongInterfaceRequirement4:Y) : Option NegativeTestCase_WrongInterfaceRequirement4:X,
                  controllers Nil@Party
                to upure @(Option NegativeTestCase_WrongInterfaceRequirement4:X)
                          (from_required_interface @NegativeTestCase_WrongInterfaceRequirement4:Y
                                                   @NegativeTestCase_WrongInterfaceRequirement4:X
                                                   arg);
            } ;
          }

          module PositiveTestCase_CircularInterfaceRequires {
            interface (this : X) =  {
              viewtype Mod:MyUnit;
              requires PositiveTestCase_CircularInterfaceRequires:Y;
              requires PositiveTestCase_CircularInterfaceRequires:Z;
              requires PositiveTestCase_CircularInterfaceRequires:X;
            };
            interface (this : Y) = { viewtype Mod:MyUnit; };
            interface (this : Z) = { viewtype Mod:MyUnit; };
          }

          module PositiveTestCase_NotClosedInterfaceRequires {
            interface (this : X) =  {
              viewtype Mod:MyUnit;
              requires PositiveTestCase_NotClosedInterfaceRequires:Y;
            };
            interface (this : Y) = {
              viewtype Mod:MyUnit;
              requires PositiveTestCase_NotClosedInterfaceRequires:Z;
            };
            interface (this : Z) = {
              viewtype Mod:MyUnit;
              requires PositiveTestCase_NotClosedInterfaceRequires:X;
            };
          }

          module PositiveTestCase_ViewtypeIsNotUserDefined {
            interface (this : X) = {
              viewtype Unit;
            };
          }

          module PositiveTestCase_ViewtypeIsNotARecord {
            interface (this : X) = {
              viewtype Mod:Color;
            };
          }

           module PositiveTestCase_ViewtypeIsNotMonomorphic {
            interface (this : X) = {
              viewtype Mod:Box;
            };
          }
      """

      val typeMismatchCases = Table(
        "moduleName",
        "PositiveTestCase_ControllersShouldBeListParty",
        "PositiveTestCase_ChoiceObserversShouldBeListParty",
      )

      val kindMismatchCases = Table(
        "moduleName",
        "PositiveTestCase_ChoiceArgumentTypeShouldBeStar",
        "PositiveTestCase_ChoiceResultTypeShouldBeStar",
      )

      val pkgInterface = PackageInterface(Map(defaultPackageId -> pkg))

      def checkModule(pkg: Package, modName: String) =
        Typing.checkModule(
          pkgInterface,
          defaultPackageId,
          pkg.modules(DottedName.assertFromString(modName)),
        )

      checkModule(pkg, "NegativeTestCase")
      checkModule(pkg, "NegativeTestCase_WrongInterfaceRequirement3")
      checkModule(pkg, "NegativeTestCase_WrongInterfaceRequirement4")
      forEvery(typeMismatchCases)(module =>
        an[ETypeMismatch] shouldBe thrownBy(checkModule(pkg, module))
      )
      forEvery(kindMismatchCases)(module =>
        an[EKindMismatch] shouldBe thrownBy(checkModule(pkg, module))
      )
      an[EUnknownDefinition] shouldBe thrownBy(
        checkModule(pkg, "PositiveTestCase_UnknownDefinition")
      )
      an[EMissingRequiredInterfaceInstance] shouldBe thrownBy(
        checkModule(pkg, "PositiveTestCase_MissingRequiredInterface")
      )
      an[EWrongInterfaceRequirement] shouldBe thrownBy(
        checkModule(pkg, "PositiveTestCase_WrongInterfaceRequirement1")
      )
      an[EWrongInterfaceRequirement] shouldBe thrownBy(
        checkModule(pkg, "PositiveTestCase_WrongInterfaceRequirement2")
      )
      an[ECircularInterfaceRequires] shouldBe thrownBy(
        checkModule(pkg, "PositiveTestCase_CircularInterfaceRequires")
      )
      an[ENotClosedInterfaceRequires] shouldBe thrownBy(
        checkModule(pkg, "PositiveTestCase_NotClosedInterfaceRequires")
      )
      an[EViewTypeHasVars] shouldBe thrownBy(
        checkModule(pkg, "PositiveTestCase_ViewtypeIsNotMonomorphic")
      )
      an[EViewTypeConNotRecord] shouldBe thrownBy(
        checkModule(pkg, "PositiveTestCase_ViewtypeIsNotARecord")
      )
      an[EViewTypeHeadNotCon] shouldBe thrownBy(
        checkModule(pkg, "PositiveTestCase_ViewtypeIsNotUserDefined")
      )
    }

  }

  "checkModule" should {

    // TEST_EVIDENCE: Integrity: ill-formed exception definitions are rejected
    "reject ill formed exception definitions" in {

      val pkg =
        p"""
          metadata ( 'pkg' : '1.0.0' )

          module Mod {
            record @serializable Exception = { message: Text };
          }

          module NegativeTestCase {
            record @serializable Exception = { message: Text } ;

            exception Exception = {
              message \(e: NegativeTestCase:Exception) -> NegativeTestCase:Exception {message} e
            } ;
          }

          module PositiveTestCase1 {
            record @serializable Exception (a: *) = { message: Text } ; // should not have parameter

            exception Exception = {
              message \(e: PositiveTestCase1:Exception) -> PositiveTestCase1:Exception {message} e
            } ;
          }

         module PositiveTestCase2 {
            variant @serializable Exception = Message : Text  ; // should be a record

            exception Exception = {
              message \(e: PositiveTestCase2:Exception) -> PositiveTestCase2:Exception {message} e
            } ;
          }

         module PositiveTestCase3 {
           exception Exception = {         // should match a existing record in the same module
             message \(e: Mod:Exception) -> PositiveTestCase3:Exception {message} e
           } ;
         }

         module PositiveTestCase4 {
           record @serializable Exception = { message: Text } ;

           exception Exception = {
             message "some error message"   // should be of type PositiveTestCase4:Exception -> Text
           } ;
         }


      """

      def checkModule(pkg: Package, modName: String) = Typing.checkModule(
        PackageInterface(Map(defaultPackageId -> pkg)),
        defaultPackageId,
        pkg.modules(DottedName.assertFromString(modName)),
      )

      checkModule(pkg, "NegativeTestCase")
      an[EExpectedExceptionableType] shouldBe thrownBy(checkModule(pkg, "PositiveTestCase1"))
      an[EExpectedExceptionableType] shouldBe thrownBy(checkModule(pkg, "PositiveTestCase2"))
      an[EUnknownDefinition] shouldBe thrownBy(checkModule(pkg, "PositiveTestCase3"))
      an[ETypeMismatch] shouldBe thrownBy(checkModule(pkg, "PositiveTestCase4"))
    }

    "accepts regression test #3777" in {
      // This is a regression test for https://github.com/digital-asset/daml/issues/3777
      def pkg =
        p"""
        metadata ( 'pkg' : '1.0.0' )

        module TypeVarShadowing2 {

         val bar : forall b1 b2 a1 a2. (b1 -> b2) -> (a1 -> a2) -> a1 -> a2 =
             /\b1 b2 a1 a2. \(f : b1 -> b2) (g : a1 -> a2) -> g ;

          val baz : forall a1 a2 b1 b2. (a1 -> a2) -> (b1 -> b2) -> b1 -> b2 =
            /\a1 a2 b1 b2.
              \(f : a1 -> a2) (g : b1 -> b2) ->
                TypeVarShadowing2:bar @a1 @a2 @b1 @b2 f g;
        }
      """

      val mod = pkg.modules(DottedName.assertFromString("TypeVarShadowing2"))
      Typing.checkModule(PackageInterface(Map(defaultPackageId -> pkg)), defaultPackageId, mod)
    }

    "expand type synonyms correctly" in {
      val testCases = Table(
        "expression" ->
          "expected type",
        E"(( λ (e : |Mod:SynInt|) → () )) " ->
          T"(( Int64 → Unit ))",
        E"(( λ (e : |Mod:SynSynInt|) → () )) " ->
          T"(( Int64 → Unit ))",
        E"(( λ (e : |Mod:SynIdentity Int64|) → () )) " ->
          T"(( Int64 → Unit ))",
        E"(( λ (e : |Mod:SynIdentity |Mod:SynIdentity Int64||) → () )) " ->
          T"(( Int64 → Unit ))",
        E"(( λ (e : |Mod:SynList Date|) → () )) " ->
          T"(( List Date → Unit ))",
        E"(( λ (e : |Mod:SynSelfFunc Text|) → () )) " ->
          T"(( (Text → Text) → Unit ))",
        E"(( λ (e : |Mod:SynFunc Text Date|) → () )) " ->
          T"(( (Text → Date) → Unit ))",
        E"(( λ (e : |Mod:SynPair Text Date|) → () )) " ->
          T"(( <one:Text, two: Date> → Unit ))",
        E"(( λ (e : forall (a:*) . a) → () )) " ->
          T"(( (forall (a:*) . a) → Unit ))",
        E"(( λ (e : |Mod:SynIdentity (forall (a:*) . a)|) → () )) " ->
          T"(( (forall (a:*) . a) → Unit ))",
        E"(( λ (e : forall (a:*) . |Mod:SynIdentity a|) → () )) " ->
          T"(( (forall (a:*) . a) → Unit ))",
        E"(( λ (e : |Mod:SynHigh List|) → () )) " ->
          T"(( List Int64 → Unit ))",
        E"(( λ (e : |Mod:SynHigh2 GenMap Party|) → () )) " ->
          T"(( (GenMap Party Party) → Unit ))",
      )

      forEvery(testCases) { (exp: Expr, expectedType: Type) =>
        env.expandTypeSynonyms(env.typeOfTopExpr(exp)) shouldBe expectedType
      }
    }

    // TEST_EVIDENCE: Integrity: ill-formed type synonyms applications are rejected
    "reject ill formed type synonym application" in {
      val testCases = Table(
        "badly formed type synonym application",
        E"(( λ (e : |Mod:MissingSyn|) → () )) ",
        E"(( λ (e : |Mod:SynInt Text|) → () )) ",
        E"(( λ (e : |Mod:SynIdentity|) → () )) ",
        E"(( λ (e : |Mod:SynIdentity Text Text|) → () )) ",
        E"(( λ (e : |Mod:SynPair Text|) → () )) ",
        E"(( λ (e : |Mod:SynPair Text Text Text|) → () )) ",
        E"(( λ (e : |Mod:SynIdentity List|) → () )) ",
        E"(( λ (e : |Mod:SynHigh Text|) → () )) ",
        E"(( λ (e : |Mod:SynHigh GenMap|) → () )) ",
        E"(( λ (e : |Mod:SynHigh2 List Party|) → () )) ",
      )

      forEvery(testCases) { exp =>
        a[ValidationError] should be thrownBy env.typeOfTopExpr(exp)
      }
    }

    // TEST_EVIDENCE: Integrity: ill-formed records are rejected
    "reject ill formed type record definitions" in {

      def checkModule(mod: Module) = {
        val pkg = Package.build(List(mod), List.empty, defaultLanguageVersion, packageMetadata)
        Typing.checkModule(PackageInterface(Map(defaultPackageId -> pkg)), defaultPackageId, mod)
      }

      val negativeTestCases = Table(
        "valid module",
        m"""module Mod { record R (a: * -> *) (b: * -> *) = { }; }""",
      )

      val positiveTestCases = Table(
        "invalid module",
        m"""module Mod { record R (a: * -> nat) (b: * -> *) = { }; }""",
        m"""module Mod { record R (a: * -> *) (b: * -> nat) = { }; }""",
      )

      forEvery(negativeTestCases)(mod => checkModule(mod))
      forEvery(positiveTestCases)(mod => a[ValidationError] should be thrownBy checkModule(mod))
    }

    // TEST_EVIDENCE: Integrity: ill-formed variants are rejected
    "reject ill formed type variant definitions" in {

      def checkModule(mod: Module) = {
        val pkg = Package.build(List(mod), List.empty, defaultLanguageVersion, packageMetadata)
        Typing.checkModule(PackageInterface(Map(defaultPackageId -> pkg)), defaultPackageId, mod)
      }

      val negativeTestCases = Table(
        "valid module",
        m"""module Mod { variant V (a: * -> *) (b: * -> *) = ; }""",
      )

      val positiveTestCases = Table(
        "invalid module",
        m"""module Mod { variant V (a: * -> nat) (b: * -> *) = ; }""",
        m"""module Mod { variant V (a: * -> *) (b: * -> nat) = ; }""",
      )

      forEvery(negativeTestCases)(mod => checkModule(mod))
      forEvery(positiveTestCases)(mod => a[ValidationError] should be thrownBy checkModule(mod))
    }

    // TEST_EVIDENCE: Integrity: ill-formed type synonyms definitions are rejected
    "reject ill formed type synonym definitions" in {

      def checkModule(mod: Module) = {
        val pkg = Package.build(List(mod), List.empty, defaultLanguageVersion, packageMetadata)
        Typing.checkModule(PackageInterface(Map(defaultPackageId -> pkg)), defaultPackageId, mod)
      }

      val negativeTestCases = Table(
        "valid module",
        m"""module Mod { synonym S = Int64 ; }""",
        m"""module Mod { synonym S a = a ; }""",
        m"""module Mod { synonym S a b = a ; }""",
        m"""module Mod { synonym S (f: *) = f ; }""",
        m"""module Mod { synonym S (f: * -> *) = f Int64; }""",
        m"""module Mod { synonym S (f: * -> *) = Unit ; }""",
      )

      val positiveTestCases = Table(
        "invalid module",
        m"""module Mod { synonym S = a ; }""",
        m"""module Mod { synonym S a = b ; }""",
        m"""module Mod { synonym S a a = a ; }""",
        m"""module Mod { synonym S = List ; }""",
        m"""module Mod { synonym S (f: * -> *) = f ; }""",
        m"""module Mod { synonym S (f: *) = f Int64; }""",
        m"""module Mod { synonym S (f: * -> nat) = Unit ; }""",
      )

      forEvery(negativeTestCases)(mod => checkModule(mod))
      forEvery(positiveTestCases)(mod => a[ValidationError] should be thrownBy checkModule(mod))
    }

  }

  private[this] val env = {
    val pkg =
      p"""
       metadata ( 'pkg' : '1.0.0' )

       module Mod {
         record R (a: *) = { f1: Int64, f2: List a } ;

         variant Tree (a: *) =  Node : < left: Mod:Tree a, right: Mod:Tree a > | Leaf : a ;

         enum Color = Red | Green | Blue ;

         synonym SynInt = Int64 ;
         synonym SynSynInt = |Mod:SynInt| ;
         synonym SynIdentity (a: *) = a ;
         synonym SynList (a: *) = List a ;
         synonym SynSelfFunc (a: *) = a -> a ;
         synonym SynFunc (a: *) (b: *) = a -> b ;
         synonym SynPair (a: *) (b: *) = <one: a, two: b>;
         synonym SynHigh (f: * -> *) = f Int64 ;
         synonym SynHigh2 (f: * -> * -> *) (a: *) = f a a ;

         synonym S = Mod:U;

         record @serializable T = { person: Party, name: Text };
         template (this : T) =  {
           precondition True;
           signatories Nil @Party;
           observers Nil @Party;
           choice Ch (self) (x: Int64) : Numeric 10, controllers Nil @Party to upure @INT64 (NUMERIC_TO_INT64 @1 x);
           key @Party (Mod:Person {person} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party));
         };

         interface (this : I) = {
              viewtype Mod:MyUnit;
              method getParties: List Party;
              choice ChIface (self) (x: Int64) : Numeric 10,
                  controllers Nil @Party
                to upure @INT64 (NUMERIC_TO_INT64 @10 x);
         };

         interface (this : SubI) = {
              viewtype Mod:MyUnit;
              requires Mod:I;
         };

          interface (this : J) = { viewtype Mod:MyUnit; };

         record @serializable Ti = { person: Party, name: Text };
         template (this: Ti) = {
           precondition True;
           signatories Nil @Party;
           observers Nil @Party;
           choice ChTmpl (self) (x: Int64) : Numeric 10, controllers Nil @Party to upure @INT64 (NUMERIC_TO_INT64 @10 x);
           implements Mod:I {
              view = Mod:MyUnit {};
              method getParties = Cons @Party [(Mod:Ti {person} this)] (Nil @Party);
           };
         };

         record @serializable U = { person: Party, name: Text };

         val f : Int64 -> Bool = ERROR @(Bool -> Int64) "not implemented";

         record @serializable E = { message: Text };
         exception E = { message \(e: Mod:E) -> Mod:E {message} e };

       }
     """
    Typing.Env(LV.default, PackageInterface(Map(defaultPackageId -> pkg)), Context.None)
  }

}
