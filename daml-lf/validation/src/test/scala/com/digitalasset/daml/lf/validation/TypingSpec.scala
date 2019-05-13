// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.archive.LanguageVersion
import com.digitalasset.daml.lf.data.Ref.DottedName
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.testing.parser._
import com.digitalasset.daml.lf.validation.SpecUtil._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

class TypingSpec extends WordSpec with TableDrivenPropertyChecks with Matchers {

  "Checker.kindOf" should {

    "infers the proper kind for builtin types (but ContractId)" in {
      val testCases = Table(
        "builtin type" -> "expected kind",
        BTInt64 -> k"*",
        BTDecimal -> k"*",
        BTText -> k"*",
        BTTimestamp -> k"*",
        BTParty -> k"*",
        BTUnit -> k"*",
        BTBool -> k"*",
        BTList -> k"* -> *",
        BTUpdate -> k"* -> *",
        BTScenario -> k"* -> *",
        BTDate -> k"*",
        BTContractId -> k"* -> *",
        BTArrow -> k"* -> * -> *",
      )

      forEvery(testCases) { (bType: BuiltinType, expectedKind: Kind) =>
        env.kindOf(TBuiltin(bType)) shouldBe expectedKind
      }
    }

    "infers the proper kind for complex type" in {

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

    "does not allow type variable shadowing" in {
      // Here env contains the variable named "alpha"
      an[EShadowingTypeVar] should be thrownBy env.kindOf(
        t"forall (a:*). forall (a:*). alpha -> Bool")
    }
  }

  "Checker.typeOf" should {

    "infers the proper type for expression" in {
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
        // ExpLitDecimal
        E"(( 3.1415926536 ))" -> T"(( Decimal ))",
        //ExpLitText
        E"""(( "text" ))""" -> T"(( Text ))",
        //ExpLitDate
        E"(( 1879-03-14 ))" -> T"(( Date ))",
        //ExpLitTimestamp
        E"(( 1969-07-20T20:17:00.000000Z ))" -> T"(( Timestamp ))",
        //ExpLitParty
        E"(( 'party' ))" -> T"(( Party ))",
        //Map
        E"Λ (τ : ⋆) . (( MAP_EMPTY @τ ))" -> T"∀ (τ : ⋆) . (( Map τ ))",
        //ExpVal
        E"(( Mod:f ))" -> T"(( Int64 →  Bool ))",
        //ExpRecCon
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
        // ExpTupleCon
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e₁ : τ₁) (e₂ : τ₂)  →  (( ⟨ f₁ = e₁, f₂ = e₂ ⟩ ))" ->
          T"∀ (τ₁ : ⋆) (τ₂ : ⋆). τ₁ → τ₂ → (( ⟨ f₁: τ₁, f₂: τ₂ ⟩ ))",
        // ExpTupleProj
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) → (( (e).f₂ ))" ->
          T"∀ (τ₁ : ⋆) (τ₂ : ⋆) . ⟨ f₁: τ₁, f₂: τ₂ ⟩ → (( τ₂ ))",
        // ExpTupleUpdate
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) (e₂ : τ₂)  → (( ⟨ e with f₂ = e₂ ⟩ ))" ->
          T"∀ (τ₁ : ⋆) (τ₂ : ⋆) . ⟨ f₁: τ₁, f₂: τ₂ ⟩ → τ₂ → (( ⟨ f₁: τ₁, f₂: τ₂ ⟩ ))",
        // ExpCaseVariant
        E"Λ (τ : ⋆). λ (e : Mod:Tree τ) → (( case e of Mod:Tree:Node x → (x).left | Mod:Tree:Leaf x -> e ))" ->
          T"∀ (τ : ⋆). Mod:Tree τ →  ((  Mod:Tree τ  ))",
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
      )

      forEvery(testCases) { (exp: Expr, expectedType: Type) =>
        env.typeOf(exp) shouldBe expectedType
      }
    }

    "infers proper type for Scenarios" in {
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
        env.typeOf(exp) shouldBe expectedType
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
        E"λ (e₁: ContractId Mod:T) (e₂: List Party) (e₃: Int64) → (( exercise @Mod:T Ch e₁ e₂ e₃ ))" ->
          T"ContractId Mod:T → List Party → Int64 → (( Update Decimal ))",
        E"λ (e: ContractId Mod:T) → (( fetch @Mod:T e ))" ->
          T"ContractId Mod:T → (( Update Mod:T ))",
        E"fetch_by_key @Mod:T 'Bob'" ->
          T"Update (⟨ contract: Mod:T, contractId: ContractId Mod:T ⟩)",
        E"lookup_by_key @Mod:T 'Bob'" ->
          T"Update (Option (ContractId Mod:T))",
        E"(( uget_time ))" ->
          T"(( Update Timestamp ))",
        E"Λ (τ : ⋆). λ (e: Update τ) →(( uembed_expr @τ e ))" ->
          T"∀ (τ : ⋆). Update τ -> (( Update τ ))"
      )

      forEvery(testCases) { (exp: Expr, expectedType: Type) =>
        env.typeOf(exp) shouldBe expectedType
      }
    }

    "shadow variables properly" in {

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
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: List σ) → (( λ (x : τ) → case e of Cons x t → x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). List σ → (( τ → σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: List σ) → (( case e of Cons x t → λ (x : τ) → x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). List σ → (( τ  → τ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: Scenario σ) → (( sbind x: σ ← e in spure @(τ → τ) (λ (x : τ) → x) ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Scenario σ → (( Scenario (τ  → τ) ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: Scenario σ) → (( λ (x : τ) → sbind x: σ ← e in spure @σ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Scenario σ → (( τ → Scenario σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Scenario τ) (e₂: Scenario σ)  → (( sbind x: τ ← e₁ ; x: σ ← e₂ in spure @σ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Scenario τ → Scenario σ → (( Scenario σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: Update σ) → (( ubind x: σ ← e in upure @(τ → τ) (λ (x : τ) → x) ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Update σ → (( Update (τ  → τ) ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: Update σ) → (( λ (x : τ) → ubind x: σ ← e in upure @σ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Update σ → (( τ → Update σ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Update τ) (e₂: Update σ)  → (( ubind x: τ ← e₁ ; x: σ ← e₂ in upure @σ x ))" ->
          T"∀ (τ : ⋆) (σ : ⋆). Update τ → Update σ → (( Update σ ))",
      )

      forEvery(testCases) { (exp: Expr, expectedType: Type) =>
        env.typeOf(exp) shouldBe expectedType
      }
    }

    "reject ill formed terms" in {
      val testCases = Table(
        "non-well formed expression",
        // ExpDefVar
        E"x",
        // ExpApp
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₂) (e₂ : τ₁) → (( e₁ e₂ ))",
        E"Λ (τ₁: ⋆) (τ₂ : ⋆) (τ₃ : ⋆). λ (e₁ : τ₂ → τ₃) (e₂ : τ₁) → (( e₁ e₂ ))",
        E"Λ (τ₁: ⋆) (τ₂ : ⋆) (τ₃ : ⋆). λ (e₁ : (τ₁ → τ₂) → τ₃) (e₂ : τ₁) → (( e₁ e₂ ))",
        E"Λ (τ₁: ⋆) (τ₂ : ⋆) (τ₃ : ⋆). λ (e₁ : Bool) (e₂ : τ₃) → (( e₁ e₂ ))",
        // ExpTyApp
        E"Λ (τ : ⋆ → ⋆) (σ: ⋆ → ⋆). λ (e : ∀ (α : ⋆). σ α) → (( e @τ ))",
        E"Λ (τ : ⋆) (σ: ⋆ → ⋆). λ (e : ∀ (α : ⋆ → ⋆). σ α) → (( e @τ ))",
        // ExpAbs
        E"Λ  (τ : ⋆ → ⋆) (σ: ⋆) . λ (e: τ → σ) → λ (x : τ) → (( e x ))",
        // ExpLet
        E"Λ  (τ₁: ⋆) (τ₂ : ⋆) (σ: ⋆). λ (e₁ : τ₁) (e₂ : σ) → (( let x : τ₂ = e₁ in e₂ ))",
        E"Λ (τ : ⋆ → ⋆) (σ: ⋆). λ (e₁ : τ) (e₂ : τ → σ) → (( let x : τ = e₁ in e₂ x ))",
        // ExpListNil
        E"Λ (τ : ⋆ → ⋆). (( Nil @τ ))",
        // ExpListCons
        E"Λ (τ : ⋆ → ⋆). λ (e₁ : τ) (e₂ : τ) (e : List τ) → (( Cons @τ [e₁, e₂] e ))",
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₂) (e₂ : τ₁) (e : List τ₁) → (( Cons @τ₁ [e₁, e₂] e ))",
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₂) (e : List τ₁) → (( Cons @τ₁ [e₁] e ))",
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₂) (e : List τ₁) → (( Cons @τ₁ [e₁] e ))",
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₁) (e : List τ₂) → (( Cons @τ₁ [e₁] e ))",
        E"Λ (τ₁: ⋆) (τ₂ : ⋆). λ (e₁ : τ₁) (e : List τ₁) → (( Cons @τ₂ [e₁] e ))",
        E"Λ (τ : ⋆). λ (e : List τ) → (( Cons @τ [] e ))",
        //ExpVal
        E"(( Mod:g ))",
        //ExpRecCon
        E"Λ (σ : ⋆). λ (e₁ : Bool) (e₂ : List σ) → (( Mod:R @σ { f1 = e₁, f2 =e₂ } ))",
        E"Λ (σ : ⋆ → ⋆). λ (e₁ : Int64) (e₂ : List σ) → (( Mod:R @σ { f1 = e₁, f2 =e₂ } ))",
        E"Λ (σ : ⋆). λ (e₁ : Int64) (e₂ : List σ) → (( Mod:R @σ { f1 = e₁, f3 =e₂ } ))",
        E"Λ (σ : ⋆). λ (e₁ : Int64) (e₂ : List σ) → (( Mod:R @σ { f1 = e₁ } ))",
        E"Λ (σ : ⋆) (τ: ⋆). λ (e₁ : Int64) (e₂ : List σ) (e₃:τ) → (( Mod:R @σ { f1 = e₁, f2 = e₂, f3 = e₃} ))",
        // ExpRecProj
        E"Λ (σ : ⋆ → ⋆). λ (e : Mod:R σ) → (( Mod:R @σ (e).f2 ))",
        E"Λ (σ : ⋆). λ (e : Mod:R σ) → (( Mod:R @σ (e).f3 ))",
        // ExpRecUpdate
        E"Λ (σ : ⋆). λ (e : Mod:R σ) (e₂ : List σ) → (( Mod:R @σ { e  with f3 = e₂ } ))",
        E"Λ (σ : ⋆). λ (e : Mod:R σ) (e₂ : Bool) → (( Mod:R @σ { e  with f2 = e₂ } ))",
        // ExpVarCon
        E"Λ (σ : ⋆ → ⋆). λ (e : σ) → (( Mod:Tree:Leaf @σ e ))",
        E"Λ (σ : ⋆). λ (e : σ) → (( Mod:Tree @σ Cons @σ [e] (Nil @σ) ))",
        // ExpTupleCon
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e₁ : τ₁)  → (( ⟨ f₁ = e₁, f₁ = e₁ ⟩ ))",
        // ExpTupleProj
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) → (( (e).f3 ))",
        // ExpTupleUpdate
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) (e₂ : τ₂)  → (( ⟨ e with f₃ = e₂ ⟩ ))",
        E"Λ (τ₁ : ⋆) (τ₂ : ⋆) (τ₃: ⋆). λ (e: ⟨ f₁: τ₁, f₂: τ₂ ⟩) (e₃: τ₃)  → (( ⟨ e with f₂ = e₃ ⟩ ))",
        // ExpCaseVariant
        E"Λ (τ : ⋆). λ (e : Mod:Tree τ) → ((  case e of Cons h t -> () ))",
        E"Λ (τ : ⋆). λ (e : List τ) → (( case e of Mod:Tree:Node x -> () ))",
        // ExpCaseNil
        E"Λ (τ : ⋆). λ (e : τ) → (( case e of Nil → () ))",
        // ExpCaseCons
        E"Λ (τ : ⋆). λ (e : τ) → (( case e of Cons x y → () ))",
        // ExpCaseFalse & ExpCaseTrue
        E"Λ (τ : ⋆). λ (e : τ) → (( case e of True → () ))",
        E"Λ (τ : ⋆). λ (e : τ) → (( case e of False → () ))",
        // ExpCaseUnit
        E"Λ (τ : ⋆). λ (e : τ) → (( case e of () → () ))",
        // ExpCaseOr
        E"Λ (τ : ⋆). λ (e : τ) → (( case e of  ))",
        // ScnPure
        E"Λ (τ : ⋆ → ⋆). λ (e: τ) → (( spure @τ e ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: τ) → (( spure @σ e ))",
        // ScnBlock
        E"Λ (τ : ⋆ → ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (e: Scenario τ) → (( sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆ → ⋆) (τ₁ : ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (e: Scenario τ) → (( sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆ → ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (e: Scenario τ) → (( sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁:  τ₁) (e₂: Scenario τ₂) (e: Scenario τ) → (( sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: Scenario τ₁) (e₂:τ₂) (e: Scenario τ) → (( sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (f: τ) → (( sbind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆) (σ : ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (e: Scenario τ) → (( sbind x₁: σ  ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆) (σ : ⋆). λ (e₁: Scenario τ₁) (e₂: Scenario τ₂) (e: Scenario τ) → (( sbind x₁: τ₁ ← e₁ ;  x₂: σ ← e₂ in e ))",
        // ScnCommit
        E"Λ (τ : ⋆ → ⋆). λ (e₁: Party) (e₂: Update τ) → (( commit @τ e₁ e₂ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: σ) (e₂: Update τ) → (( commit @τ e₁ e₂ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: Update σ) → (( commit @τ e₁ e₂ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: σ) → (( commit @τ e₁ e₂ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: Update τ) → (( commit @σ e₁ e₂ ))",
        // ScnMustFail
        E"Λ (τ : ⋆ → ⋆). λ (e₁: Party) (e₂: Update τ) → (( must_fail_at @τ e₁ e₂ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: σ) (e₂: Update τ) → (( must_fail_at @τ e₁ e₂ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: Update σ) → (( must_fail_at @τ e₁ e₂ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: σ) → (( must_fail_at @τ e₁ e₂ ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e₁: Party) (e₂: Update τ) → (( must_fail_at @σ e₁ e₂ ))",
        // ScnPass
        E"Λ (σ : ⋆). λ (e: σ) → (( pass e ))",
        // ScnGetParty
        E"Λ (σ : ⋆). λ (e: σ) → (( sget_party e ))",
        // ScnEmbedExpr
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : Scenario σ) → (( sembed_expr @τ e ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : σ) → (( sembed_expr @τ e ))",
        //  UpdPure
        E"Λ (τ : ⋆ → ⋆). λ (e: τ) → (( upure @τ e ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e: τ) → (( upure @σ e ))",
        // UpdBlock
        E"Λ (τ : ⋆ → ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: e: Update τ τ₁) (e₂: e: Update τ τ₂) (e: e: Update τ τ) → (( ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆ → ⋆) (τ₁ : ⋆). λ (e₁: e: Update τ τ₁) (e₂: e: Update τ τ₂) (e: e: Update τ τ) → (( ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆ → ⋆). λ (e₁: e: Update τ τ₁) (e₂: e: Update τ τ₂) (e: e: Update τ τ) → (( ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁:  τ₁) (e₂: e: Update τ τ₂) (e: e: Update τ τ) → (( ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: e: Update τ τ₁) (e₂:τ₂) (e: e: Update τ τ) → (( ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆). λ (e₁: e: Update τ τ₁) (e₂: e: Update τ τ₂) (f: τ) → (( ubind x₁: τ₁ ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆) (σ : ⋆). λ (e₁: e: Update τ τ₁) (e₂: e: Update τ τ₂) (e: e: Update τ τ) → (( ubind x₁: σ  ← e₁ ;  x₂: τ₂ ← e₂ in e ))",
        E"Λ (τ : ⋆) (τ₂ : ⋆) (τ₁ : ⋆) (σ : ⋆). λ (e₁: e: Update τ τ₁) (e₂: e: Update τ τ₂) (e: e: Update τ τ) → (( ubind x₁: τ₁ ← e₁ ;  x₂: σ ← e₂ in e ))",
        // UpdCreate
        E"λ (e: Mod:R) → (( create @Mod:R e))",
        // UpdExercise
        E"λ (e₁: ContractId Mod:R) (e₂: List Party) (e₃: Int64) → (( exercise @Mod:R Ch e₁ e₂ e₃ ))",
        E"λ (e₁: ContractId Mod:T) (e₂: List Party) (e₃: Int64) → (( exercise @Mod:T Not e₁ e₂ e₃ ))",
        E"Λ (σ : ⋆).λ (e₁: ContractId Mod:T) (e₂: List Party) (e₃: σ) → (( exercise @Mod:T Ch e₁ e₂ e₃ ))",
        E"Λ (σ : ⋆).λ (e₁: ContractId Mod:T) (e₂: List σ) (e₃: Int64) → (( exercise @Mod:T Ch e₁ e₂ e₃ ))",
        E"Λ (σ : ⋆).λ (e₁: ContractId Mod:T) (e₂: σ) (e₃: Int64) → (( exercise @Mod:T Ch e₁ e₂ e₃ ))",
        // FecthByKey & lookupByKey
        E"""((fetch_by_key @Mod:T "Bob"))""",
        E"""((lookup_by_key @Mod:T "Bob"))""",
        // UpdFetch
        E"Λ (σ : ⋆). λ (e: σ) → (( fetch @Mod:T e ))",
        // ScenarioEmbedExpr
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : Udpate σ) → (( uembed_expr @τ e ))",
        E"Λ (τ : ⋆) (σ : ⋆). λ (e : σ) → (( uembed_expr @τ e ))",
      )

      forEvery(testCases) { exp: Expr =>
        an[ValidationError] should be thrownBy env.typeOf(exp)
      }
    }

    "reject ill formed template definition" in {

      /*
      (Mod:T8Bis { person = (Mod:T {name} this), party = (Mod:T {person} this) })
      \ (p: Party) -> Cons @Party [(Mod:T {person} this), 'Alice'] (Nil @Party)
       */

      val pkg =
        p"""

          module Mod {
            record @serializable U = {};
            record @serializable Box (a :*) = { value: a };
          }

          module NegativeTestCase {
            record @serializable T = {person: Party, name: Text};
            record @serializable TBis = {person: Text, party: Party};

            template (this : T) =  {
              precondition True,
              signatories Cons @Party ['Bob'] (Nil @Party),
              observers Cons @Party ['Alice'] (Nil @Party),
              agreement "Agreement",
              choices {
                choice Ch (i : Unit) : Unit by Cons @Party ['Alice'] (Nil @Party) to upure @Unit ()
              },
              key @NegativeTestCase:TBis
                  (NegativeTestCase:TBis { person = (NegativeTestCase:T {name} this), party = (NegativeTestCase:T {person} this) })
                  (\ (key: NegativeTestCase:TBis) -> Cons @Party [(NegativeTestCase:TBis {party} key), 'Alice'] (Nil @Party)  )
            } ;
          }

          module PositiveTestCase1 {
            record @serializable T = {};

            template (this : T) =  {
              precondition True,
              signatories (),                                 // should be of (type List Party)
              observers Cons @Party ['Alice'] (Nil @Party),
              agreement "Agreement",
              choices {
                choice Ch (i : Unit) : Unit by Cons @Party ['Alice'] (Nil @Party) to upure @Unit ()
              }
            } ;
          }

          module PositiveTestCase2 {
            record @serializable T = {};

            template (this : T) =  {
              precondition True,
              signatories Cons @Party ['Bob'] (Nil @Party),
              observers (),                                  // should be of type (List Party)
              agreement "Agreement",
              choices {
                choice Ch (i : Unit) : Unit by Cons @Party ['Alice'] (Nil @Party) to upure @Unit ()
              }
            } ;
          }

          module PositiveTestCase3 {
            record @serializable T = {};

            template (this : T) =  {
              precondition True,
              signatories Cons @Party ['Bob'] (Nil @Party),
              observers Cons @Party ['Alice'] (Nil @Party),
              agreement (),                                 // should be of type Text
              choices {
                choice Ch (i : Unit) : Unit by Cons @Party ['Alice'] (Nil @Party) to upure @Unit ()
              }
            } ;
          }

          module PositiveTestCase4 {
            record @serializable T = {};

            template (this : T) =  {
              precondition True,
              signatories Cons @Party ['Bob'] (Nil @Party),
              observers Cons @Party ['Alice'] (Nil @Party),
              agreement "Agreement",
              choices {
                choice Ch (i : List) : Unit   // the type of i (here List) should be of kind * (here it is * -> *)
                  by Cons @Party ['Alice'] (Nil @Party) to upure @Unit ()
              }
            } ;
          }

          module PositiveTestCase5 {
            record @serializable T = {};

            template (this : T) =  {
              precondition True,
              signatories Cons @Party ['Bob'] (Nil @Party),
              observers Cons @Party ['Alice'] (Nil @Party),
              agreement "Agreement",
              choices {
                choice Ch (i : Unit) : List   // the return type (here List) should be of kind * (here it is * -> *)
                   by Cons @Party ['Alice'] (Nil @Party) to upure @(List) (/\ (tau : *). Nil @tau)
              }
            } ;
          }

      module PositiveTestCase6 {
        record @serializable T = {person: Party, name: Text};
        record @serializable TBis = {person: Text, party: Party};

        template (this : T) =  {
          precondition True,
          signatories Cons @Party ['Bob'] (Nil @Party),
          observers Cons @Party ['Alice'] (Nil @Party),
          agreement "Agreement",
          choices {
            choice Ch (i : Unit) : Unit by Cons @Party ['Alice'] (Nil @Party) to upure @Unit ()
          },
          key @PositiveTestCase6:TBis
              // In the next line, should use only record construction and projection, and variable. Here use string literal.
              (PositiveTestCase6:TBis { person = "Alice", party = (PositiveTestCase6:T {person} this) })
              (\ (key: PositiveTestCase6:TBis) -> Cons @Party [(PositiveTestCase6:TBis {party} key), 'Alice'] (Nil @Party)  )
        } ;
      }


      module PositiveTestCase7 {
        record @serializable T = {person: Party, name: Text};
        record @serializable TBis = {person: Text, party: Party};

        template (this : T) =  {
          precondition True,
          signatories Cons @Party ['Bob'] (Nil @Party),
          observers Cons @Party ['Alice'] (Nil @Party),
          agreement "Agreement",
          choices {
            choice Ch (i : Unit) : Unit by Cons @Party ['Alice'] (Nil @Party) to upure @Unit ()
          },
          key @PositiveTestCase7:TBis
          // In the next line, the declared type do not match body
          (NegativeTestCase:TBis { person = (PositiveTestCase7:T {name} this), party = (PositiveTestCase7:T {person} this) })
          (\ (key: PositiveTestCase7:TBis) -> Cons @Party [(PositiveTestCase7:TBis {party} key), 'Alice'] (Nil @Party)  )
        } ;
      }


      module PositiveTestCase8 {
        record @serializable T = {person: Party, name: Text};
        record @serializable TBis = {person: Text, party: Party};

        template (this : T) =  {
          precondition True,
          signatories Cons @Party ['Bob'] (Nil @Party),
          observers Cons @Party ['Alice'] (Nil @Party),
          agreement "Agreement",
          choices {
            choice Ch (i : Unit) : Unit by Cons @Party ['Alice'] (Nil @Party) to upure @Unit ()
          },
          key @PositiveTestCase8:TBis
          (PositiveTestCase8:TBis { person = (PositiveTestCase8:T {name} this), party = (PositiveTestCase8:T {person} this) })
          // in the next line, expect PositiveTestCase8:TBis -> List Party
          (\ (key: NegativeTestCase:TBis) -> Cons @Party [(PositiveTestCase8:TBis {party} key), 'Alice'] (Nil @Party)  )
        } ;
      }

      module PositiveTestCase9 {
        record @serializable T = {person: Party, name: Text};
        record @serializable TBis = {person: Text, party: Party};

        template (this : T) =  {
          precondition True,
          signatories Cons @Party ['Bob'] (Nil @Party),
          observers Cons @Party ['Alice'] (Nil @Party),
          agreement "Agreement",
          choices {
            choice Ch (i : Unit) : Unit by Cons @Party ['Alice'] (Nil @Party) to upure @Unit ()
          },
          key @PositiveTestCase9:TBis
          (PositiveTestCase9:TBis { person = (PositiveTestCase9:T {name} this), party = (PositiveTestCase9:T {person} this) })
          // In the next line, cannot use `this`
          (\ (key: PositiveTestCase9:TBis) -> Cons @Party [(PositiveTestCase9:T {person} this), 'Alice'] (Nil @Party)  )
        } ;
      }
      """

      val world = new World(Map(defaultPkgId -> pkg))

      val typeMismatchCases = Table(
        "moduleName",
        "PositiveTestCase1",
        "PositiveTestCase2",
        "PositiveTestCase3",
        "PositiveTestCase7",
        "PositiveTestCase8"
      )

      val kindMismatchCases = Table(
        "moduleName",
        "PositiveTestCase4",
        "PositiveTestCase5",
      )

      def checkModule(modName: String) = Typing.checkModule(
        world,
        defaultPkgId,
        pkg.modules(DottedName.assertFromString(modName))
      )

      checkModule("NegativeTestCase")
      forAll(typeMismatchCases)(module => an[ETypeMismatch] shouldBe thrownBy(checkModule(module))) // and
      forAll(kindMismatchCases)(module => an[EKindMismatch] shouldBe thrownBy(checkModule(module)))
      an[EIllegalKeyExpression] shouldBe thrownBy(checkModule("PositiveTestCase6"))
      an[EUnknownExprVar] shouldBe thrownBy(checkModule("PositiveTestCase9"))
    }

  }

  "rejects choice controller expressions that use choice argument if DAML-LF < 1.2 " in {

    import com.digitalasset.daml.lf.archive.{LanguageMajorVersion => LVM, LanguageVersion => LV}

    val testCases = Table[LV, Boolean](
      "LF version" -> "reject",
      LV.defaultV0 -> true,
      LV(LVM.V1, "0") -> true,
      LV(LVM.V1, "1") -> true,
      LV(LVM.V1, "2") -> false,
    )

    val pkg0 =
      p"""
           module Mod {
             record @serializable T = { party: Party };
             template (this : T) =  {
               precondition True,
               signatories Cons @Party ['Bob'] (Nil @Party),
               observers Cons @Party ['Alice'] (Nil @Party),
               agreement "Agreement",
               choices {
                 choice Ch (record : Mod:T) : Unit by Cons @Party [(Mod:T {party} record), 'Alice'] (Nil @Party) to upure @Unit ()
               }
             } ;
           }
            """

    val modName = DottedName.assertFromString("Mod")

    forEvery(testCases) { (version: LanguageVersion, rejected: Boolean) =>
      val pkg = pkg0.updateVersion(version)
      val mod = pkg.modules(modName)
      val world = new World(Map(defaultPkgId -> pkg))

      if (rejected)
        an[EUnknownExprVar] should be thrownBy
          Typing.checkModule(world, defaultPkgId, mod)
      else
        Typing.checkModule(world, defaultPkgId, mod)

      ()
    }

  }

  "rejects choice that use same variable for template and choice params if DAML-LF < 1.2 " in {

    import com.digitalasset.daml.lf.archive.{LanguageMajorVersion => LVM, LanguageVersion => LV}

    val testCases = Table[LV, Boolean](
      "LF version" -> "reject",
      LV.defaultV0 -> true,
      LV(LVM.V1, "0") -> true,
      LV(LVM.V1, "1") -> true,
      LV(LVM.V1, "2") -> false,
    )

    val pkg0 =
      p"""
           module Mod {
             record @serializable T = { party: Party };
             template (record : T) =  {
               precondition True,
               signatories Cons @Party ['Bob'] (Nil @Party),
               observers Cons @Party ['Alice'] (Nil @Party),
               agreement "Agreement",
               choices {
                 choice Ch (record : Mod:T) : Unit by Cons @Party [(Mod:T {party} record), 'Alice'] (Nil @Party) to upure @Unit ()
               }
             } ;
           }
            """

    val modName = DottedName.assertFromString("Mod")

    forEvery(testCases) { (version: LanguageVersion, rejected: Boolean) =>
      val pkg = pkg0.updateVersion(version)
      val mod = pkg.modules(modName)
      val world = new World(Map(defaultPkgId -> pkg))

      if (rejected)
        an[EIllegalShadowingExprVar] should be thrownBy
          Typing.checkModule(world, defaultPkgId, mod)
      else
        Typing.checkModule(world, defaultPkgId, mod)
      ()
    }

  }

  private val pkg =
    p"""
       module Mod {
         record R (a: *) = {f1: Int64, f2: List a } ;

         variant Tree (a: *) =  Node : < left: Mod:Tree a, right: Mod:Tree a > | Leaf : a ;

         record T = {person: Party, name: Text };
         template (this : T) =  {
           precondition True,
           signatories Cons @Party ['Bob'] Nil @Party,
           observers Cons @Party ['Alice'] (Nil @Party),
           agreement "Agreement",
           choices {
             choice Ch (x: Int64) : Decimal by 'Bob' to upure @INT64 (DECIMAL_TO_INT64 x)
           },
           key @Party (Mod:Person {person} this) (\ (p: Party) -> Cons @Party ['Alice', p] (Nil @Party))
         } ;

          val f : Int64 -> Bool = ERROR @(Bool -> Int64) "not implemented";
       }
     """

  private val env =
    Typing.Env(LanguageVersion.default, new World(Map(defaultPkgId -> pkg)), NoContext)

}
