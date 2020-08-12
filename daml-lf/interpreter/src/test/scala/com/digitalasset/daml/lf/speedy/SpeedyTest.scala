// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util

import com.daml.lf.data.Ref._
import com.daml.lf.PureCompiledPackages
import com.daml.lf.data.{FrontStack, Ref}
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SError.SError
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult.{SResultFinalValue, SResultError}
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.validation.Validation
import org.scalactic.Equality
import org.scalatest.{Matchers, WordSpec}

class SpeedyTest extends WordSpec with Matchers {

  import SpeedyTest._
  import defaultParserParameters.{defaultPackageId => pkgId}
  def qualify(name: String) = Identifier(pkgId, QualifiedName.assertFromString(name))

  val pkgs = typeAndCompile(p"")

  "application arguments" should {
    "be handled correctly" in {
      eval(
        e"""
        (\ (a: Int64) (b: Int64) -> SUB_INT64 a b) 88 33
      """,
        pkgs
        // Test should fail if we get the order of the function arguments wrong.
      ) shouldEqual Right(SInt64(55))
    }
  }

  "stack variables" should {
    "be handled correctly" in {
      eval(
        e"""
        let a : Int64 = 88 in
        let b : Int64 = 33 in
        SUB_INT64 a b
      """,
        pkgs
        // Test should fail if we access the stack with incorrect indexing.
      ) shouldEqual Right(SInt64(55))
    }
  }

  "free variables" should {
    "be handled correctly" in {
      eval(
        e"""
        (\(a : Int64) ->
         let b : Int64 = 33 in
         (\ (x: Unit) -> SUB_INT64 a b) ()) 88
      """,
        pkgs
        // Test should fail if we index free-variables of a closure incorrectly.
      ) shouldEqual Right(SInt64(55))
    }
  }

  "pattern matching" should {

    val pkg =
      p"""
        module Matcher {
          val unit : Unit -> Int64 = \ (x: Unit) -> case x of () -> 2;
          val bool : Bool -> Int64 = \ (x: Bool) -> case x of True -> 3 | False -> 5;
          val list : forall (a: *). List a -> Option <x1: a, x2: List a> = /\ (a: *).
            \(x: List a) -> case x of
              Nil      -> None @(<x1: a, x2: List a>)
           |  Cons h t -> Some @(<x1: a, x2: List a>) <x1 = h, x2 = t>;
          val option: forall (a: *). a -> Option a-> a = /\ (a: *). \(x: a) -> \(y: Option a) -> case y of
              None -> x
           |  Some z -> z;
          val variant: forall (a: *). Mod:Tree a -> a = /\ (a: *). \(x: Mod:Tree a) -> case x of
              Mod:Tree:Leaf y -> y
           |  Mod:Tree:Node node -> Matcher:variant @a (Mod:Tree.Node @a {left} node);
          val enum: Mod:Color -> Int64 = \(x: Mod:Color) -> case x of
              Mod:Color:Red -> 37
           |  Mod:Color:Green -> 41
           |  Mod:Color:Blue -> 43;
        }

        module Mod {
          variant Tree (a: *) =  Node : Mod:Tree.Node a | Leaf : a ;
          record Tree.Node (a: *) = {left: Mod:Tree a, right: Mod:Tree a } ;
          enum Color = Red | Green | Blue ;
       }


      """

    val pkgs = typeAndCompile(pkg)

    "works as expected on primitive constructors" in {

      eval(e"Matcher:unit ()", pkgs) shouldEqual Right(SInt64(2))
      eval(e"Matcher:bool True", pkgs) shouldEqual Right(SInt64(3))
      eval(e"Matcher:bool False", pkgs) shouldEqual Right(SInt64(5))

    }

    "works as expected on lists" in {
      eval(e"Matcher:list @Int64 ${intList()}", pkgs) shouldEqual Right(SOptional(None))
      eval(e"Matcher:list @Int64 ${intList(7, 11, 13)}", pkgs) shouldEqual Right(
        SOptional(
          Some(
            SStruct(
              Ref.Name.Array(n"x1", n"x2"),
              ArrayList(SInt64(7), SList(FrontStack(SInt64(11), SInt64(13)))),
            ),
          ),
        ),
      )
    }

    "works as expected on Optionals" in {
      eval(e"Matcher:option @Int64 17 (None @Int64)", pkgs) shouldEqual Right(SInt64(17))
      eval(e"Matcher:option @Int64 17 (Some @Int64 19)", pkgs) shouldEqual Right(SInt64(19))
    }

    "works as expected on Variants" in {
      eval(e"""Matcher:variant @Int64 (Mod:Tree:Leaf @Int64 23)""", pkgs) shouldEqual Right(
        SInt64(23),
      )
      eval(
        e"""Matcher:variant @Int64 (Mod:Tree:Node @Int64 (Mod:Tree.Node @Int64 {left = Mod:Tree:Leaf @Int64 27, right = Mod:Tree:Leaf @Int64 29 }))""",
        pkgs,
      ) shouldEqual Right(SInt64(27))
    }

    "works as expected on Enums" in {
      eval(e"""Matcher:enum Mod:Color:Red""", pkgs) shouldEqual Right(SInt64(37))
      eval(e"""Matcher:enum Mod:Color:Green""", pkgs) shouldEqual Right(SInt64(41))
      eval(e"""Matcher:enum Mod:Color:Blue""", pkgs) shouldEqual Right(SInt64(43))
    }

  }

  val anyPkg =
    p"""
      module Test {
        record @serializable T1 = { party: Party } ;
        template (record : T1) = {
          precondition True,
          signatories Cons @Party [(Test:T1 {party} record)] (Nil @Party),
          observers Nil @Party,
          agreement "Agreement",
          choices {
          }
        } ;
        record @serializable T2 = { party: Party } ;
        template (record : T2) = {
          precondition True,
          signatories Cons @Party [(Test:T2 {party} record)] (Nil @Party),
          observers Nil @Party,
          agreement "Agreement",
          choices {
          }
        } ;
        record T3 (a: *) = { party: Party } ;
     }
    """

  val anyPkgs = typeAndCompile(anyPkg)

  "to_any" should {

    "succeed on Int64" in {
      eval(e"""to_any @Int64 1""", anyPkgs) shouldEqual Right(SAny(TBuiltin(BTInt64), SInt64(1)))
    }
    "succeed on record type without parameters" in {
      eval(e"""to_any @Test:T1 (Test:T1 {party = 'Alice'})""", anyPkgs) shouldEqual
        Right(
          SAny(
            Ast.TTyCon(Identifier(pkgId, QualifiedName.assertFromString("Test:T1"))),
            SRecord(
              Identifier(pkgId, QualifiedName.assertFromString("Test:T1")),
              Name.Array(Name.assertFromString("party")),
              ArrayList(SParty(Party.assertFromString("Alice"))),
            ),
          ),
        )
    }
    "succeed on record type with parameters" in {
      eval(e"""to_any @(Test:T3 Int64) (Test:T3 @Int64 {party = 'Alice'})""", anyPkgs) shouldEqual
        Right(
          SAny(
            TApp(
              TTyCon(Identifier(pkgId, QualifiedName.assertFromString("Test:T3"))),
              TBuiltin(BTInt64),
            ),
            SRecord(
              Identifier(pkgId, QualifiedName.assertFromString("Test:T3")),
              Name.Array(Name.assertFromString("party")),
              ArrayList(SParty(Party.assertFromString("Alice"))),
            ),
          ),
        )
      eval(e"""to_any @(Test:T3 Text) (Test:T3 @Text {party = 'Alice'})""", anyPkgs) shouldEqual
        Right(
          SAny(
            TApp(
              TTyCon(Identifier(pkgId, QualifiedName.assertFromString("Test:T3"))),
              TBuiltin(BTText),
            ),
            SRecord(
              Identifier(pkgId, QualifiedName.assertFromString("Test:T3")),
              Name.Array(Name.assertFromString("party")),
              ArrayList(SParty(Party.assertFromString("Alice"))),
            ),
          ),
        )
    }
  }

  "from_any" should {

    "throw an exception on Int64" in {
      eval(e"""from_any @Test:T1 1""", anyPkgs) shouldBe 'left
    }

    "return Some(tpl) if template type matches" in {
      eval(e"""from_any @Test:T1 (to_any @Test:T1 (Test:T1 {party = 'Alice'}))""", anyPkgs) shouldEqual
        Right(
          SOptional(
            Some(
              SRecord(
                Identifier(pkgId, QualifiedName.assertFromString("Test:T1")),
                Name.Array(Name.assertFromString("party")),
                ArrayList(SParty(Party.assertFromString("Alice"))),
              ),
            ),
          ),
        )
    }

    "return None if template type does not match" in {
      eval(e"""from_any @Test:T2 (to_any @Test:T1 (Test:T1 {party = 'Alice'}))""", anyPkgs) shouldEqual Right(
        SOptional(None),
      )
    }
    "return Some(v) if type parameter is the same" in {
      eval(
        e"""from_any @(Test:T3 Int64) (to_any @(Test:T3 Int64) (Test:T3 @Int64 {party = 'Alice'}))""",
        anyPkgs,
      ) shouldEqual Right(
        SOptional(
          Some(
            SRecord(
              Identifier(pkgId, QualifiedName.assertFromString("Test:T3")),
              Name.Array(Name.assertFromString("party")),
              ArrayList(SParty(Party.assertFromString("Alice"))),
            ),
          ),
        ),
      )
    }
    "return None if type parameter is different" in {
      eval(
        e"""from_any @(Test:T3 Int64) (to_any @(Test:T3 Text) (Test:T3 @Int64 {party = 'Alice'}))""",
        anyPkgs,
      ) shouldEqual Right(SOptional(None))
    }
  }

  "type_rep" should {

    "produces expected output" in {
      eval(e"""type_rep @Test:T1""", anyPkgs) shouldEqual Right(STypeRep(t"Test:T1"))
      eval(e"""type_rep @Test2:T2""", anyPkgs) shouldEqual Right(STypeRep(t"Test2:T2"))
      eval(e"""type_rep @(Mod:Tree (List Text))""", anyPkgs) shouldEqual Right(
        STypeRep(t"Mod:Tree (List Text)"),
      )
      eval(e"""type_rep @((ContractId Mod:T) -> Mod:Color)""", anyPkgs) shouldEqual Right(
        STypeRep(t"(ContractId Mod:T) -> Mod:Color"),
      )
    }

  }

  val recUpdPkgs = typeAndCompile(p"""
    module M {
      record Point = { x: Int64, y: Int64 } ;
      val origin: M:Point = M:Point { x = 0, y = 0 } ;
      val p_1_0: M:Point = M:Point { M:origin with x = 1 } ;
      val p_1_2: M:Point = M:Point { M:Point { M:origin with x = 1 } with y = 2 } ;
      val p_3_4_loc: M:Point = loc(M,p_3_4_loc,0,0,0,0) M:Point {
        loc(M,p_3_4_loc,1,1,1,1) M:Point {
          loc(M,p_3_4_loc,2,2,2,2) M:origin with x = 3
        } with y = 4
      } ;
    }
  """)
  "record update" should {
    "use SBRecUpd for single update" in {
      val p_1_0 = recUpdPkgs.getDefinition(LfDefRef(qualify("M:p_1_0")))
      p_1_0 shouldEqual
        Some(
          SELet1General(
            SEVal(LfDefRef(qualify("M:origin"))),
            SEAppAtomicSaturatedBuiltin(
              SBRecUpd(qualify("M:Point"), 0),
              Array(SELocS(1), SEValue(SInt64(1))))))

    }

    "produce expected output for single update" in {
      eval(e"M:p_1_0", recUpdPkgs) shouldEqual
        Right(
          SRecord(
            qualify("M:Point"),
            Name.Array(n"x", n"y"),
            ArrayList(SInt64(1), SInt64(0))
          )
        )
    }

    "use SBRecUpdMulti for multi update" in {
      val p_1_2 = recUpdPkgs.getDefinition(LfDefRef(qualify("M:p_1_2")))
      p_1_2 shouldEqual
        Some(
          SELet1General(
            SEVal(LfDefRef(qualify("M:origin"))),
            SEAppAtomicSaturatedBuiltin(
              SBRecUpdMulti(qualify("M:Point"), Array(0, 1)),
              Array(
                SELocS(1),
                SEValue(SInt64(1)),
                SEValue(SInt64(2)),
              ),
            )
          )
        )
    }

    "produce expected output for multi update" in {
      eval(e"M:p_1_2", recUpdPkgs) shouldEqual
        Right(
          SRecord(
            qualify("M:Point"),
            Name.Array(n"x", n"y"),
            ArrayList(SInt64(1), SInt64(2)),
          )
        )
    }

    "use SBRecUpdMulti for multi update with location annotations" in {
      def mkLocation(n: Int) =
        Location(
          pkgId,
          ModuleName.assertFromString("M"),
          "p_3_4_loc",
          (n, n),
          (n, n),
        )
      val p_3_4 = recUpdPkgs.getDefinition(LfDefRef(qualify("M:p_3_4_loc")))
      p_3_4 shouldEqual
        Some(
          SELet1General(
            SELocation(mkLocation(2), SEVal(LfDefRef(qualify("M:origin")))),
            SELocation(
              mkLocation(0),
              SEAppAtomicSaturatedBuiltin(
                SBRecUpdMulti(qualify("M:Point"), Array(0, 1)),
                Array(
                  SELocS(1),
                  SEValue(SInt64(3)),
                  SEValue(SInt64(4)),
                ),
              )),
          )
        )
    }

    "produce expected output for multi update with location annotations" in {
      eval(e"M:p_3_4_loc", recUpdPkgs) shouldEqual
        Right(
          SRecord(
            qualify("M:Point"),
            Name.Array(n"x", n"y"),
            ArrayList(SInt64(3), SInt64(4)),
          )
        )
    }
  }

  "profiler" should {
    "evaluate arguments before open event" in {
      val events = profile(e"""
        let f: Int64 -> Int64 = \(x: Int64) -> ADD_INT64 x 1 in
        let g: Int64 -> Int64 = \(x: Int64) -> ADD_INT64 x 2 in
        f (g 1)
      """)
      events should have size 4
      events.get(0) should matchPattern { case Profile.Event(true, "g", _) => }
      events.get(1) should matchPattern { case Profile.Event(false, "g", _) => }
      events.get(2) should matchPattern { case Profile.Event(true, "f", _) => }
      events.get(3) should matchPattern { case Profile.Event(false, "f", _) => }
    }
  }
}

object SpeedyTest {

  private def eval(e: Expr, packages: PureCompiledPackages): Either[SError, SValue] = {
    val machine = Speedy.Machine.fromPureExpr(packages, e)
    final case class Goodbye(e: SError) extends RuntimeException("", null, false, false)
    try {
      val value = machine.run() match {
        case SResultFinalValue(v) => v
        case SResultError(err) => throw Goodbye(err)
        case res => throw new RuntimeException(s"Got unexpected interpretation result $res")
      }
      Right(value)
    } catch {
      case Goodbye(err) => Left(err)
    }
  }

  private def profile(e: Expr): java.util.ArrayList[Profile.Event] = {
    val packages = PureCompiledPackages(Map.empty, profiling = Compiler.FullProfile).right.get
    val machine = Speedy.Machine.fromPureExpr(packages, e)
    machine.run()
    machine.profile.events
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private implicit def resultEq: Equality[Either[SError, SValue]] = {
    case (Right(v1: SValue), Right(v2: SValue)) => svalue.Equality.areEqual(v1, v2)
    case (Left(e1), Left(e2)) => e1 == e2
    case _ => false
  }

  private def typeAndCompile(pkg: Ast.Package): PureCompiledPackages = {
    val rawPkgs = Map(defaultParserParameters.defaultPackageId -> pkg)
    Validation.checkPackage(rawPkgs, defaultParserParameters.defaultPackageId)
    PureCompiledPackages(rawPkgs).right.get
  }

  private def intList(xs: Long*): String =
    if (xs.isEmpty) "(Nil @Int64)"
    else xs.mkString(s"(Cons @Int64 [", ", ", s"] (Nil @Int64))")

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }
}
