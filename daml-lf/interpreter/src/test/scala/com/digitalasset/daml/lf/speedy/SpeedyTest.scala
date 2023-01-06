// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Struct}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SError.{SError, SErrorDamlException}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SpeedyTestLib.typeAndCompile
import com.daml.lf.testing.parser.Implicits._
import org.scalactic.Equality
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import defaultParserParameters.{defaultPackageId => pkgId}
import SpeedyTestLib.loggingContext
import com.daml.lf.speedy.Speedy.CachedContract
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.ContractId
import com.daml.logging.ContextualizedLogger
import org.scalatest.Inside

import scala.util.{Failure, Success, Try}

class SpeedyTest extends AnyWordSpec with Matchers with Inside {

  import SpeedyTest._

  import Speedy.Machine.{runPureExpr => eval}

  "application arguments" should {
    "be handled correctly" in {
      eval(
        pkgs,
        e"""
        (\ (a: Int64) (b: Int64) -> SUB_INT64 a b) 88 33
      """,
      ) shouldEqual Right(SInt64(55))
    }
  }

  "stack variables" should {
    "be handled correctly" in {
      eval(
        pkgs,
        e"""
        let a : Int64 = 88 in
        let b : Int64 = 33 in
        SUB_INT64 a b
      """,
      ) shouldEqual Right(SInt64(55))
    }
  }

  "free variables" should {
    "be handled correctly" in {
      eval(
        pkgs,
        e"""
        (\(a : Int64) ->
         let b : Int64 = 33 in
         (\ (x: Unit) -> SUB_INT64 a b) ()) 88
      """,
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
      eval(pkgs, e"Matcher:unit ()") shouldEqual Right(SInt64(2))
      eval(pkgs, e"Matcher:bool True") shouldEqual Right(SInt64(3))
      eval(pkgs, e"Matcher:bool False") shouldEqual Right(SInt64(5))
    }

    "works as expected on lists" in {
      eval(pkgs, e"Matcher:list @Int64 ${intList()}") shouldEqual Right(SOptional(None))
      eval(pkgs, e"Matcher:list @Int64 ${intList(7, 11, 13)}") shouldEqual Right(
        SOptional(
          Some(
            SStruct(
              Struct.assertFromNameSeq(List(n"x1", n"x2")),
              ArrayList(SInt64(7), SList(FrontStack(SInt64(11), SInt64(13)))),
            )
          )
        )
      )
    }

    "works as expected on Optionals" in {
      eval(pkgs, e"Matcher:option @Int64 17 (None @Int64)") shouldEqual Right(SInt64(17))
      eval(pkgs, e"Matcher:option @Int64 17 (Some @Int64 19)") shouldEqual Right(SInt64(19))
    }

    "works as expected on Variants" in {
      eval(pkgs, e"""Matcher:variant @Int64 (Mod:Tree:Leaf @Int64 23)""") shouldEqual Right(
        SInt64(23)
      )
      eval(
        pkgs,
        e"""Matcher:variant @Int64 (Mod:Tree:Node @Int64 (Mod:Tree.Node @Int64 {left = Mod:Tree:Leaf @Int64 27, right = Mod:Tree:Leaf @Int64 29 }))""",
      ) shouldEqual Right(SInt64(27))
    }

    "works as expected on Enums" in {
      eval(pkgs, e"""Matcher:enum Mod:Color:Red""") shouldEqual Right(SInt64(37))
      eval(pkgs, e"""Matcher:enum Mod:Color:Green""") shouldEqual Right(SInt64(41))
      eval(pkgs, e"""Matcher:enum Mod:Color:Blue""") shouldEqual Right(SInt64(43))
    }
  }

  "to_any" should {
    "succeed on Int64" in {
      eval(anyPkgs, e"""to_any @Int64 1""") shouldEqual Right(SAny(TBuiltin(BTInt64), SInt64(1)))
    }

    "succeed on record type without parameters" in {
      evalApp(
        anyPkgs,
        e"""\ (p: Party) -> to_any @Test:T1 (Test:T1 {party = p})""",
        Array(alice),
      ) shouldEqual
        Right(
          SAny(
            TTyCon(Identifier(pkgId, QualifiedName.assertFromString("Test:T1"))),
            SRecord(
              Identifier(pkgId, QualifiedName.assertFromString("Test:T1")),
              ImmArray(Name.assertFromString("party")),
              ArrayList(SParty(Party.assertFromString("Alice"))),
            ),
          )
        )
    }

    "succeed on record type with parameters" in {
      evalApp(
        anyPkgs,
        e"""\ (p : Party) -> to_any @(Test:T3 Int64) (Test:T3 @Int64 {party = p})""",
        Array(alice),
      ) shouldEqual
        Right(
          SAny(
            TApp(
              TTyCon(Identifier(pkgId, QualifiedName.assertFromString("Test:T3"))),
              TBuiltin(BTInt64),
            ),
            SRecord(
              Identifier(pkgId, QualifiedName.assertFromString("Test:T3")),
              ImmArray(Name.assertFromString("party")),
              ArrayList(SParty(Party.assertFromString("Alice"))),
            ),
          )
        )
      evalApp(
        anyPkgs,
        e"""\ (p : Party) -> to_any @(Test:T3 Text) (Test:T3 @Text {party = p})""",
        Array(alice),
      ) shouldEqual
        Right(
          SAny(
            TApp(
              TTyCon(Identifier(pkgId, QualifiedName.assertFromString("Test:T3"))),
              TBuiltin(BTText),
            ),
            SRecord(
              Identifier(pkgId, QualifiedName.assertFromString("Test:T3")),
              ImmArray(Name.assertFromString("party")),
              ArrayList(SParty(Party.assertFromString("Alice"))),
            ),
          )
        )
    }
  }

  "from_any" should {
    "throw an exception on Int64" in {
      eval(anyPkgs, e"""from_any @Test:T1 1""") shouldBe a[Left[_, _]]
    }

    "return Some(tpl) if template type matches" in {
      evalApp(
        anyPkgs,
        e"""\(p : Party) -> from_any @Test:T1 (to_any @Test:T1 (Test:T1 {party = p}))""",
        Array(alice),
      ) shouldEqual
        Right(
          SOptional(
            Some(
              SRecord(
                Identifier(pkgId, QualifiedName.assertFromString("Test:T1")),
                ImmArray(Name.assertFromString("party")),
                ArrayList(SParty(Party.assertFromString("Alice"))),
              )
            )
          )
        )
    }

    "return None if template type does not match" in {
      evalApp(
        anyPkgs,
        e"""\(p : Party) -> from_any @Test:T2 (to_any @Test:T1 (Test:T1 {party = p}))""",
        Array(alice),
      ) shouldEqual Right(
        SOptional(None)
      )
    }

    "return Some(v) if type parameter is the same" in {
      evalApp(
        anyPkgs,
        e"""\(p : Alice) -> from_any @(Test:T3 Int64) (to_any @(Test:T3 Int64) (Test:T3 @Int64 {party = p}))""",
        Array(alice),
      ) shouldEqual Right(
        SOptional(
          Some(
            SRecord(
              Identifier(pkgId, QualifiedName.assertFromString("Test:T3")),
              ImmArray(Name.assertFromString("party")),
              ArrayList(SParty(Party.assertFromString("Alice"))),
            )
          )
        )
      )
    }

    "return None if type parameter is different" in {
      evalApp(
        anyPkgs,
        e"""\ (p : Party) -> from_any @(Test:T3 Int64) (to_any @(Test:T3 Text) (Test:T3 @Int64 {party = p}))""",
        Array(alice),
      ) shouldEqual Right(SOptional(None))
    }
  }

  "type_rep" should {
    "produces expected output" in {
      eval(anyPkgs, e"""type_rep @Test:T1""") shouldEqual Right(STypeRep(t"Test:T1"))
      eval(anyPkgs, e"""type_rep @Test2:T2""") shouldEqual Right(STypeRep(t"Test2:T2"))
      eval(anyPkgs, e"""type_rep @(Mod:Tree (List Text))""") shouldEqual Right(
        STypeRep(t"Mod:Tree (List Text)")
      )
      eval(anyPkgs, e"""type_rep @((ContractId Mod:T) -> Mod:Color)""") shouldEqual Right(
        STypeRep(t"(ContractId Mod:T) -> Mod:Color")
      )
    }
  }

  "record update" should {
    "use SBRecUpd for single update" in {
      val p_1_0 = recUpdPkgs.getDefinition(LfDefRef(qualify("M:p_1_0")))
      p_1_0 shouldEqual
        Some(
          SDefinition(
            SELet1General(
              SEVal(LfDefRef(qualify("M:origin"))),
              SEAppAtomicSaturatedBuiltin(
                SBRecUpd(qualify("M:Point"), 0),
                Array(SELocS(1), SEValue(SInt64(1))),
              ),
            )
          )
        )

    }

    "produce expected output for single update" in {
      eval(recUpdPkgs, e"M:p_1_0") shouldEqual
        Right(
          SRecord(
            qualify("M:Point"),
            ImmArray(n"x", n"y"),
            ArrayList(SInt64(1), SInt64(0)),
          )
        )
    }

    "use SBRecUpdMulti for multi update" in {
      val p_1_2 = recUpdPkgs.getDefinition(LfDefRef(qualify("M:p_1_2")))
      p_1_2 shouldEqual
        Some(
          SDefinition(
            SELet1General(
              SEVal(LfDefRef(qualify("M:origin"))),
              SEAppAtomicSaturatedBuiltin(
                SBRecUpdMulti(qualify("M:Point"), ImmArray(0, 1)),
                Array(
                  SELocS(1),
                  SEValue(SInt64(1)),
                  SEValue(SInt64(2)),
                ),
              ),
            )
          )
        )
    }

    "produce expected output for multi update" in {
      eval(recUpdPkgs, e"M:p_1_2") shouldEqual
        Right(
          SRecord(
            qualify("M:Point"),
            ImmArray(n"x", n"y"),
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
          SDefinition(
            SELet1General(
              SELocation(mkLocation(2), SEVal(LfDefRef(qualify("M:origin")))),
              SELocation(
                mkLocation(0),
                SEAppAtomicSaturatedBuiltin(
                  SBRecUpdMulti(qualify("M:Point"), ImmArray(0, 1)),
                  Array(
                    SELocS(1),
                    SEValue(SInt64(3)),
                    SEValue(SInt64(4)),
                  ),
                ),
              ),
            )
          )
        )
    }

    "produce expected output for multi update with location annotations" in {
      eval(recUpdPkgs, e"M:p_3_4_loc") shouldEqual
        Right(
          SRecord(
            qualify("M:Point"),
            ImmArray(n"x", n"y"),
            ArrayList(SInt64(3), SInt64(4)),
          )
        )
    }

    "use SBRecUpdMulti for non-atomic multi update" in {
      recUpdPkgs.getDefinition(LfDefRef(qualify("M:p_6_8"))) shouldEqual
        Some(
          SDefinition(
            SELet1General(
              SEVal(LfDefRef(qualify("M:origin"))),
              SELet1General(
                SEVal(LfDefRef(qualify("M:f"))),
                SELet1General(
                  SEAppAtomicGeneral(SELocS(1), Array(SEValue(SInt64(3)))),
                  SELet1General(
                    SEVal(LfDefRef(qualify("M:f"))),
                    SELet1General(
                      SEAppAtomicGeneral(SELocS(1), Array(SEValue(SInt64(4)))),
                      SEAppAtomicSaturatedBuiltin(
                        SBRecUpdMulti(qualify("M:Point"), ImmArray(0, 1)),
                        Array(
                          SELocS(5),
                          SELocS(3),
                          SELocS(1),
                        ),
                      ),
                    ),
                  ),
                ),
              ),
            )
          )
        )
    }

    "produce expected output for non-atomic multi update" in {
      eval(recUpdPkgs, e"M:p_6_8") shouldEqual
        Right(
          SRecord(
            qualify("M:Point"),
            ImmArray(n"x", n"y"),
            ArrayList(SInt64(6), SInt64(8)),
          )
        )
    }

    "use SBRecUpdMulti for overwriting multi update" in {
      recUpdPkgs.getDefinition(LfDefRef(qualify("M:p_3_2"))) shouldEqual
        Some(
          SDefinition(
            SELet1General(
              SEVal(LfDefRef(qualify("M:origin"))),
              SEAppAtomicSaturatedBuiltin(
                SBRecUpdMulti(qualify("M:Point"), ImmArray(0, 1, 0)),
                Array(
                  SELocS(1),
                  SEValue(SInt64(1)),
                  SEValue(SInt64(2)),
                  SEValue(SInt64(3)),
                ),
              ),
            )
          )
        )
    }

    "produce expected output for overwriting multi update" in {
      eval(recUpdPkgs, e"M:p_3_2") shouldEqual
        Right(
          SRecord(
            qualify("M:Point"),
            ImmArray(n"x", n"y"),
            ArrayList(SInt64(3), SInt64(2)),
          )
        )
    }
  }

  "checkContractVisibility" should {

    "warn about non-visible local contracts" in new VisibilityChecking {
      machine.checkContractVisibility(localContractId, localCachedContract)

      testLogger.iterator.size shouldBe 1
    }

    "accept non-visible disclosed contracts" in new VisibilityChecking {
      machine.checkContractVisibility(disclosedContractId, disclosedCachedContract)

      testLogger.iterator.size shouldBe 0
    }

    "warn about non-visible global contracts" in new VisibilityChecking {
      machine.checkContractVisibility(globalContractId, globalCachedContract)

      testLogger.iterator.size shouldBe 1
    }
  }

  "checkKeyVisibility" should {
    val handleKeyFound = (contractId: ContractId) => Speedy.Control.Value(SContractId(contractId))

    "reject non-visible local contract keys" in new VisibilityChecking {
      val result = Try {
        machine.checkKeyVisibility(localContractKey, localContractId, handleKeyFound)
      }

      inside(result) {
        case Failure(
              SErrorDamlException(
                interpretation.Error.ContractKeyNotVisible(
                  `localContractId`,
                  `localContractKey`,
                  _,
                  _,
                  _,
                )
              )
            ) =>
          succeed
      }
    }

    "accept non-visible disclosed contract keys" in new VisibilityChecking {
      val result = Try {
        machine.checkKeyVisibility(
          disclosedContractKey,
          disclosedContractId,
          handleKeyFound,
        )
      }

      inside(result) { case Success(Speedy.Control.Value(SContractId(`disclosedContractId`))) =>
        succeed
      }
    }

    "reject non-visible global contract keys" in new VisibilityChecking {
      val result = Try {
        machine.checkKeyVisibility(globalContractKey, globalContractId, handleKeyFound)
      }

      inside(result) {
        case Failure(
              SErrorDamlException(
                interpretation.Error.ContractKeyNotVisible(
                  `globalContractId`,
                  `globalContractKey`,
                  _,
                  _,
                  _,
                )
              )
            ) =>
          succeed
      }
    }
  }
}

object SpeedyTest {

  val anyPkg =
    p"""
      module Test {
        record @serializable T1 = { party: Party };
        template (record : T1) = {
          precondition True;
          signatories Cons @Party [(Test:T1 {party} record)] (Nil @Party);
          observers Nil @Party;
          agreement "Agreement";
        };

        record @serializable T2 = { party: Party };
        template (record : T2) = {
          precondition True;
          signatories Cons @Party [(Test:T2 {party} record)] (Nil @Party);
          observers Nil @Party;
          agreement "Agreement";
        };

        record T3 (a: *) = { party: Party };
     }
    """
  val anyPkgs: PureCompiledPackages = typeAndCompile(anyPkg)
  val pkgs: PureCompiledPackages = typeAndCompile(p"")
  val recUpdPkgs: PureCompiledPackages = typeAndCompile(p"""
    module M {
      record Point = { x: Int64, y: Int64 } ;
      val f: Int64 -> Int64 = \(x: Int64) -> MUL_INT64 2 x ;
      val origin: M:Point = M:Point { x = 0, y = 0 } ;
      val p_1_0: M:Point = M:Point { M:origin with x = 1 } ;
      val p_1_2: M:Point = M:Point { M:Point { M:origin with x = 1 } with y = 2 } ;
      val p_3_4_loc: M:Point = loc(M,p_3_4_loc,0,0,0,0) M:Point {
        loc(M,p_3_4_loc,1,1,1,1) M:Point {
          loc(M,p_3_4_loc,2,2,2,2) M:origin with x = 3
        } with y = 4
      } ;
      val p_6_8: M:Point = M:Point { M:Point { M:origin with x = M:f 3 } with y = M:f 4 } ;
      val p_3_2: M:Point = M:Point { M:Point { M:Point { M:origin with x = 1 } with y = 2 } with x = 3 } ;
    }
  """)
  val alice: SParty = SParty(Party.assertFromString("Alice"))

  def qualify(name: String): Ref.ValueRef =
    Identifier(pkgId, QualifiedName.assertFromString(name))

  private def evalApp(packages: PureCompiledPackages, e: Expr, args: Array[SValue]) = {
    val se = packages.compiler.unsafeCompile(e)
    Speedy.Machine.runPureSExpr(packages, SEApp(se, args))
  }

  def intList(xs: Long*): String =
    if (xs.isEmpty) "(Nil @Int64)"
    else xs.mkString(s"(Cons @Int64 [", ", ", s"] (Nil @Int64))")

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  implicit def resultEq: Equality[Either[SError, SValue]] = {
    case (Right(v1: SValue), Right(v2: SValue)) => svalue.Equality.areEqual(v1, v2)
    case (Left(e1), Left(e2)) => e1 == e2
    case _ => false
  }

  trait VisibilityChecking {
    import ExplicitDisclosureLib._
    import SpeedyTestLib.Implicits._

    val alice: IdString.Party = Ref.Party.assertFromString("alice")
    val localContractId: ContractId =
      ContractId.V1(crypto.Hash.hashPrivateKey("test-local-contract-id"))
    val localContractKey: GlobalKey = buildContractKey(alice, "local-label")
    val localCachedContract: CachedContract =
      buildHouseCachedContract(alice, alice, label = "local-label")
    val globalContractId: ContractId =
      ContractId.V1(crypto.Hash.hashPrivateKey("test-global-contract-id"))
    val globalContractKey: GlobalKey = buildContractKey(alice, "global-label")
    val globalCachedContract: CachedContract =
      buildHouseCachedContract(alice, alice, label = "global-label")
    val disclosedContractId: ContractId =
      ContractId.V1(crypto.Hash.hashPrivateKey("test-disclosed-contract-id"))
    val disclosedContract: DisclosedContract =
      buildDisclosedHouseContract(disclosedContractId, alice, alice, label = "disclosed-label")
    val disclosedContractKey: GlobalKey = buildContractKey(alice, "disclosed-label")
    val disclosedCachedContract: CachedContract =
      buildHouseCachedContract(alice, alice, label = "disclosed-label")
    val testLogger: WarningLog = new WarningLog(ContextualizedLogger.createFor("daml.warnings"))
    val machine: Speedy.UpdateMachine = Speedy.Machine
      .fromUpdateSExpr(
        pkg,
        crypto.Hash.hashPrivateKey("VisibilityChecking"),
        SEValue(SUnit),
        // As committers is empty, our readers will be empty and so contracts and contract keys will *always* be non-visible to stakeholders
        committers = Set.empty,
        disclosedContracts = ImmArray(disclosedContract),
      )
      .withWarningLog(testLogger)
      .withCachedContracts(
        localContractId -> localCachedContract,
        globalContractId -> globalCachedContract,
        disclosedContractId -> disclosedCachedContract,
      )
      .withLocalContractKey(localContractId, localContractKey)
      .withDisclosedContractKeys(
        houseTemplateType,
        disclosedContractKey.hash -> disclosedContractId,
      )
  }
}
