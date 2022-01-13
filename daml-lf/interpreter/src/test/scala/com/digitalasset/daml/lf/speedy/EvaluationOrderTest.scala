// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Location, Party}
import com.daml.lf.interpretation.Error._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.value.Value.ContractId
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec
import scala.collection.mutable.ArrayBuffer

class TestTraceLog extends TraceLog {
  private val messages: ArrayBuffer[(String, Option[Location])] = new ArrayBuffer()

  override def add(message: String, optLocation: Option[Location]) = {
    messages += ((message, optLocation))
  }

  override def iterator = messages.iterator

  def getMessages: Seq[String] = messages.view.map(_._1).toSeq
}

class EvaluationOrderTest extends AnyFreeSpec with Matchers with Inside {

  private val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(p"""
    module M {
      record @serializable TKey = { maintainers : List Party, optCid : Option (ContractId Unit), nested: M:Nested };

      record @serializable Nested = { f : Option M:Nested };

      val buildNested : Int64 -> M:Nested = \(i: Int64) ->
        case (EQUAL @Int64 i 0) of
          True -> M:Nested { f = None @M:Nested }
          | _ -> M:Nested { f = Some @M:Nested (M:buildNested (SUB_INT64 i 1)) };

      val toKey : Party -> M:TKey = \(p : Party) ->
         M:TKey { maintainers = Cons @Party [p] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 0 };
      val keyNoMaintainers : M:TKey = M:TKey { maintainers = Nil @Party, optCid = None @(ContractId Unit), nested = M:buildNested 0 };
      val toKeyWithCid : Party -> ContractId Unit -> M:TKey = \(p : Party) (cid : ContractId Unit) -> M:TKey { maintainers = Cons @Party [p] (Nil @Party), optCid = Some @(ContractId Unit) cid, nested = M:buildNested 0 };


      record @serializable T = { signatory : Party, observer : Party, precondition : Bool, key: M:TKey, nested: M:Nested };
      template (this : T) = {
        precondition TRACE @Bool "precondition" (M:T {precondition} this);
        signatories TRACE @(List Party) "signatories" (Cons @Party [M:T {signatory} this] (Nil @Party));
        observers TRACE @(List Party) "observers" (Cons @Party [M:T {observer} this] (Nil @Party));
        agreement TRACE @Text "agreement" "";
        key @M:TKey
           (TRACE @M:TKey "key" (M:T {key} this))
           (\(key : M:TKey) -> TRACE @(List Party) "maintainers" (M:TKey {maintainers} key));
      };
    }
  """)

  private val seed = crypto.Hash.hashPrivateKey("seed")

  private val allTraces = Seq(
    "precondition",
    "agreement",
    "signatories",
    "observers",
    "key",
    "maintainers",
  )

  private def evalUpdateApp(
      pkgs: CompiledPackages,
      e: Expr,
      args: Array[SValue],
      party: Party,
  ): (SResult, Seq[String]) = {
    val se = pkgs.compiler.unsafeCompile(e)
    val traceLog = new TestTraceLog()
    val res = Speedy.Machine
      .fromUpdateSExpr(pkgs, seed, SEApp(se, args.map(SEValue(_))), Set(party), traceLog = traceLog)
      .run()
    val msgs = traceLog.getMessages
    (res, msgs)
  }

  private val alice = Ref.Party.assertFromString("alice")
  private val bob = Ref.Party.assertFromString("bob")

  // We cover all errors for each node in the order they are defined
  // in com.daml.lf.interpretation.Error.
  // We don’t check for exceptions/aborts during evaluation of an expression instead
  // assume that those always stop at the point of the corresponding
  // trace statement.
  // The important cases to test are ones that result in either a different transaction
  // or a transaction that is rejected vs one that is accepted. Cases where the transaction
  // is rejected in both cases “only” change the error message which is relatively harmless.
  // Specifically this means that we need to test ordering of catchable errors
  // relative to other catchable errors and other non-catchable errors but we don’t
  // need to check ordering of non-catchable errors relative to other non-cachable errors.

  "evaluation order" - {
    "create node" - {
      // TEST_EVIDENCE: Semantics: Evaluation order of successful create
      "successful create" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> create @M:T
              M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        res shouldBe a[SResultFinalValue]
        msgs shouldBe allTraces
      }
      // TEST_EVIDENCE: Semantics: Evaluation order of create with failed precondition
      "failed precondition" in {
        // Note that for LF >= 1.14 we don’t hit this as the compiler
        // generates code that throws an exception instead of returning False.
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> create @M:T
              M:T { signatory = sig, observer = obs, precondition = False, key = M:toKey sig, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        inside(res) { case SResultError(SErrorDamlException(err)) =>
          err shouldBe a[TemplatePreconditionViolated]

        }
        msgs shouldBe Seq(
          "precondition"
        )
      }
      // TEST_EVIDENCE: Semantics: Evaluation order of create with duplicate contract key
      "duplicate contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
                let c: M:T = M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
                in sbind x : ContractId M:T <- create @M:T c
                  in create @M:T c
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        inside(res) { case SResultError(SErrorDamlException(err)) =>
          err shouldBe a[DuplicateContractKey]
        }
        msgs shouldBe allTraces ++ allTraces
      }
      // TEST_EVIDENCE: Semantics: Evaluation order of create with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> create @M:T
              M:T { signatory = sig, observer = obs, precondition = True, key = M:keyNoMaintainers, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        inside(res) { case SResultError(SErrorDamlException(err)) =>
          err shouldBe a[CreateEmptyContractKeyMaintainers]

        }
        msgs shouldBe allTraces
      }
      // TEST_EVIDENCE: Semantics: Evaluation order of create with authorization failure
      "authorization failure" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> create @M:T
              M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          bob,
        )
        inside(res) { case SResultError(SErrorDamlException(err)) =>
          err shouldBe a[FailedAuthorization]

        }
        msgs shouldBe allTraces
      }
      // TEST_EVIDENCE: Semantics: Evaluation order of create with contract id in contract key
      "contract id in contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) (cid : ContractId Unit) -> create @M:T
              M:T { signatory = sig, observer = obs, precondition = True, key = M:toKeyWithCid sig cid, nested = M:buildNested 0 }
           """,
          Array(
            SParty(alice),
            SParty(bob),
            SContractId(ContractId.V1.assertFromString("00" * 32 + "0000")),
          ),
          alice,
        )
        inside(res) { case SResultError(SErrorDamlException(err)) =>
          err shouldBe a[ContractIdInContractKey]

        }
        msgs shouldBe allTraces
      }
      // TEST_EVIDENCE: Semantics: Evaluation order of create with create argument exceeding max nesting
      "create argument exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> create @M:T
              M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 100 }
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        inside(res) { case SResultError(SErrorDamlException(Limit(err))) =>
          err shouldBe a[Limit.ValueNesting]

        }
        msgs shouldBe allTraces
      }
      // TEST_EVIDENCE: Semantics: Evaluation order of create with contract key exceeding max nesting
      "contract key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> create @M:T
              let key: M:TKey = M:TKey { maintainers = Cons @Party [sig] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 100 }
              in M:T { signatory = sig, observer = obs, precondition = True, key = key, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        inside(res) { case SResultError(SErrorDamlException(Limit(err))) =>
          err shouldBe a[Limit.ValueNesting]

        }
        msgs shouldBe allTraces
      }
    }
  }

}
