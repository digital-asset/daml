// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.data.Ref.{Location, Party}
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ValueParty, ValueRecord}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class TestTraceLog extends TraceLog {
  private val messages: ArrayBuffer[(String, Option[Location])] = new ArrayBuffer()

  override def add(message: String, optLocation: Option[Location]) = {
    messages += ((message, optLocation))
  }

  def tracePF[X, Y](text: String, pf: PartialFunction[X, Y]): PartialFunction[X, Y] = {
    case x if { add(text, None); pf.isDefinedAt(x) } => pf(x)
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
      
      record @serializable NoopArg = { controller : Party, observer : Party };
 
      val toKey : Party -> M:TKey = \(p : Party) ->
         M:TKey { maintainers = Cons @Party [p] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 0 };
      val keyNoMaintainers : M:TKey = M:TKey { maintainers = Nil @Party, optCid = None @(ContractId Unit), nested = M:buildNested 0 };
      val toKeyWithCid : Party -> ContractId Unit -> M:TKey = \(p : Party) (cid : ContractId Unit) -> M:TKey { maintainers = Cons @Party [p] (Nil @Party), optCid = Some @(ContractId Unit) cid, nested = M:buildNested 0 };

      record @serializable T = { signatory : Party, observer : Party, precondition : Bool, key: M:TKey, nested: M:Nested };
      template (this : T) = {
        precondition TRACE @Bool "precondition" (M:T {precondition} this);
        signatories TRACE @(List Party) "contract signatories" (Cons @Party [M:T {signatory} this] (Nil @Party));
        observers TRACE @(List Party) "contract observers" (Cons @Party [M:T {observer} this] (Nil @Party));
        agreement TRACE @Text "agreement" "";
        choice @nonConsuming Noop (self) (arg: M:NoopArg) : Unit,
          controllers TRACE @(List Party) "choice controllers" (Cons @Party [M:NoopArg {controller} arg] (Nil @Party)),
          observers TRACE @(List Party) "choice observers" (Cons @Party [M:NoopArg {observer} arg] (Nil @Party))
          to upure @Unit (TRACE @Unit "choice body" ()); 
        choice Archive (self) (p: Party): Unit, 
          controllers Cons @Party [p] (Nil @Party)
          to upure @Unit (TRACE @Unit "archive" ());
        key @M:TKey
           (TRACE @M:TKey "key" (M:T {key} this))
           (\(key : M:TKey) -> TRACE @(List Party) "maintainers" (M:TKey {maintainers} key));
      };
      
      record @serializable Dummy = { signatory : Party };
      template (this: Dummy) = {
        precondition True;
        signatories Cons @Party [M:Dummy {signatory} this] (Nil @Party);
        observers Nil @Party;
        agreement "";
      };   
    }
  """)

  private[this] val List(alice, bob, charlie) =
    List("alice", "bob", "charlie").map(Ref.Party.assertFromString)

  private[this] val T = t"M:T" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpect error")
  }

  private[this] val Dummy = t"M:Dummy" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpect error")
  }

  private[this] val cId: Value.ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))

  private[this] val emptyNestedValue = Value.ValueRecord(None, ImmArray(None -> Value.ValueNone))

  private[this] val keyValue = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueList(FrontStack(Value.ValueParty(alice))),
      None -> Value.ValueNone,
      None -> emptyNestedValue,
    ),
  )

  private[this] val contract = Versioned(
    TransactionVersion.StableVersions.max,
    Value.ContractInstance(
      T,
      Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(alice),
          None -> Value.ValueParty(bob),
          None -> Value.ValueTrue,
          None -> keyValue,
          None -> emptyNestedValue,
        ),
      ),
      "agreement",
    ),
  )

  private[this] val getContract = Map(cId -> contract)

  private[this] val getKey = Map(
    GlobalKeyWithMaintainers(GlobalKey.assertBuild(T, keyValue), Set(alice)) -> cId
  )

  private val seed = crypto.Hash.hashPrivateKey("seed")

  private def evalUpdateApp(
      pkgs: CompiledPackages,
      e: Expr,
      args: Array[SValue],
      party: Party,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  ): (Try[Either[SError, SValue]], Seq[String]) = {
    val se = pkgs.compiler.unsafeCompile(e)
    val traceLog = new TestTraceLog()
    val res = Try(
      SpeedyTestLib.run(
        Speedy.Machine
          .fromUpdateSExpr(
            pkgs,
            seed,
            if (args.isEmpty) se else SEApp(se, args.map(SEValue(_))),
            Set(party),
            traceLog = traceLog,
          ),
        getContract = traceLog.tracePF("getContract", getContract),
        getKey = traceLog.tracePF("getKey", getKey),
      )
    )
    val msgs = traceLog.getMessages
    (res, msgs)
  }

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

  val createTraces = Seq(
    "precondition",
    "agreement",
    "contract signatories",
    "contract observers",
    "key",
    "maintainers",
  )

  val localFetchByIdTraces = Seq.empty

  val globalFetchByIdTraces = Seq(
    "getContract",
    "contract signatories",
    "contract observers",
    "key",
    "maintainers",
  )

  val localFetchByKeyTraces = Seq("maintainers")

  val globalFetchByKeyTraces = Seq(
    "maintainers",
    "getKey",
    "getContract",
    "contract signatories",
    "contract observers",
  )

  "evaluation order" - {

    "create" - {

      // TEST_EVIDENCE: Semantics: Evaluation order of successful create
      "success" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> 
                 create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        inside(res) { case Success(Right(_)) =>
          msgs shouldBe createTraces
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with failed precondition
      "failed precondition" in {
        // Note that for LF >= 1.14 we don’t hit this as the compiler
        // generates code that throws an exception instead of returning False.
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> 
                create @M:T M:T { signatory = sig, observer = obs, precondition = False, key = M:toKey sig, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        inside(res) {
          case Success(Left(SErrorDamlException(IE.TemplatePreconditionViolated(T, _, _)))) =>
            msgs shouldBe Seq("precondition")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with duplicate contract key
      "duplicate contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
                let c: M:T = M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
                in ubind x : ContractId M:T <- create @M:T c
                  in create @M:T c
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.DuplicateContractKey(_)))) =>
          msgs shouldBe createTraces :++ createTraces
        }
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
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.CreateEmptyContractKeyMaintainers(T, _, _)))
              ) =>
            msgs shouldBe createTraces
        }
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
        inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
          msgs shouldBe createTraces
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with contract ID in contract key
      "contract ID in contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) (cid : ContractId Unit) -> create @M:T
              M:T { signatory = sig, observer = obs, precondition = True, key = M:toKeyWithCid sig cid, nested = M:buildNested 0 }
           """,
          Array(
            SParty(alice),
            SParty(bob),
            SContractId(Value.ContractId.V1.assertFromString("00" * 32 + "0000")),
          ),
          alice,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe createTraces
        }
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
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe createTraces
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> create @M:T
              let key: M:TKey = M:TKey { maintainers = Cons @Party [sig] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 100 }
              in M:T { signatory = sig, observer = obs, precondition = True, key = key, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          alice,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe createTraces
        }
      }
    }

    "fetch" - {

      "a global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch of a global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: ContractId M:T) -> fetch @M:T cId""",
            Array(SContractId(cId)),
            alice,
            getContract = getContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe globalFetchByIdTraces
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: ContractId M:T) (sig: Party) -> 
             ubind x: Unit <- exercise @M:T Archive cId sig
             in fetch @M:T cId""",
            Array(SContractId(cId), SParty(alice)),
            alice,
            getContract = getContract,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe globalFetchByIdTraces :+ "archive"
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of a wrongly type global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: ContractId M:T) -> fetch @M:T cId""",
            Array(SContractId(cId)),
            alice,
            getContract = Map(
              cId -> Versioned(
                TransactionVersion.StableVersions.max,
                Value.ContractInstance(
                  Dummy,
                  ValueRecord(None, ImmArray(None -> ValueParty(alice))),
                  "",
                ),
              )
            ),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq("getContract")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: ContractId M:T) -> fetch @M:T cId""",
            Array(SContractId(cId)),
            charlie,
            getContract = getContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe globalFetchByIdTraces
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) ->
             ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
             in fetch @M:T cId""",
            Array(SParty(alice), SParty(bob)),
            alice,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe createTraces :++ localFetchByIdTraces
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } ;
               x: Unit <- exercise @M:T Archive cId sig
             in fetch @M:T cId""",
            Array(SParty(alice), SParty(bob)),
            alice,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe createTraces :+ "archive"
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of an wrongly typed local contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) ->
             ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig } 
             in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
             in fetch @M:T cId2""",
            Array(SParty(alice)),
            alice,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq.empty
          }
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch of an unknown contract
      "unknown contract" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(cId: ContractId M:T) -> fetch @M:T cId""",
          Array(SContractId(cId)),
          alice,
          getContract = PartialFunction.empty,
        )
        inside(res) { case Failure(SpeedyTestLib.UnknownContract(`cId`)) =>
          msgs shouldBe Seq("getContract")
        }
      }

    }

    "fetch_by_key" - {

      "a global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch_by_key of a global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) -> fetch_by_key @M:T (M:toKey sig)""",
            Array(SParty(alice)),
            alice,
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe globalFetchByKeyTraces
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of an inactive global contract
        "inactive contract" in {

          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: ContractId M:T) (sig: Party) -> 
             ubind x: Unit <- exercise @M:T Archive cId sig
             in fetch_by_key @M:T (M:toKey sig)""",
            Array(SContractId(cId), SParty(alice)),
            alice,
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
            key.templateId shouldBe T
            msgs shouldBe globalFetchByIdTraces :++ Set("archive", "maintainers", "getKey")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of a global contract with authorization failure
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) -> fetch_by_key @M:T (M:toKey sig)""",
            Array(SParty(alice)),
            charlie,
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe globalFetchByKeyTraces
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch_by_key of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } 
             in fetch_by_key @M:T (M:toKey sig)""",
            Array(SParty(alice), SParty(bob)),
            alice,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe createTraces :++ localFetchByKeyTraces
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 };
               x: Unit <- exercise @M:T Archive cId sig
             in fetch_by_key @M:T (M:toKey sig)""",
            Array(SParty(alice), SParty(bob)),
            alice,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
            key.templateId shouldBe T
            msgs shouldBe createTraces :+ "archive" :++ localFetchByKeyTraces
          }
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of an unknown contract key
      "unknown contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig: Party) -> fetch_by_key @M:T (M:toKey sig)""",
          Array(SParty(alice)),
          alice,
          getContract = getContract,
          getKey = PartialFunction.empty,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
          key.templateId shouldBe T
          msgs shouldBe Seq("maintainers", "getKey")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""fetch_by_key @M:T M:keyNoMaintainers""",
          Array.empty,
          alice,
        )
        inside(res) {
          case Success(Left(SErrorDamlException(IE.FetchEmptyContractKeyMaintainers(T, _)))) =>
            msgs shouldBe localFetchByKeyTraces
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key with contract ID in contract key
      "contract ID in contract key " in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig: Party) (cId: ContractId M:T) -> fetch_by_key @M:T (M:toKeyWithCid sig cId)""",
          Array(SParty(alice), SContractId(cId)),
          alice,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe localFetchByKeyTraces
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) -> 
              fetch_by_key @M:T (M:TKey { maintainers = Cons @Party [sig] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 100 })
           """,
          Array(SParty(alice)),
          alice,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe localFetchByKeyTraces
        }
      }
    }
  }

}
