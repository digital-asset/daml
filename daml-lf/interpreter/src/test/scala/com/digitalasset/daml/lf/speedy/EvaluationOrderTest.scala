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
      
      val toKey : Party -> M:TKey = \(p : Party) ->
         M:TKey { maintainers = Cons @Party [p] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 0 };
      val keyNoMaintainers : M:TKey = M:TKey { maintainers = Nil @Party, optCid = None @(ContractId Unit), nested = M:buildNested 0 };
      val toKeyWithCid : Party -> ContractId Unit -> M:TKey = \(p : Party) (cid : ContractId Unit) -> M:TKey { maintainers = Cons @Party [p] (Nil @Party), optCid = Some @(ContractId Unit) cid, nested = M:buildNested 0 };

      variant @serializable Either (a:*) (b:*) = Left: a | Right : b;
      
      record @serializable T = { signatory : Party, observer : Party, precondition : Bool, key: M:TKey, nested: M:Nested };
      template (this : T) = {
        precondition TRACE @Bool "precondition" (M:T {precondition} this);
        signatories TRACE @(List Party) "contract signatories" (Cons @Party [M:T {signatory} this] (Nil @Party));
        observers TRACE @(List Party) "contract observers" (Cons @Party [M:T {observer} this] (Nil @Party));
        agreement TRACE @Text "agreement" "";
        choice Choice (self) (arg: M:Either M:Nested Int64) : M:Nested,
          controllers TRACE @(List Party) "choice controllers" (Cons @Party [M:T {signatory} this] (Nil @Party)),
          observers TRACE @(List Party) "choice observers" (Nil @Party)
          to upure @M:Nested (TRACE @M:Nested "choice body" (M:buildNested (case arg of M:Either:Right i -> i | _ -> 0)));
        choice Archive (self) (arg: Unit): Unit, 
          controllers Cons @Party [M:T {signatory} this] (Nil @Party)
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
        choice Archive (self) (arg: Unit): Unit, 
          controllers Cons @Party [M:Dummy {signatory} this] (Nil @Party)
          to upure @Unit ();
      };   
    }
    
    module Test{
      val noParty: Option Party = None @Party;
      val someParty: Party -> Option Party = \(p: Party) -> Some @Party p;
      val noCid: Option (ContractId Unit) = None @(ContractId Unit);
      val someCid: ContractId Unit -> Option (ContractId Unit) = \(cid: ContractId Unit) -> Some @(ContractId Unit) cid;
    
      val run: forall (t: *). Update t -> Update Unit = 
        /\(t: *). \(u: Update t) -> 
          ubind x:Unit <- upure @Unit (TRACE @Unit "starts test" ())
          in ubind y:t <- u 
          in upure @Unit (TRACE @Unit "ends test" ());

      val create: M:T -> Update Unit = 
        \(arg: M:T) -> Test:run @(ContractId M:T) (create @M:T arg);

      val exercise_by_id: Party -> ContractId M:T -> M:Either Int64 Int64 -> Update Unit =
        \(exercisingParty: Party) (cId: ContractId M:T) (argParams: M:Either Int64 Int64) -> 
          let arg: Test:ExeArg = Test:ExeArg {
            idOrKey = M:Either:Left @(ContractId M:T) @Test:TKeyParams cId,
            argParams = argParams
          }
          in ubind 
            bridgeId: ContractId Test:Bridge <- Test:createBridge exercisingParty;
            x: M:Nested <-exercise @Test:Bridge Exe bridgeId arg
          in upure @Unit ();
      
      val exercise_by_key: Party -> Option Party -> Option (ContractId Unit) -> Int64 -> M:Either Int64 Int64 -> Update Unit =
        \(exercisingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) (argParams: M:Either Int64 Int64) -> 
          let arg: Test:ExeArg = Test:ExeArg {
            idOrKey = M:Either:Right @(ContractId M:T) @Test:TKeyParams (Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting}),
            argParams = argParams
          }
          in ubind 
            bridgeId: ContractId Test:Bridge <- Test:createBridge exercisingParty;
            x: M:Nested <-exercise @Test:Bridge Exe bridgeId arg
          in upure @Unit ();

      val fetch_by_id: Party -> ContractId M:T -> Update Unit =
        \(fetchingParty: Party) (cId: ContractId M:T) -> 
          ubind bridgeId: ContractId Test:Bridge <- Test:createBridge fetchingParty  
          in exercise @Test:Bridge FetchById bridgeId cId;

      val fetch_by_key: Party -> Option Party -> Option (ContractId Unit) -> Int64 -> Update Unit = 
        \(fetchingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) -> 
           ubind bridgeId: ContractId Test:Bridge <- Test:createBridge fetchingParty  
           in exercise @Test:Bridge FetchByKey bridgeId (Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting});
      
      val lookup_by_key: Party -> Option Party -> Option (ContractId Unit) -> Int64 -> Update Unit = 
        \(lookingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) -> 
           ubind bridgeId: ContractId Test:Bridge <- Test:createBridge lookingParty  
           in exercise @Test:Bridge LookupByKey bridgeId (Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting});
      
      val createBridge: Party -> Update (ContractId Test:Bridge) = 
        \(party: Party) -> create @Test:Bridge Test:Bridge { sig = party, obs = party }; 
      
      val optToList: forall(t:*). Option t -> List t  = 
        /\(t:*). \(opt: Option t) -> 
          case opt of 
             None -> Nil @t
           | Some x -> Cons @t [x] (Nil @t);

      record @serializable TKeyParams = { maintainers : List Party, optCid : Option (ContractId Unit), nesting: Int64 };
      val buildTKey: (Test:TKeyParams) -> M:TKey = 
        \(params: Test:TKeyParams) -> M:TKey { 
            maintainers = Test:TKeyParams {maintainers} params,
            optCid = Test:TKeyParams {optCid} params,
            nested = M:buildNested (Test:TKeyParams {nesting} params)
          };
       
      record @serializable ExeArg = {
        idOrKey: M:Either (ContractId M:T) Test:TKeyParams,
        argParams: M:Either Int64 Int64 
      };

      record @serializable Bridge = { sig: Party, obs: Party };
      template (this: Bridge) = {
        precondition True;
        signatories Cons @Party [Test:Bridge {sig} this] (Nil @Party);
        observers Nil @Party;
        agreement "";
        choice CreateNonvisibleKey (self) (arg: Unit): ContractId M:T,
          controllers Cons @Party [Test:Bridge {obs} this] (Nil @Party),
          observers Nil @Party
           to let sig: Party = Test:Bridge {sig} this 
           in create @M:T M:T { signatory = sig, observer = sig, precondition = True, key = M:toKey sig, nested = M:buildNested 0 };              
        choice Exe (self) (arg: Test:ExeArg): M:Nested,
          controllers Cons @Party [Test:Bridge {sig} this] (Nil @Party),
          observers Nil @Party
          to 
            let choiceArg: M:Either M:Nested Int64 = case (Test:ExeArg {argParams} arg) of
                M:Either:Left n -> M:Either:Left @M:Nested @Int64 (M:buildNested n)
              | M:Either:Right n -> M:Either:Right @M:Nested @Int64 n
            in let update: Update M:Nested = case (Test:ExeArg {idOrKey} arg) of 
                M:Either:Left cId -> exercise @M:T Choice cId choiceArg
              | M:Either:Right keyParams -> exercise_by_key @M:T Choice (Test:buildTKey keyParams) choiceArg
            in ubind
              x:Unit <- upure @Unit (TRACE @Unit "starts test" ());
              res: M:Nested <- update;
              y:Unit <- upure @Unit (TRACE @Unit "ends test" ())
            in upure @M:Nested res;
        choice FetchById (self) (cId: ContractId M:T): Unit, 
          controllers Cons @Party [Test:Bridge {sig} this] (Nil @Party),
          observers Nil @Party
          to Test:run @M:T (fetch @M:T cId);
        choice FetchByKey (self) (params: Test:TKeyParams): Unit,
          controllers Cons @Party [Test:Bridge {sig} this] (Nil @Party),
          observers Nil @Party
          to let key: M:TKey = Test:buildTKey params 
             in Test:run @<contract: M:T, contractId: ContractId M:T> (fetch_by_key @M:T key);
        choice LookupByKey (self) (params: Test:TKeyParams): Unit,
          controllers Cons @Party [Test:Bridge {sig} this] (Nil @Party),
          observers Nil @Party
          to let key: M:TKey = Test:buildTKey params 
             in Test:run @(Option (ContractId M:T)) (lookup_by_key @M:T key);
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

  private[this] val Bridge = t"Test:Bridge" match {
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

  private[this] val dummyContract = Versioned(
    TransactionVersion.StableVersions.max,
    Value.ContractInstance(
      Dummy,
      ValueRecord(None, ImmArray(None -> ValueParty(alice))),
      "",
    ),
  )
  private[this] val getWronglyTypedContract = Map(cId -> dummyContract)

  private[this] val seed = crypto.Hash.hashPrivateKey("seed")

  private[this] def evalUpdateApp(
      pkgs: CompiledPackages,
      e: Expr,
      args: Array[SValue],
      parties: Set[Party],
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
            parties,
            traceLog = traceLog,
          ),
        getContract = traceLog.tracePF("queries contract", getContract),
        getKey = traceLog.tracePF("queries key", getKey),
      )
    )
    val msgs = traceLog.getMessages.dropWhile(_ != "starts test")
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

  "evaluation order" - {

    "create" - {

      // TEST_EVIDENCE: Semantics: Evaluation order of successful create
      "success" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> 
                 Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) { case Success(Right(_)) =>
          msgs shouldBe Seq(
            "starts test",
            "precondition",
            "agreement",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
            "ends test",
          )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with failed precondition
      "failed precondition" in {
        // Note that for LF >= 1.14 we don’t hit this as the compiler
        // generates code that throws an exception instead of returning False.
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> 
                Test:create M:T { signatory = sig, observer = obs, precondition = False, key = M:toKey sig, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(Left(SErrorDamlException(IE.TemplatePreconditionViolated(T, _, _)))) =>
            msgs shouldBe Seq("starts test", "precondition")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with duplicate contract key
      "duplicate contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
                let c: M:T = M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
                in ubind x : ContractId M:T <- create @M:T c
                  in Test:create c
           """,
          Array(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.DuplicateContractKey(_)))) =>
          msgs shouldBe Seq(
            "starts test",
            "precondition",
            "agreement",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
          )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> 
                Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:keyNoMaintainers, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(Left(SErrorDamlException(IE.CreateEmptyContractKeyMaintainers(T, _, _)))) =>
            msgs shouldBe Seq(
              "starts test",
              "precondition",
              "agreement",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
            )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with authorization failure
      "authorization failure" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> 
                Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          Set(bob),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
          msgs shouldBe Seq(
            "starts test",
            "precondition",
            "agreement",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
          )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with contract ID in contract key
      "contract ID in contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) (cid : ContractId Unit) -> 
                Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:toKeyWithCid sig cid, nested = M:buildNested 0 }
           """,
          Array(
            SParty(alice),
            SParty(bob),
            SContractId(Value.ContractId.V1.assertFromString("00" * 32 + "0000")),
          ),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe Seq(
            "starts test",
            "precondition",
            "agreement",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
          )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with create argument exceeding max nesting
      "create argument exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> 
                Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 100 }
           """,
          Array(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe Seq(
            "starts test",
            "precondition",
            "agreement",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
          )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) -> 
              let key: M:TKey = M:TKey { maintainers = Cons @Party [sig] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 100 }
              in Test:create M:T { signatory = sig, observer = obs, precondition = True, key = key, nested = M:buildNested 0 }
           """,
          Array(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe Seq(
            "starts test",
            "precondition",
            "agreement",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
          )
        }
      }
    }

    "exercise" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "queries contract",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "choice controllers",
              "choice observers",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of a wrongly typed non-cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq("starts test", "queries contract")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of a non-cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq(
              "starts test",
              "queries contract",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "choice controllers",
              "choice observers",
            )
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> 
               ubind x: M:T <- fetch @M:T cId in 
               Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
               """,
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "choice controllers",
              "choice observers",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T)  -> 
             ubind x: Unit <- exercise @M:T Archive cId () in 
               Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
             """,
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> 
               ubind x: M:Dummy <- fetch @M:Dummy cId in
               Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
               """,
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> 
               ubind x: M:Dummy <- exercise @M:Dummy Archive cId () in
               Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
               """,
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> 
               ubind x: M:T <- fetch @M:T cId
               in  Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test", "choice controllers", "choice observers")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (exercisingParty : Party) ->
             ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } in 
             Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
             """,
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "choice controllers",
              "choice observers",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (exercisingParty : Party) ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } ;
               x: Unit <- exercise @M:T Archive cId ()
             in 
               Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
             """,
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of an wrongly typed local contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (exercisingParty : Party) ->
             ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig } 
             in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
             in 
               Test:exercise_by_id exercisingParty cId1 (M:Either:Left @Int64 @Int64 0)
             """,
            Array(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (exercisingParty : Party) ->
             ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig } 
             in ubind x: Unit <- exercise @M:Dummy Archive cId1 ()
             in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
             in 
               Test:exercise_by_id exercisingParty cId1 (M:Either:Left @Int64 @Int64 0)
             """,
            Array(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of a cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (exercisingParty : Party) ->
                  ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
                  in 
                    Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
                  """,
            Array(SParty(alice), SParty(bob), SParty(charlie)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq(
              "starts test",
              "choice controllers",
              "choice observers",
            )
          }
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of exercise of an unknown contract
      "unknown contract" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
          Array(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = PartialFunction.empty,
        )
        inside(res) { case Failure(SpeedyTestLib.UnknownContract(`cId`)) =>
          msgs shouldBe Seq("starts test", "queries contract")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of exercise with argument exceeding max nesting
      "argument exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 100)""",
          Array(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = getContract,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe Seq(
            "starts test",
            "queries contract",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
            "choice controllers",
            "choice observers",
          )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of exercise with output exceeding max nesting
      "output exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Right @Int64 @Int64 100)""",
          Array(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = getContract,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe Seq(
            "starts test",
            "queries contract",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
            "choice controllers",
            "choice observers",
            "choice body",
          )
        }
      }
    }

    "exercise_by_key" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise_by_key of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "maintainers",
              "queries key",
              "queries contract",
              "contract signatories",
              "contract observers",
              "choice controllers",
              "choice observers",
              "choice body",
              "ends test",
            )
          }
        }

        // This case may happen only if there is a bug in the ledger.
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getWronglyTypedContract,
            getKey = getKey,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq("starts test", "maintainers", "queries key", "queries contract")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key of a non-cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(charlie), SParty(alice)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = getKey,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq(
              "starts test",
              "maintainers",
              "queries key",
              "queries contract",
              "contract signatories",
              "contract observers",
              "choice controllers",
              "choice observers",
            )
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise_by_key of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) (cId: ContractId M:T) ->
               ubind x: M:T <- fetch @M:T cId in 
               Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
               """,
            Array(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "maintainers",
              "queries key",
              "choice controllers",
              "choice observers",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (exercisingParty : Party) (cId: ContractId M:T)  -> 
             ubind x: Unit <- exercise @M:T Archive cId () in
               Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
             """,
            Array(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(gkey)))) =>
            gkey.templateId shouldBe T
            msgs shouldBe Seq("starts test", "maintainers", "queries key")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) (sig: Party) ->
               ubind x: M:Dummy <- fetch @M:Dummy cId in
               Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
               """,
            Array(SParty(alice), SContractId(cId), SParty(alice)),
            Set(alice),
            getContract = getWronglyTypedContract,
            getKey = getKey,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq("starts test", "maintainers", "queries key")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key of cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) (sig: Party) ->
               ubind x: M:T <- fetch @M:T cId
               in Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(charlie), SContractId(cId), SParty(alice)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = getKey,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq(
              "starts test",
              "maintainers",
              "queries key",
              "choice controllers",
              "choice observers",
            )

          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise_by_key of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (exercisingParty : Party) ->
             ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } in 
             Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
             """,
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "maintainers",
              "choice controllers",
              "choice observers",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (exercisingParty : Party) ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } ;
               x: Unit <- exercise @M:T Archive cId ()
             in 
               Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
             """,
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(gKey)))) =>
            gKey.templateId shouldBe T
            msgs shouldBe Seq("starts test", "maintainers")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key of a cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (exercisingParty : Party) ->
                  ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
                  in 
                    Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
                  """,
            Array(SParty(alice), SParty(bob), SParty(charlie)),
            Set(alice, charlie),
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq(
              "starts test",
              "maintainers",
              "choice controllers",
              "choice observers",
            )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup of a local contract with visibility failure
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: Test:Bridge) (sig : Party) (exercisingParty: Party) ->
             ubind x: ContractId M:T <- exercise @Test:Bridge CreateNonvisibleKey cId ()
             in Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            Array(SContractId(cId), SParty(alice), SParty(charlie)),
            Set(charlie),
            getContract = Map(
              cId -> Versioned(
                TransactionVersion.StableVersions.max,
                Value.ContractInstance(
                  Bridge,
                  ValueRecord(
                    None,
                    ImmArray(None -> ValueParty(alice), None -> ValueParty(charlie)),
                  ),
                  "",
                ),
              )
            ),
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.LocalContractKeyNotVisible(_, key, _, _, _)))
                ) =>
              key.templateId shouldBe T
              msgs shouldBe Seq("starts test", "maintainers")
          }
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key of an unknown contract
      "unknown contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Some @Party sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
          Array(SParty(alice), SParty(alice)),
          Set(alice),
          getContract = PartialFunction.empty,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
          key.templateId shouldBe T
          msgs shouldBe Seq("starts test", "maintainers", "queries key")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key with argument exceeding max nesting
      "argument exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Some @Party sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 100)""",
          Array(SParty(alice), SParty(alice)),
          Set(alice),
          getContract = getContract,
          getKey = getKey,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe Seq(
            "starts test",
            "maintainers",
            "queries key",
            "queries contract",
            "contract signatories",
            "contract observers",
            "choice controllers",
            "choice observers",
          )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key with result exceeding max nesting
      "result exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Right @Int64 @Int64 100)""",
          Array(SParty(alice), SParty(alice)),
          Set(alice),
          getContract = getContract,
          getKey = getKey,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe Seq(
            "starts test",
            "maintainers",
            "queries key",
            "queries contract",
            "contract signatories",
            "contract observers",
            "choice controllers",
            "choice observers",
            "choice body",
          )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of exercise_vy_key with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty: Party) -> Test:exercise_by_key exercisingParty Test:noParty Test:noCid 0 (M:Either:Right @Int64 @Int64 100)""",
          Array(SParty(alice)),
          Set(alice),
        )
        inside(res) {
          case Success(Left(SErrorDamlException(IE.FetchEmptyContractKeyMaintainers(T, _)))) =>
            msgs shouldBe Seq("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key with contract ID in contract key
      "contract ID in contract key " in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty: Party) (sig: Party) (cId: ContractId M:T) -> 
                 Test:exercise_by_key exercisingParty (Test:someParty sig) (Test:someCid cId) 0 (M:Either:Right @Int64 @Int64 100)""",
          Array(SParty(alice), SParty(alice), SContractId(cId)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe Seq("starts test", "maintainers")
        }
      }

    }

    "fetch" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_by_id""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "queries contract",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of a wrongly typed non-cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_by_id""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq("starts test", "queries contract")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of a non-cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_by_id""",
            Array(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq(
              "starts test",
              "queries contract",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
            )
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T) -> 
               ubind x: M:T <- fetch @M:T cId in 
               Test:fetch_by_id fetchingParty cId
               """,
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T)  -> 
             ubind x: Unit <- exercise @M:T Archive cId ()
             in Test:fetch_by_id fetchingParty cId""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T) -> 
               ubind x: M:Dummy <- fetch @M:Dummy cId
               in Test:fetch_by_id fetchingParty cId""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T) -> 
               ubind x: M:Dummy <- exercise @M:Dummy Archive cId ()
               in Test:fetch_by_id fetchingParty cId""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T) -> 
               ubind x: M:T <- fetch @M:T cId
               in Test:fetch_by_id fetchingParty cId""",
            Array(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (fetchingParty: Party) ->
             ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
             in Test:fetch_by_id fetchingParty cId""",
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (fetchingParty: Party) ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } ;
               x: Unit <- exercise @M:T Archive cId ()
             in Test:fetch_by_id fetchingParty cId""",
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of an wrongly typed local contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (fetchingParty: Party) ->
             ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig } 
             in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
             in Test:fetch_by_id fetchingParty cId2""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (fetchingParty: Party) ->
             ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig } 
             in ubind x: Unit <- exercise @M:Dummy Archive cId1 () 
             in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
             in Test:fetch_by_id fetchingParty cId2""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of a cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (fetchingParty: Party) ->
                  ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
                  in Test:fetch_by_id fetchingParty cId""",
            Array(SParty(alice), SParty(bob), SParty(charlie)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test")
          }
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch of an unknown contract
      "unknown contract" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty: Party) (cId: ContractId M:T) -> Test:fetch_by_id fetchingParty cId""",
          Array(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = PartialFunction.empty,
        )
        inside(res) { case Failure(SpeedyTestLib.UnknownContract(`cId`)) =>
          msgs shouldBe Seq("starts test", "queries contract")
        }
      }
    }

    "fetch_by_key" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch_by_key of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) -> Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "maintainers",
              "queries key",
              "queries contract",
              "contract signatories",
              "contract observers",
              "ends test",
            )
          }
        }

        // This case may happen only if there is a bug in the ledger.
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) -> Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getWronglyTypedContract,
            getKey = getKey,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe Seq(
                "starts test",
                "maintainers",
                "queries key",
                "queries contract",
              )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of a non-cached global contract with authorization failure
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) -> Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq(
              "starts test",
              "maintainers",
              "queries key",
              "queries contract",
              "contract signatories",
              "contract observers",
            )
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch_by_key of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) (cId: ContractId M:T) -> 
                 ubind x: M:T <- fetch @M:T cId 
                 in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "maintainers", "queries key", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of an inactive global contract
        "inactive contract" in {

          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: ContractId M:T) (fetchingParty: Party) (sig: Party) -> 
             ubind x: Unit <- exercise @M:T Archive cId ()
             in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SContractId(cId), SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
            key.templateId shouldBe T
            msgs shouldBe Seq("starts test", "maintainers", "queries key")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of a cached global contract with authorization failure
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) (cId: ContractId M:T) ->                 
               ubind x: M:T <- fetch @M:T cId                                        
               in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test", "maintainers", "queries key")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch_by_key of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (fetchingParty: Party)  ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } 
             in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "maintainers", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (fetchingParty: Party) ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 };
               x: Unit <- exercise @M:T Archive cId ()
             in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
            key.templateId shouldBe T
            msgs shouldBe Seq("starts test", "maintainers")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of a local contract with authorization failure
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: Test:Bridge) (sig : Party) (fetchingParty: Party) ->
             ubind x: ContractId M:T <- exercise @Test:Bridge CreateNonvisibleKey cId ()
             in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SContractId(cId), SParty(alice), SParty(charlie)),
            Set(charlie),
            getContract = Map(
              cId -> Versioned(
                TransactionVersion.StableVersions.max,
                Value.ContractInstance(
                  Bridge,
                  ValueRecord(
                    None,
                    ImmArray(None -> ValueParty(alice), None -> ValueParty(charlie)),
                  ),
                  "",
                ),
              )
            ),
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.LocalContractKeyNotVisible(_, key, _, _, _)))
                ) =>
              key.templateId shouldBe T
              msgs shouldBe Seq("starts test", "maintainers")
          }
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of an unknown contract key
      "unknown contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty:Party) (sig: Party) -> Test:fetch_by_key fetchingParty (Some @Party sig) (None @(ContractId Unit)) 0""",
          Array(SParty(alice), SParty(alice)),
          Set(alice),
          getContract = getContract,
          getKey = PartialFunction.empty,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
          key.templateId shouldBe T
          msgs shouldBe Seq("starts test", "maintainers", "queries key")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty: Party) -> Test:fetch_by_key fetchingParty Test:noParty Test:noCid 0""",
          Array(SParty(alice)),
          Set(alice),
        )
        inside(res) {
          case Success(Left(SErrorDamlException(IE.FetchEmptyContractKeyMaintainers(T, _)))) =>
            msgs shouldBe Seq("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key with contract ID in contract key
      "contract ID in contract key " in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty: Party) (sig: Party) (cId: ContractId M:T) -> 
                 Test:fetch_by_key fetchingParty (Test:someParty sig) (Test:someCid cId) 0""",
          Array(SParty(alice), SParty(alice), SContractId(cId)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe Seq("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (fetchingParty: Party) -> Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 100""",
          Array(SParty(alice), SParty(alice)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe Seq("starts test", "maintainers")
        }
      }
    }

    "lookup_by_key" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful lookup_by_key of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) -> Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "maintainers",
              "queries key",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key of a non-cached global contract with authorization failure
        "authorization failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) -> Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test", "maintainers", "queries key")
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful lookup_by_key of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) (cId: ContractId M:T) -> 
                 ubind x: M:T <- fetch @M:T cId 
                 in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "maintainers", "queries key", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key of an inactive global contract
        "inactive contract" in {

          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: ContractId M:T) (lookingParty: Party) (sig: Party) -> 
             ubind x: Unit <- exercise @M:T Archive cId ()
             in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SContractId(cId), SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "maintainers", "queries key", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key of a cached global contract with authorization failure
        "authorization failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) (cId: ContractId M:T) ->                 
               ubind x: M:T <- fetch @M:T cId                                        
               in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test", "maintainers", "queries key")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful lookup_by_key of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (lookingParty: Party)  ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } 
             in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "maintainers", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (lookingParty: Party) ->
             ubind 
               cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 };
               x: Unit <- exercise @M:T Archive cId ()
             in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "maintainers", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key of a local contract with failure authorization
        "authorization failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (lookingParty: Party) ->
                  ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
                 in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(bob), SParty(charlie)),
            Set(alice, charlie),
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test", "maintainers")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key of a local contract with authorization failure
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: Test:Bridge) (sig : Party) (lookingParty: Party) ->
             ubind x: ContractId M:T <- exercise @Test:Bridge CreateNonvisibleKey cId ()
             in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SContractId(cId), SParty(alice), SParty(charlie)),
            Set(charlie),
            getContract = Map(
              cId -> Versioned(
                TransactionVersion.StableVersions.max,
                Value.ContractInstance(
                  Bridge,
                  ValueRecord(
                    None,
                    ImmArray(None -> ValueParty(alice), None -> ValueParty(charlie)),
                  ),
                  "",
                ),
              )
            ),
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.LocalContractKeyNotVisible(_, key, _, _, _)))
                ) =>
              key.templateId shouldBe T
              msgs shouldBe Seq("starts test", "maintainers")
          }
        }
      }

      "an undefined key" - {
        // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key of an unknown contract key
        "successful" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) -> Test:lookup_by_key lookingParty (Some @Party sig) None @(ContractId Unit) 0""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKey = PartialFunction.empty,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "maintainers", "queries key", "ends test")
          }
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(lookingParty: Party) -> Test:lookup_by_key lookingParty Test:noParty Test:noCid 0""",
          Array(SParty(alice)),
          Set(alice),
        )
        inside(res) {
          case Success(Left(SErrorDamlException(IE.FetchEmptyContractKeyMaintainers(T, _)))) =>
            msgs shouldBe Seq("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key with contract ID in contract key
      "contract ID in contract key " in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(lookingParty: Party) (sig: Party) (cId: ContractId M:T) -> 
                 Test:lookup_by_key lookingParty (Test:someParty sig) (Test:someCid cId) 0""",
          Array(SParty(alice), SParty(alice), SContractId(cId)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe Seq("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (lookingParty: Party) -> Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 100""",
          Array(SParty(alice), SParty(alice)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.Limit(IE.Limit.ValueNesting(_))))) =>
          msgs shouldBe Seq("starts test", "maintainers")
        }
      }
    }
  }

}
