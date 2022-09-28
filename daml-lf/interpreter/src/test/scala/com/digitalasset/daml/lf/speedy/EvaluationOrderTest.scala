// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.command.ContractMetadata
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.daml.lf.data.Ref.{Location, Party}
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits.{defaultParserParameters => _, _}
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.ledger.FailedAuthorization
import com.daml.lf.ledger.FailedAuthorization.{
  ExerciseMissingAuthorization,
  FetchMissingAuthorization,
  LookupByKeyMissingAuthorization,
}
import com.daml.lf.testing.parser.{ParserParameters, defaultPackageId}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ValueParty, ValueRecord}
import com.daml.logging.LoggingContext
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class TestTraceLog extends TraceLog {
  private val messages: ArrayBuffer[(String, Option[Location])] = new ArrayBuffer()

  override def add(message: String, optLocation: Option[Location])(implicit
      loggingContext: LoggingContext
  ) = {
    messages += ((message, optLocation))
  }

  def tracePF[X, Y](text: String, pf: PartialFunction[X, Y]): PartialFunction[X, Y] = {
    case x if { add(text, None)(LoggingContext.ForTesting); pf.isDefinedAt(x) } => pf(x)
  }

  override def iterator = messages.iterator

  def getMessages: Seq[String] = messages.view.map(_._1).toSeq
}

class EvaluationOrderTest extends AnyFreeSpec with Matchers with Inside {

  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  private[this] implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters(defaultPackageId, languageVersion = LanguageVersion.v1_dev)

  private lazy val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(p"""
    module M {

      record @serializable MyUnit = {};

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

      interface (this : I1) =  { viewtype M:MyUnit; };

      interface (this: Person) = {
        viewtype M:MyUnit;
        method asParty: Party;
        method getCtrl: Party;
        method getName: Text;
        choice Sleep (self) (u:Unit) : ContractId M:Person
          , controllers TRACE @(List Party) "choice controllers" (Cons @Party [call_method @M:Person getCtrl this] (Nil @Party))
          to upure @(ContractId M:Person) self;
        choice @nonConsuming Nap (self) (i : Int64): Int64
          , controllers TRACE @(List Party) "choice controllers" (Cons @Party [call_method @M:Person getCtrl this] (Nil @Party))
          , observers TRACE @(List Party) "choice observers" (Nil @Party)
          to upure @Int64 (TRACE @Int64 "choice body" i);
      } ;

      record @serializable T = { signatory : Party, observer : Party, precondition : Bool, key: M:TKey, nested: M:Nested };
      template (this: T) = {
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
        choice @nonConsuming Divulge (self) (divulgee: Party): Unit,
          controllers Cons @Party [divulgee] (Nil @Party)
          to upure @Unit ();
        key @M:TKey
           (TRACE @M:TKey "key" (M:T {key} this))
           (\(key : M:TKey) -> TRACE @(List Party) "maintainers" (M:TKey {maintainers} key));
      };

      record @serializable Human = { person: Party, obs: Party, ctrl: Party, precond: Bool, key: M:TKey, nested: M:Nested };
      template (this: Human) = {
        precondition TRACE @Bool "precondition" (M:Human {precond} this);
        signatories TRACE @(List Party) "contract signatories" (Cons @Party [M:Human {person} this] (Nil @Party));
        observers TRACE @(List Party) "contract observers" (Cons @Party [M:Human {obs} this] (Nil @Party));
        agreement TRACE @Text "agreement" "";
        choice Archive (self) (arg: Unit): Unit,
          controllers Cons @Party [M:Human {person} this] (Nil @Party)
          to upure @Unit (TRACE @Unit "archive" ());
        implements M:Person {
          view = TRACE @M:MyUnit "view" (M:MyUnit {});
          method asParty = M:Human {person} this;
          method getName = "foobar";
          method getCtrl = M:Human {ctrl} this;
          };
        key @M:TKey
           (TRACE @M:TKey "key" (M:Human {key} this))
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

    module Test {
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

      val create_interface: M:Human -> Update Unit =
        \(arg: M:Human) -> Test:run @(ContractId M:Person) (create_by_interface @M:Person (to_interface @M:Person @M:Human arg));

      val exercise_by_id: Party -> ContractId M:T -> M:Either Int64 Int64 -> Update Unit =
        \(exercisingParty: Party) (cId: ContractId M:T) (argParams: M:Either Int64 Int64) ->
          let arg: Test:ExeArg = Test:ExeArg {
            idOrKey = M:Either:Left @(ContractId M:T) @Test:TKeyParams cId,
            argParams = argParams
          }
          in ubind
            helperId: ContractId Test:Helper <- Test:createHelper exercisingParty;
            x: M:Nested <-exercise @Test:Helper Exe helperId arg
          in upure @Unit ();

      val exercise_interface_with_guard: Party -> ContractId M:Person -> Update Unit =
        \(exercisingParty: Party) (cId: ContractId M:Person) ->
          Test:run @Int64 (exercise_interface_with_guard @M:Person Nap cId 42 (\(x: M:Person) -> TRACE @Bool "interface guard" True));

      val exercise_interface: Party -> ContractId M:Person -> Update Unit =
        \(exercisingParty: Party) (cId: ContractId M:Person) ->
          Test:run @Int64 (exercise_interface @M:Person Nap cId 42);

      val exercise_by_key: Party -> Option Party -> Option (ContractId Unit) -> Int64 -> M:Either Int64 Int64 -> Update Unit =
        \(exercisingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) (argParams: M:Either Int64 Int64) ->
          let arg: Test:ExeArg = Test:ExeArg {
            idOrKey = M:Either:Right @(ContractId M:T) @Test:TKeyParams (Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting}),
            argParams = argParams
          }
          in ubind
            helperId: ContractId Test:Helper <- Test:createHelper exercisingParty;
            x: M:Nested <- exercise @Test:Helper Exe helperId arg
          in upure @Unit ();

      val fetch_by_id: Party -> ContractId M:T -> Update Unit =
        \(fetchingParty: Party) (cId: ContractId M:T) ->
          ubind helperId: ContractId Test:Helper <- Test:createHelper fetchingParty
          in exercise @Test:Helper FetchById helperId cId;

      val fetch_interface: Party -> ContractId M:Person -> Update Unit =
        \(fetchingParty: Party) (cId: ContractId M:Person) ->
          ubind helperId: ContractId Test:Helper <- Test:createHelper fetchingParty
          in exercise @Test:Helper FetchByInterface helperId cId;

      val fetch_by_key: Party -> Option Party -> Option (ContractId Unit) -> Int64 -> Update Unit =
        \(fetchingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) ->
           ubind helperId: ContractId Test:Helper <- Test:createHelper fetchingParty
           in exercise @Test:Helper FetchByKey helperId (Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting});

      val lookup_by_key: Party -> Option Party -> Option (ContractId Unit) -> Int64 -> Update Unit =
        \(lookingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) ->
           ubind helperId: ContractId Test:Helper <- Test:createHelper lookingParty
           in exercise @Test:Helper LookupByKey helperId (Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting});

      val createHelper: Party -> Update (ContractId Test:Helper) =
        \(party: Party) -> create @Test:Helper Test:Helper { sig = party, obs = party };

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

      record @serializable Helper = { sig: Party, obs: Party };
      template (this: Helper) = {
        precondition True;
        signatories Cons @Party [Test:Helper {sig} this] (Nil @Party);
        observers Nil @Party;
        agreement "";
        choice CreateNonvisibleKey (self) (arg: Unit): ContractId M:T,
          controllers Cons @Party [Test:Helper {obs} this] (Nil @Party),
          observers Nil @Party
           to let sig: Party = Test:Helper {sig} this
           in create @M:T M:T { signatory = sig, observer = sig, precondition = True, key = M:toKey sig, nested = M:buildNested 0 };
        choice Exe (self) (arg: Test:ExeArg): M:Nested,
          controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
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
          controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
          observers Nil @Party
          to Test:run @M:T (fetch_template @M:T cId);
        choice FetchByInterface (self) (cId: ContractId M:Person): Unit,
          controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
          observers Nil @Party
          to Test:run @M:Person (fetch_interface @M:Person cId);
        choice FetchByKey (self) (params: Test:TKeyParams): Unit,
          controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
          observers Nil @Party
          to let key: M:TKey = Test:buildTKey params
             in Test:run @<contract: M:T, contractId: ContractId M:T> (fetch_by_key @M:T key);
        choice LookupByKey (self) (params: Test:TKeyParams): Unit,
          controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
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

  private[this] val Human = t"M:Human" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpect error")
  }

  private[this] val Person = t"M:Person" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpect error")
  }

  private[this] val Dummy = t"M:Dummy" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpect error")
  }

  private[this] val Helper = t"Test:Helper" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpect error")
  }

  private[this] val cId: Value.ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))

  private[this] val helperCId: Value.ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("Helper"))

  private[this] val emptyNestedValue = Value.ValueRecord(None, ImmArray(None -> Value.ValueNone))

  private[this] val keyValue = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueList(FrontStack(Value.ValueParty(alice))),
      None -> Value.ValueNone,
      None -> emptyNestedValue,
    ),
  )

  private[this] def buildContract(observer: Party): Versioned[Value.ContractInstance] =
    Versioned(
      TransactionVersion.StableVersions.max,
      Value.ContractInstance(
        T,
        Value.ValueRecord(
          None,
          ImmArray(
            None -> Value.ValueParty(alice),
            None -> Value.ValueParty(observer),
            None -> Value.ValueTrue,
            None -> keyValue,
            None -> emptyNestedValue,
          ),
        ),
        "agreement",
      ),
    )

  private[this] def buildDisclosedContract(signatory: Party): Versioned[DisclosedContract] =
    Versioned(
      TransactionVersion.minExplicitDisclosure,
      DisclosedContract(
        Dummy,
        SContractId(cId),
        SRecord(
          Dummy,
          ImmArray(Ref.Name.assertFromString("signatory")),
          ArrayList(SParty(signatory)),
        ),
        ContractMetadata(Time.Timestamp.now(), None, ImmArray.Empty),
      ),
    )

  private[this] val visibleContract = buildContract(bob)
  private[this] val nonVisibleContract = buildContract(alice)

  private[this] val helper = Versioned(
    TransactionVersion.StableVersions.max,
    Value.ContractInstance(
      Helper,
      ValueRecord(
        None,
        ImmArray(None -> ValueParty(alice), None -> ValueParty(charlie)),
      ),
      "",
    ),
  )

  private[this] val iface_contract = Versioned(
    TransactionVersion.StableVersions.max,
    Value.ContractInstance(
      Human,
      Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(alice),
          None -> Value.ValueParty(bob),
          None -> Value.ValueParty(alice),
          None -> Value.ValueTrue,
          None -> keyValue,
          None -> emptyNestedValue,
        ),
      ),
      "agreement",
    ),
  )

  private[this] val getContract = Map(cId -> visibleContract)
  private[this] val getNonVisibleContract = Map(cId -> nonVisibleContract)
  private[this] val getIfaceContract = Map(cId -> iface_contract)
  private[this] val getHelper = Map(helperCId -> helper)

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
      disclosedContracts: ImmArray[Versioned[DisclosedContract]] = ImmArray.Empty,
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
            if (args.isEmpty) se else SEApp(se, args),
            parties,
            disclosedContracts = disclosedContracts.map(_.unversioned),
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
  // need to check ordering of non-catchable errors relative to other non-catchable errors.

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

    "create by interface" - {

      // TEST_EVIDENCE: Semantics: Evaluation order of successful create_interface
      "success" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
                 Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
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
            "view",
            "ends test",
          )
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create_interface with failed precondition
      "failed precondition" in {
        // Note that for LF >= 1.14 we don’t hit this as the compiler
        // generates code that throws an exception instead of returning False.
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
                 Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = False, key = M:toKey sig, nested = M:buildNested 0}
           """,
          Array(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(Left(SErrorDamlException(IE.TemplatePreconditionViolated(Human, _, _)))) =>
            msgs shouldBe Seq("starts test", "precondition")
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of create_interface with duplicate contract key
      "duplicate contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
                let c: M:Human = M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
                in ubind x : ContractId M:Human <- create @M:Human c
                  in Test:create_interface c
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

      // TEST_EVIDENCE: Semantics: Evaluation order of create_interface with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
                Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:keyNoMaintainers, nested = M:buildNested 0}
           """,
          Array(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.CreateEmptyContractKeyMaintainers(Human, _, _)))
              ) =>
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

      // TEST_EVIDENCE: Semantics: Evaluation order of create_interface with authorization failure
      "authorization failure" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
                Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
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

      // TEST_EVIDENCE: Semantics: Evaluation order of create_interface with contract ID in contract key
      "contract ID in contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) (cid : ContractId Unit) ->
                Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKeyWithCid sig cid, nested = M:buildNested 0}
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

      // TEST_EVIDENCE: Semantics: Evaluation order of create_interface with create argument exceeding max nesting
      "create argument exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
                Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 100 }
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

      // TEST_EVIDENCE: Semantics: Evaluation order of create_interface with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
              let key: M:TKey = M:TKey { maintainers = Cons @Party [sig] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 100 }
              in Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = key, nested = M:buildNested 0 }
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

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of a non-cached global contract with inconsistent key
        "inconsistent key" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(maintainer: Party) (exercisingParty: Party) (cId: ContractId M:T) ->
               ubind x : Option (ContractId M:T) <- lookup_by_key @M:T (M:toKey maintainer)
               in Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
               """,
            Array(SParty(alice), SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = PartialFunction.empty,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.InconsistentContractKey(_)))) =>
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
               ubind x: M:T <- fetch_template @M:T cId in
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
               ubind x: M:Dummy <- fetch_template @M:Dummy cId in
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
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise of cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) ->
               ubind x: M:T <- fetch_template @M:T cId
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
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
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

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise-by-key of a non-cached global contract with visibility failure
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(charlie), SParty(alice)),
            Set(charlie),
            getContract = getNonVisibleContract,
            getKey = getKey,
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.ContractKeyNotVisible(cid, key, _, _, _)))
                ) =>
              cid shouldBe cId
              key.templateId shouldBe T
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

        // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise_by_key of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) (cId: ContractId M:T) ->
               ubind x: M:T <- fetch_template @M:T cId in
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
            msgs shouldBe Seq("starts test", "maintainers")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) (sig: Party) ->
               ubind x: M:Dummy <- fetch_template @M:Dummy cId in
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
               ubind x: M:T <- fetch_template @M:T cId
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
              "choice controllers",
              "choice observers",
            )

          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise-by-key of a cached global contract with visibility failure
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty: Party) (sig : Party) (cId: ContractId M:T)  ->
              ubind x: M:T <- exercise @M:T Divulge cId exercisingParty
              in Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            Array(SParty(charlie), SParty(alice), SContractId(cId)),
            Set(charlie),
            getContract = getNonVisibleContract,
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.ContractKeyNotVisible(cid, key, _, _, _)))
                ) =>
              cid shouldBe cId
              key.templateId shouldBe T
              msgs shouldBe Seq("starts test", "maintainers")
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

        // TEST_EVIDENCE: Semantics: Evaluation order of exercise_by_key of a local contract with failure authorization
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(helperCId: ContractId Test:Helper) (sig : Party) (exercisingParty: Party) ->
             ubind x: ContractId M:T <- exercise @Test:Helper CreateNonvisibleKey helperCId ()
             in Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            Array(SContractId(helperCId), SParty(alice), SParty(charlie)),
            Set(charlie),
            getContract = getHelper,
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        ExerciseMissingAuthorization(`T`, _, _, authParties, requiredParties),
                      )
                    )
                  )
                ) =>
              authParties shouldBe Set(charlie)
              requiredParties shouldBe Set(alice)
              msgs shouldBe Seq(
                "starts test",
                "maintainers",
                "choice controllers",
                "choice observers",
              )
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

    List("exercise_interface", "exercise_interface_with_guard").foreach { testCase =>
      def buildLog(msgs: String*) = testCase match {
        case "exercise_interface" => msgs.filter(_ != "interface guard")
        case _ => msgs
      }

      testCase - {

        "a non-cached global contract" - {

          // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise by interface of a non-cached global contract
          "success" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty: Party) (cId: ContractId M:Human) -> Test:$testCase exercisingParty cId""",
              Array(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getIfaceContract,
            )
            inside(res) { case Success(Right(_)) =>
              msgs shouldBe buildLog(
                "starts test",
                "queries contract",
                "contract signatories",
                "contract observers",
                "key",
                "maintainers",
                "view",
                "interface guard",
                "choice controllers",
                "choice observers",
                "choice body",
                "ends test",
              )
            }
          }

          // TEST_EVIDENCE: Semantics: exercise_interface with a contract instance that does not implement the interface fails.
          "contract doesn't implement interface" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:Human) -> Test:$testCase exercisingParty cId""",
              Array(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getWronglyTypedContract,
              getKey = getKey,
            )
            inside(res) {
              case Success(
                    Left(SErrorDamlException(IE.ContractDoesNotImplementInterface(_, _, _)))
                  ) =>
                msgs shouldBe buildLog("starts test", "queries contract")
            }
          }

          // TEST_EVIDENCE: Semantics: Evaluation order of exercise_interface of a non-cached global contract with failed authorization
          "authorization failures" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:Human) -> Test:$testCase exercisingParty cId""",
              Array(SParty(charlie), SContractId(cId)),
              Set(charlie),
              getContract = getIfaceContract,
            )

            inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
              msgs shouldBe buildLog(
                "starts test",
                "queries contract",
                "contract signatories",
                "contract observers",
                "key",
                "maintainers",
                "view",
                "interface guard",
                "choice controllers",
                "choice observers",
              )
            }
          }
        }

        "a cached global contract" - {

          // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise_interface of a cached global contract
          "success" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId Human) ->
               ubind x: M:Human <- fetch_template @M:Human cId in
               Test:$testCase exercisingParty cId
               """,
              Array(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getIfaceContract,
            )
            inside(res) { case Success(Right(_)) =>
              msgs shouldBe buildLog(
                "starts test",
                "view",
                "interface guard",
                "choice controllers",
                "choice observers",
                "choice body",
                "ends test",
              )
            }
          }

          // TEST_EVIDENCE: Semantics: Evaluation order of exercise by interface of an inactive global contract
          "inactive contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:Human)  ->
             ubind x: Unit <- exercise @M:Human Archive cId () in
               Test:$testCase exercisingParty cId
             """,
              Array(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getIfaceContract,
            )
            inside(res) {
              case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Human, _)))) =>
                msgs shouldBe buildLog("starts test")
            }
          }

          // TEST_EVIDENCE: Semantics: Evaluation order of exercise by interface of a cached global contract that does not implement the interface.
          "wrongly typed contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:Human) ->
               ubind x: M:Dummy <- fetch_template @M:Dummy cId in
               Test:$testCase exercisingParty cId
               """,
              Array(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getWronglyTypedContract,
            )
            inside(res) {
              case Success(
                    Left(
                      SErrorDamlException(IE.ContractDoesNotImplementInterface(Person, _, Dummy))
                    )
                  ) =>
                msgs shouldBe buildLog("starts test")
            }
          }

          // TEST_EVIDENCE: Semantics: This checks that type checking is done after checking activeness.
          "wrongly typed inactive contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:T) ->
               ubind x: M:Dummy <- exercise @M:Dummy Archive cId () in
               Test:$testCase exercisingParty cId
               """,
              Array(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getWronglyTypedContract,
            )
            inside(res) {
              case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
                msgs shouldBe buildLog("starts test")
            }
          }

          // TEST_EVIDENCE: Semantics: Evaluation order of exercise by interface of cached global contract with failed authorization
          "authorization failures" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:Human) ->
               ubind x: M:Human <- fetch_template @M:Human cId
               in  Test:$testCase exercisingParty cId""",
              Array(SParty(bob), SContractId(cId)),
              Set(bob),
              getContract = getIfaceContract,
            )

            inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
              msgs shouldBe buildLog(
                "starts test",
                "view",
                "interface guard",
                "choice controllers",
                "choice observers",
              )
            }
          }
        }

        "a local contract" - {

          // TEST_EVIDENCE: Semantics: Evaluation order of successful exercise_interface of a local contract
          "success" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) ->
             ubind cId: ContractId M:Human <- create @M:Human M:Human {person = exercisingParty, ob = exercisingParty, ctrl = exercisingParty, precond = True, key = M:toKey exercisingParty, nested = M:buildNested 0} in
             Test:$testCase exercisingParty cId
             """,
              Array(SParty(alice)),
              Set(alice),
            )
            inside(res) { case Success(Right(_)) =>
              msgs shouldBe buildLog(
                "starts test",
                "view",
                "interface guard",
                "choice controllers",
                "choice observers",
                "choice body",
                "ends test",
              )
            }
          }

          // TEST_EVIDENCE: Semantics: Evaluation order of exercise_interface of an inactive local contract
          "inactive contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) ->
             ubind cId: ContractId M:Human <- create @M:Human M:Human {person = exercisingParty, ob = exercisingParty, ctrl = exercisingParty, precond = True, key = M:toKey exercisingParty, nested = M:buildNested 0} in
             ubind x: Unit <- exercise @M:Human Archive cId ()
             in
             Test:$testCase exercisingParty cId
             """,
              Array(SParty(alice)),
              Set(alice),
            )
            inside(res) {
              case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Human, _)))) =>
                msgs shouldBe Seq("starts test")
            }
          }

          // TEST_EVIDENCE: Semantics: Evaluation order of exercise_interface of an local contract not implementing the interface
          "wrongly typed contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) ->
             ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = exercisingParty }
             in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
             in
               Test:$testCase exercisingParty cId1
             """,
              Array(SParty(alice)),
              Set(alice),
            )
            inside(res) {
              case Success(
                    Left(
                      SErrorDamlException(IE.ContractDoesNotImplementInterface(Person, _, Dummy))
                    )
                  ) =>
                msgs shouldBe buildLog("starts test")
            }
          }

          // TEST_EVIDENCE: Semantics: This checks that type checking in exercise_interface is done after checking activeness.
          "wrongly typed inactive contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) ->
             ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = exercisingParty}
             in ubind x: Unit <- exercise @M:Dummy Archive cId1 ()
             in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
             in
               Test:$testCase exercisingParty cId1
             """,
              Array(SParty(alice)),
              Set(alice),
            )
            inside(res) {
              case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
                msgs shouldBe buildLog("starts test")
            }
          }

          // TEST_EVIDENCE: Semantics: Evaluation order of exercise_interface of a cached local contract with failed authorization
          "authorization failures" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (other : Party)->
                  ubind cId: ContractId M:Human <- create @M:Human M:Human {person = exercisingParty, ob = other, ctrl = other, precond = True, key = M:toKey exercisingParty, nested = M:buildNested 0} in
                    Test:$testCase exercisingParty cId
                  """,
              Array(SParty(alice), SParty(bob)),
              Set(alice),
            )

            inside(res) {
              case Success(
                    Left(
                      SErrorDamlException(
                        IE.FailedAuthorization(
                          _,
                          FailedAuthorization.ExerciseMissingAuthorization(Human, _, None, _, _),
                        )
                      )
                    )
                  ) =>
                msgs shouldBe buildLog(
                  "starts test",
                  "view",
                  "interface guard",
                  "choice controllers",
                  "choice observers",
                )
            }
          }
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

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of a non-cached global contract with inconsistent key
        "inconsistent key" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(maintainer: Party) (fetchingParty: Party) (cId: ContractId M:T) ->
               ubind x : Option (ContractId M:T) <- lookup_by_key @M:T (M:toKey maintainer)
               in Test:fetch_by_id fetchingParty cId
               """,
            Array(SParty(alice), SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = PartialFunction.empty,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.InconsistentContractKey(_)))) =>
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
               ubind x: M:T <- fetch_template @M:T cId in
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
               ubind x: M:Dummy <- fetch_template @M:Dummy cId
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
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T) ->
               ubind x: M:T <- fetch_template @M:T cId
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
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
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

      "a disclosed contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch of a wrongly typed disclosed contract
        "wrongly typed contract" in {
          val (result, events) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (fetchingParty: Party) (cId1: ContractId M:Dummy) ->
             let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
             in Test:fetch_by_id fetchingParty cId2""",
            Array(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            disclosedContracts = ImmArray(buildDisclosedContract(alice)),
          )

          inside(result) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(`cId`, T, Dummy)))) =>
              events shouldBe Seq("starts test")
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

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch-by-key of a non-cached global contract with visibility failure
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) -> Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice)),
            Set(charlie),
            getContract = getNonVisibleContract,
            getKey = getKey,
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.ContractKeyNotVisible(cid, key, _, _, _)))
                ) =>
              cid shouldBe cId
              key.templateId shouldBe T
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
                 ubind x: M:T <- fetch_template @M:T cId
                 in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "maintainers", "ends test")
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
            msgs shouldBe Seq("starts test", "maintainers")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_by_key of a cached global contract with authorization failure
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) (cId: ContractId M:T) ->
               ubind x: M:T <- fetch_template @M:T cId
               in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test", "maintainers")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch-by-key of a cached global contract with visibility failure
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig : Party)  (cId: ContractId M:T)  ->
               ubind x: M:T <- exercise @M:T Divulge cId fetchingParty
               in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice), SContractId(cId)),
            Set(charlie),
            getContract = getNonVisibleContract,
            getKey = getKey,
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.ContractKeyNotVisible(cid, key, _, _, _)))
                ) =>
              cid shouldBe cId
              key.templateId shouldBe T
              msgs shouldBe Seq("starts test", "maintainers")
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
            e"""\(helperCId: ContractId Test:Helper) (sig : Party) (fetchingParty: Party) ->
             ubind x: ContractId M:T <- exercise @Test:Helper CreateNonvisibleKey helperCId ()
             in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            Array(SContractId(helperCId), SParty(alice), SParty(charlie)),
            Set(charlie),
            getContract = getHelper,
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        FetchMissingAuthorization(`T`, _, stakeholders, authParties),
                      )
                    )
                  )
                ) =>
              stakeholders shouldBe Set(alice)
              authParties shouldBe Set(charlie)
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

    "fetch_interface" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch_interface of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_interface""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getIfaceContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq(
              "starts test",
              "queries contract",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "view",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_interface of a non-cached global contract that doesn't implement interface.
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_interface""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.ContractDoesNotImplementInterface(Person, _, Dummy)))
                ) =>
              msgs shouldBe Seq("starts test", "queries contract")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_interface of a non-cached global contract with failed authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_interface""",
            Array(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getIfaceContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq(
              "starts test",
              "queries contract",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "view",
            )
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch_interface of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:Person) ->
               ubind x: M:Person <- fetch_interface @M:Person cId in
               Test:fetch_interface fetchingParty cId
               """,
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getIfaceContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "view", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_interface of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:Person)  ->
             ubind x: Unit <- exercise @M:Human Archive cId ()
             in Test:fetch_interface fetchingParty cId""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getIfaceContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Human, _)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_interface of a cached global contract not implementing the interface.
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:Person) ->
               ubind x: M:Dummy <- fetch_template @M:Dummy cId
               in Test:fetch_interface fetchingParty cId""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.ContractDoesNotImplementInterface(Person, _, Dummy)))
                ) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:Human) ->
               ubind x: M:Dummy <- exercise @M:Dummy Archive cId ()
               in Test:fetch_interface fetchingParty cId""",
            Array(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_interface of cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:Person) ->
               ubind x: M:Person <- fetch_interface @M:Person cId
               in Test:fetch_interface fetchingParty cId""",
            Array(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getIfaceContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test", "view")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Semantics: Evaluation order of successful fetch_interface of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (fetchingParty: Party) ->
             ubind cId: ContractId M:Human <- create @M:Human M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
             in Test:fetch_interface fetchingParty cId""",
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "view", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_interface of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (fetchingParty: Party) ->
             ubind cId: ContractId M:Human <- create @M:Human M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
             in ubind x: Unit <- exercise @M:Human Archive cId ()
             in Test:fetch_interface fetchingParty cId""",
            Array(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Human, _)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_interface of an local contract not implementing the interface
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (fetchingParty: Party) ->
             ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig }
             in let cId2: ContractId M:Human = COERCE_CONTRACT_ID @M:Dummy @M:Human cId1
             in Test:fetch_interface fetchingParty cId2""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.ContractDoesNotImplementInterface(Person, _, Dummy)))
                ) =>
              msgs shouldBe Seq("starts test")
          }
        }
        // TEST_EVIDENCE: Semantics: This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (fetchingParty: Party) ->
             ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig }
             in ubind x: Unit <- exercise @M:Dummy Archive cId1 ()
             in let cId2: ContractId M:Human = COERCE_CONTRACT_ID @M:Dummy @M:Human cId1
             in Test:fetch_interface fetchingParty cId2""",
            Array(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe Seq("starts test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of fetch_interface of a cached global contract with failure authorization
        "authorization failures" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (fetchingParty: Party) ->
                  ubind cId: ContractId M:Human <- create @M:Human M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
                  in Test:fetch_interface fetchingParty cId""",
            Array(SParty(alice), SParty(bob), SParty(charlie)),
            Set(alice, charlie),
            getContract = getIfaceContract,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test", "view")
          }
        }
      }

      // TEST_EVIDENCE: Semantics: Evaluation order of fetch_interface of an unknown contract
      "unknown contract" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty: Party) (cId: ContractId M:Person) -> Test:fetch_interface fetchingParty cId""",
          Array(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = PartialFunction.empty,
        )
        inside(res) { case Failure(SpeedyTestLib.UnknownContract(`cId`)) =>
          msgs shouldBe Seq("starts test", "queries contract")
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
              "queries contract",
              "contract signatories",
              "contract observers",
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

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup of a non-cached global contract with visibility failure
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) -> Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice)),
            Set(charlie),
            getContract = getNonVisibleContract,
            getKey = getKey,
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.ContractKeyNotVisible(cid, key, _, _, _)))
                ) =>
              cid shouldBe cId
              key.templateId shouldBe T
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

        // TEST_EVIDENCE: Semantics: Evaluation order of successful lookup_by_key of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) (cId: ContractId M:T) ->
                 ubind x: M:T <- fetch_template @M:T cId
                 in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe Seq("starts test", "maintainers", "ends test")
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
            msgs shouldBe Seq("starts test", "maintainers", "ends test")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup_by_key of a cached global contract with authorization failure
        "authorization failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) (cId: ContractId M:T) ->
               ubind x: M:T <- fetch_template @M:T cId
               in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKey = getKey,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.FailedAuthorization(_, _)))) =>
            msgs shouldBe Seq("starts test", "maintainers")
          }
        }

        // TEST_EVIDENCE: Semantics: Evaluation order of lookup of a cached global contract with visibility failure
        "visibility failure" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) (cId: ContractId M:T) ->
               ubind x: M:T <- exercise @M:T Divulge cId lookingParty
               in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SParty(charlie), SParty(alice), SContractId(cId)),
            Set(charlie),
            getContract = getNonVisibleContract,
            getKey = getKey,
          )
          inside(res) {
            case Success(
                  Left(SErrorDamlException(IE.ContractKeyNotVisible(cid, key, _, _, _)))
                ) =>
              cid shouldBe cId
              key.templateId shouldBe T
              msgs shouldBe Seq("starts test", "maintainers")
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
            e"""\(helperCId: ContractId Test:Helper) (sig : Party) (lookingParty: Party) ->
             ubind x: ContractId M:T <- exercise @Test:Helper CreateNonvisibleKey helperCId ()
             in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            Array(SContractId(helperCId), SParty(alice), SParty(charlie)),
            Set(charlie),
            getContract = getHelper,
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        LookupByKeyMissingAuthorization(`T`, _, maintainers, authParties),
                      )
                    )
                  )
                ) =>
              maintainers shouldBe Set(alice)
              authParties shouldBe Set(charlie)
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
