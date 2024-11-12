// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.crypto.Hash
import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageDevConfig.{LeftToRight, RightToLeft}
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion, StablePackages}
import com.daml.lf.speedy.SError.{SError, SErrorDamlException}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult.{SResultError, SResultFinal}
import com.daml.lf.speedy.SValue.{SContractId, SParty, SUnit}
import com.daml.lf.speedy.SpeedyTestLib.typeAndCompile
import com.daml.lf.testing.parser
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ValueRecord, ValueText}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class ExceptionTestV1 extends ExceptionTest(LanguageMajorVersion.V1)
//class ExceptionTestV2 extends ExceptionTest(LanguageMajorVersion.V2)

// TEST_EVIDENCE: Integrity: Exceptions, throw/catch.
class ExceptionTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Inside
    with Matchers
    with TableDrivenPropertyChecks {

  import SpeedyTestLib.loggingContext

  implicit val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)
  val defaultPackageId = defaultParserParameters.defaultPackageId

  private def applyToParty(pkgs: CompiledPackages, e: Expr, p: Party): SExpr = {
    val se = pkgs.compiler.unsafeCompile(e)
    SEApp(se, Array(SParty(p)))
  }

  private val alice = Party.assertFromString("Alice")

  private def runUpdateExpr(
      compiledPackages: PureCompiledPackages,
      expr: Expr,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = Map.empty,
  ): Either[SError, SValue] = {
    runUpdateExpr(
      compiledPackages,
      Map.empty,
      compiledPackages.compiler.unsafeCompile(expr),
      PartialFunction.empty,
      getKey,
      Map.empty,
    )
  }

  private def runUpdateApp(
      compiledPackages: PureCompiledPackages,
      packageResolution: Map[Ref.PackageName, Ref.PackageId],
      expr: Expr,
      args: Array[SValue],
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance],
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId],
      disclosures: Iterable[(Value.ContractId, Speedy.ContractInfo)],
  ): Either[SError, SValue] = {
    runUpdateExpr(
      compiledPackages,
      packageResolution,
      SEApp(compiledPackages.compiler.unsafeCompile(expr), args),
      getContract,
      getKey,
      disclosures,
    )
  }

  private def runUpdateExpr(
      compiledPackages: PureCompiledPackages,
      packageResolution: Map[Ref.PackageName, Ref.PackageId],
      sexpr: SExpr,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance],
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId],
      disclosures: Iterable[(Value.ContractId, Speedy.ContractInfo)],
  ): Either[SError, SValue] = {
    val machine = Speedy.Machine
      .fromUpdateSExpr(
        compiledPackages = compiledPackages,
        packageResolution = packageResolution,
        transactionSeed = crypto.Hash.hashPrivateKey("ExceptionTest.scala"),
        updateSE = sexpr,
        committers = Set(alice),
      )
    disclosures.foreach { case (coid, info) =>
      machine.addDisclosedContracts(coid, info)
    }
    SpeedyTestLib
      .run(machine, getContract = getContract, getKey = getKey)
  }

  "unhandled throw" - {

    // Behaviour when no handler catches a throw:
    // 1: User Exception thrown; no try-catch in scope
    // 2. User Exception thrown; try catch in scope, but handler does not catch
    // 3. User Exception thrown; no handler in scope
    // 4. User Exception thrown; no handler in scope; secondary throw from the message function of the 1st exception

    val pkgs: PureCompiledPackages = typeAndCompile(p""" metadata ( 'pkg' : '1.0.0' )

   module M {

     val unhandled1 : Update Int64 =
        upure @Int64 (ADD_INT64 10 (throw @Int64 @M:E1 (M:E1 {})));

     val unhandled2 : Update Int64 =
        try @Int64 (upure @Int64 (ADD_INT64 10 (throw @Int64 @M:E1 (M:E1 {}))))
        catch e -> None @(Update Int64);

     record @serializable E1 = { } ;
     exception E1 = { message \(e: M:E1) -> "E1" };
     val unhandled3 : Update Int64 = upure @Int64 (throw @Int64 @M:E1 (M:E1 {})) ;

     record @serializable E2 = { } ;
     exception E2 = { message \(e: M:E2) -> throw @Text @M:E1 (M:E1 {}) } ; //throw from the message function
     val unhandled4 : Update Int64 = upure @Int64 (throw @Int64 @M:E2 (M:E2 {})) ;

     val divZero : Update Int64 = upure @Int64 (DIV_INT64 1 0) ;
   }
  """)

    val List((t1, e1), (t2, e2)) =
      List("M:E1", "M:E2")
        .map(id => data.Ref.Identifier.assertFromString(s"$defaultPackageId:$id"))
        .map(tyCon => TTyCon(tyCon) -> ValueRecord(Some(tyCon), data.ImmArray.Empty))
    val divZeroE =
      ValueRecord(
        Some(StablePackages(majorLanguageVersion).ArithmeticError),
        data.ImmArray(
          Some(data.Ref.Name.assertFromString("message")) ->
            ValueText("ArithmeticError while evaluating (DIV_INT64 1 0).")
        ),
      )

    val testCases = Table[String, SError](
      ("expression", "expected"),
      ("M:unhandled1", SErrorDamlException(IE.UnhandledException(t1, e1))),
      ("M:unhandled2", SErrorDamlException(IE.UnhandledException(t1, e1))),
      ("M:unhandled3", SErrorDamlException(IE.UnhandledException(t1, e1))),
      ("M:unhandled4", SErrorDamlException(IE.UnhandledException(t2, e2))),
      (
        "M:divZero",
        SErrorDamlException(
          IE.UnhandledException(
            TTyCon(StablePackages(majorLanguageVersion).ArithmeticError),
            divZeroE,
          )
        ),
      ),
    )

    forEvery(testCases) { (exp: String, expected: SError) =>
      s"eval[$exp] --> $expected" in {
        inside(runUpdateExpr(pkgs, e"$exp")) { case Left(err) =>
          err shouldBe expected
        }
      }
    }
  }

  "throw/catch (UserDefined)" - {

    // Basic throw/catch example a user defined exception

    val pkgs: PureCompiledPackages = typeAndCompile(p""" metadata ( 'pkg' : '1.0.0' )

   module M {

     record @serializable MyException = { message: Text } ;

     exception MyException = {
       message \(e: M:MyException) -> M:MyException {message} e
     };

     val throwAndCatch : Update Int64 =
       try @Int64 (upure @Int64 (throw @Int64 @M:MyException (M:MyException {message = "oops"})))
       catch e -> Some @(Update Int64) (upure @Int64 77);

   }
  """)

    val testCases = Table[String, Long](
      ("expression", "expected"),
      ("M:throwAndCatch", 77),
    )

    forEvery(testCases) { (exp: String, num: Long) =>
      s"eval[$exp] --> $num" in {
        inside(runUpdateExpr(pkgs, e"$exp")) { case Right(v) =>
          v shouldBe SValue.SInt64(num)
        }
      }
    }
  }

  "throw/catch (UserDefined, with integer payload)" - {

    // Throw/catch example of a user defined exception, passing an integer payload.

    val pkgs: PureCompiledPackages = typeAndCompile(p""" metadata ( 'pkg' : '1.0.0' )

   module M {

     record @serializable MyException = { message: Text, payload : Int64 } ;

     exception MyException = {
       message \(e: M:MyException) -> M:MyException {message} e
     };

     val throwAndCatch : Update Int64 =
         try @Int64 (upure @Int64 (throw @Int64 @M:MyException (M:MyException {message = "oops", payload = 77})))
         catch e ->
           case (from_any_exception @M:MyException e) of
              None -> Some @(Update Int64) (upure @Int64 999)
            | Some my -> Some @(Update Int64) (upure @Int64 (M:MyException {payload} my))
         ;
   }
  """)

    val testCases = Table[String, Long](
      ("expression", "expected"),
      ("M:throwAndCatch", 77),
    )

    forEvery(testCases) { (exp: String, num: Long) =>
      s"eval[$exp] --> $num" in {
        inside(runUpdateExpr(pkgs, e"$exp")) { case Right(v) =>
          v shouldBe SValue.SInt64(num)
        }
      }
    }
  }

  "selective exception handling" - {

    // Example which nests two try-catch block around an expression which throws:
    // 3 variants: (with one source line changed,marked)
    // -- The inner handler catches (some)
    // -- The inner handler does not catch (none); allowing the outer handler to catch
    // -- The inner handler throws while deciding whether to catch

    val pkgs: PureCompiledPackages = typeAndCompile(p""" metadata ( 'pkg' : '1.0.0' )

  module M {
    record @serializable MyException = { message: Text } ;

    exception MyException = {
      message \(e: M:MyException) -> M:MyException {message} e
    };

    val innerCatch : Update Int64 =
        ubind x: Int64 <-
            try @Int64
                ubind x: Int64 <-
                    try @Int64
                        upure @Int64 (throw @Int64 @M:MyException (M:MyException {message = "oops"}))
                    catch e ->
                        Some @(Update Int64) (upure @Int64 77)
                in
                    upure @Int64 (ADD_INT64 200 x)
            catch e ->
                Some @(Update Int64) (upure @Int64 88)
        in
            upure @Int64 (ADD_INT64 100 x)
     ;

    val outerCatch : Update Int64 =
        ubind x: Int64 <-
            try @Int64
                ubind x: Int64 <-
                    try @Int64
                        upure @Int64 (throw @Int64 @M:MyException (M:MyException {message = "oops"}))
                    catch e ->
                        None @(Update Int64) // THIS IS THE ONLY LINE CHANGED
                in
                    upure @Int64 (ADD_INT64 200 x)
            catch e ->
                Some @(Update Int64) (upure @Int64 88)
        in
            upure @Int64 (ADD_INT64 100 x)
     ;

    val throwWhileInnerHandlerDecides : Update Int64 =
        ubind x: Int64 <-
            try @Int64
                ubind x: Int64 <-
                    try @Int64
                        upure @Int64 (throw @Int64 @M:MyException (M:MyException {message = "oops"}))
                    catch e ->
                        throw @(Option (Update Int64)) @M:MyException (M:MyException {message = "oops"}) //CHANGE
                in
                    upure @Int64 (ADD_INT64 200 x)
            catch e ->
                Some @(Update Int64) (upure @Int64 88)
        in
            upure @Int64 (ADD_INT64 100 x)
     ;

   }
  """)

    val testCases = Table[String, Long](
      ("expression", "expected"),
      ("M:innerCatch", 377),
      ("M:outerCatch", 188),
      ("M:throwWhileInnerHandlerDecides", 188),
    )

    forEvery(testCases) { (exp: String, num: Long) =>
      s"eval[$exp] --> $num" in {
        inside(runUpdateExpr(pkgs, e"$exp")) { case Right(v) =>
          v shouldBe SValue.SInt64(num)
        }
      }
    }
  }

  "selective exception handling (using payload)" - {

    // Selective handling, where either an inner or outer handler may access the exception payload:
    // 3 variantions, this time seleced dynamically using a pair of bool
    //
    // 1. True/False  -- The inner handler catches (some)
    // 2. False/False -- The inner handler does not catch (none); allowing the outer handler to catch
    // 3. False/True  -- The inner handler throws while deciding whether to catch
    //
    // The test result is an integer, composed by adding components demonstrating the control flow taken
    // 1000 -- exception payload or original throw
    // 2000 -- exception payload of secondary throw (variation-3)
    // 77 -- inner handler catches
    // 88 -- outer handler catches
    // 200 -- normal controlflow following inner catch
    // 100 -- normal controlflow following outer catch

    val pkgs: PureCompiledPackages = typeAndCompile(p""" metadata ( 'pkg' : '1.0.0' )

   module M {
    record @serializable MyException = { payload: Int64 } ;

    exception MyException = {
      message \(e: M:MyException) -> "no-message"
    };

    val maybeInnerCatch : Bool -> Bool -> Update Int64 = \(catchAtInner: Bool) (throwAtInner: Bool) ->
      ubind x: Int64 <-
        try @Int64
          ubind x: Int64 <-
            try @Int64
              upure @Int64 (throw @Int64 @M:MyException (M:MyException {payload = 1000}))
            catch e ->
              case catchAtInner of
                True ->
                  (case (from_any_exception @M:MyException e) of
                    None -> None @(Update Int64)
                  | Some my -> Some @(Update Int64) (upure @Int64 (ADD_INT64 77 (M:MyException {payload} my))))
              | False ->
                  (case throwAtInner of
                     True -> throw @(Option (Update Int64)) @M:MyException (M:MyException {payload = 2000})
                   | False -> None @(Update Int64))
          in
            upure @Int64 (ADD_INT64 200 x)
        catch e ->
          case (from_any_exception @M:MyException e) of
            None -> None @(Update Int64)
          | Some my -> Some @(Update Int64) (upure @Int64 (ADD_INT64 88 (M:MyException {payload} my)))

      in
        upure @Int64 (ADD_INT64 100 x) ;
   }
  """)

    val testCases = Table[String, Long](
      ("expression", "expected"),
      ("M:maybeInnerCatch True False", 1377),
      ("M:maybeInnerCatch False False", 1188),
      ("M:maybeInnerCatch False True", 2188),
    )

    forEvery(testCases) { (exp: String, num: Long) =>
      s"eval[$exp] --> $num" in {
        inside(runUpdateExpr(pkgs, e"$exp")) { case Right(v) =>
          v shouldBe SValue.SInt64(num)
        }
      }
    }
  }

  "throw/catch (control flow)" - {

    // Another example allowing dynamic control of different test paths. Novelty here:
    // - uses GeneralError, with Text payload encoding an int
    // - variations 1..4 selected by an integer arg
    // - final result contains elements which demonstrate the control flow taken

    val pkgs: PureCompiledPackages = typeAndCompile(p""" metadata ( 'pkg' : '1.0.0' )

   module M {

     record @serializable E = { message: Text } ;
     exception E = {
       message \(e: M:E) -> M:E {message} e
     };

    val myThrow : forall (a: *). (Text -> a) =
      /\ (a: *). \(mes : Text) ->
        throw @a @M:E (M:E { message = mes });

    val isPayLoad : AnyException -> Text -> Bool =
      \(e: AnyException) (mes: Text) ->
        EQUAL @AnyException e (to_any_exception @M:E (M:E { message = mes })) ;

    val extractPayload : AnyException -> Int64 =
      \(e: AnyException) ->
        case (M:isPayLoad e "payload2") of True -> 2 | False ->
        case (M:isPayLoad e "payload3") of True -> 3 | False ->
        1000000 ;

    val myCatch : forall (a: *). (Int64 -> a) -> (Text -> Update a) -> Update a =
      /\ (a: *). \ (handler: Int64 -> a) (body: Text -> Update a) ->
        try @a
          (body "body-unitish")
        catch e ->
          Some @(Update a) (upure @a (handler (M:extractPayload e))) ;

    val throwCatchTest : (Int64 -> Update Int64) = \ (x: Int64) ->
      ubind x: Int64 <-
        M:myCatch @Int64 (\(pay : Int64) -> ADD_INT64 100 pay) (\(u : Text) ->
          ubind x: Int64 <-
            M:myCatch @Int64 (\(pay : Int64) -> ADD_INT64 200 pay) (\(u : Text) ->
              upure @Int64
                (ADD_INT64 4000
                  (case (EQUAL @Int64 x 1) of True -> x | False ->
                   case (EQUAL @Int64 x 2) of True -> M:myThrow @Int64 "payload2" | False ->
                   case (EQUAL @Int64 x 3) of True -> M:myThrow @Int64 "payload3" | False ->
                   case (EQUAL @Int64 x 4) of True -> x | False ->
                   2000000)))
          in
            upure @Int64 (ADD_INT64 2000 x))
      in
        upure @Int64 (ADD_INT64 1000 x) ;

  }""")

    val testCases = Table[String, Long](
      ("expression", "expected"),
      ("M:throwCatchTest 1", 7001),
      ("M:throwCatchTest 2", 3202),
      ("M:throwCatchTest 3", 3203),
      ("M:throwCatchTest 4", 7004),
    )

    forEvery(testCases) { (exp: String, num: Long) =>
      s"eval[$exp] --> $num" in {
        inside(runUpdateExpr(pkgs, e"$exp")) { case Right(v) =>
          v shouldBe SValue.SInt64(num)
        }
      }
    }
  }

  "throws from various places" - {

    // Example define a common "wrap" function which handles just a specific user exception:
    // Series of tests which may throw within its context:
    //
    // - example1 -- no thow
    // - example2 -- throw handled exception
    // - example3 -- throw unhandled exception
    // - example4 -- throw handled exception on left & right of a binary op
    // - example5 -- throw handled exception which computing the payload of an outer throw

    val pkgs: PureCompiledPackages = typeAndCompile(p""" metadata ( 'pkg' : '1.0.0' )

   module M {

    record @serializable MyExceptionH = { message: Text } ;

    // H,U -- handled/unhandled user exceptions

    exception MyExceptionH = {
      message \(e: M:MyExceptionH) -> M:MyExceptionH {message} e
    } ;

    record @serializable MyExceptionU = { message: Text } ;

    exception MyExceptionU = {
      message \(e: M:MyExceptionU) -> M:MyExceptionU {message} e
    } ;

    val wrap : (Unit -> Text) -> Update Text = \(body: Unit -> Text) ->
      try @Text (upure @Text (APPEND_TEXT "RESULT: " (body ())))
      catch e ->
        Some @(Update Text)
          upure @Text
            case (from_any_exception @M:MyExceptionH e) of
              None -> "UNHANDLED"
            | Some my -> APPEND_TEXT "HANDLED: " (M:MyExceptionH {message} my) ;

    val example1 : Update Text =
      M:wrap (\(u: Unit) ->
        "Happy Path") ;

    val example2 : Update Text =
      M:wrap (\(u: Unit) ->
        throw @Text @M:MyExceptionH (M:MyExceptionH {message = "oops1"})) ;

    val example3 : Update Text =
      M:wrap (\(u: Unit) ->
        throw @Text @M:MyExceptionU (M:MyExceptionU {message = "oops2"})) ;

    val example4 : Update Text =
      M:wrap (\(u: Unit) ->
        APPEND_TEXT
          (throw @Text @M:MyExceptionH (M:MyExceptionH {message = "left"}))
          (throw @Text @M:MyExceptionH (M:MyExceptionH {message = "right"}))
      );

    val example5 : Update Text =
      M:wrap (\(u: Unit) ->
        throw @Text @M:MyExceptionH (throw @M:MyExceptionH @M:MyExceptionH (M:MyExceptionH {message = "throw-in-throw"}))
      );

  } """)

    val testCases = Table[String, String](
      ("expression", "expected"),
      ("M:example1", "RESULT: Happy Path"),
      ("M:example2", "HANDLED: oops1"),
      ("M:example3", "UNHANDLED"),
      (
        "M:example4",
        majorLanguageVersion.evaluationOrder match {
          case LeftToRight => "HANDLED: left"
          case RightToLeft => "HANDLED: right"
        },
      ),
      ("M:example5", "HANDLED: throw-in-throw"),
    )

    forEvery(testCases) { (exp: String, str: String) =>
      s"eval[$exp] --> $str" in {
        inside(runUpdateExpr(pkgs, e"$exp")) { case Right(v) =>
          v shouldBe SValue.SText(str)
        }
      }
    }
  }

  // TODO https://github.com/digital-asset/daml/issues/12821
  //  add tests for interface
  "uncatchable exceptions" - {
    "not be caught" in {

      val pkgs: PureCompiledPackages = typeAndCompile(p""" metadata ( 'pkg' : '1.0.0' )

   module M {

     record @serializable MyUnit = {};

     record @serializable E = { } ;
     exception E = { message \(e: M:E) -> "E" };

     record @serializable T = { party: Party, viewFails: Bool };

     interface (this: I) = {
       viewtype M:MyUnit;
       method parties: List Party;
       choice Noop (self) (u: Unit) : Unit,
         controllers (call_method @M:I parties this),
         observers Nil @Party
         to upure @Unit ();
       choice BodyCrash (self) (u: Unit) : Unit,
         controllers (call_method @M:I parties this),
         observers Nil @Party
         to upure @Unit (throw @Unit @M:E (M:E {}));
       choice ControllersCrash (self) (u: Unit) : Unit,
         controllers throw @(List Party) @M:E (M:E {}),
         observers Nil @Party
         to upure @Unit ();
       choice ObserversCrash (self) (u: Unit) : Unit,
         controllers (call_method @M:I parties this),
         observers throw @(List Party) @M:E (M:E {})
         to upure @Unit ();
     };

     template (this: T) = {
       precondition True;
       signatories Cons @Party [M:T {party} this] Nil @Party;
       observers Nil @Party;
       agreement "Agreement";
       choice BodyCrash (self) (u: Unit) : Unit,
         controllers Cons @Party [M:T {party} this] Nil @Party,
         observers Nil @Party
         to upure @Unit (throw @Unit @M:E (M:E {}));
       choice ControllersCrash (self) (u: Unit) : Unit,
         controllers throw @(List Party) @M:E (M:E {}),
         observers Nil @Party
         to upure @Unit ();
       choice ObserversCrash (self) (u: Unit) : Unit,
         controllers Cons @Party [M:T {party} this] Nil @Party,
         observers throw @(List Party) @M:E (M:E {})
         to upure @Unit ();
       implements M:I {
         view = case (M:T {viewFails} this) of
             False -> M:MyUnit {}
           | True -> throw @M:MyUnit @M:E (M:E {});
          method parties = Cons @Party [M:T {party} this] Nil @Party;
       };
     };
   }
  """)

      val transactionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("transactionSeed")

      val testCases = Table[String, String](
        ("description", "update"),
        "exception thrown by the evaluation of the choice body during exercise by template can be caught" ->
          """
        ubind
          cid : ContractId M:T <- create @M:T (M:T {party = sig, viewFails = False})
        in exercise @M:T BodyCrash cid ()
    """,
        "exception thrown by the evaluation of the choice controllers during exercise by template cannot be caught" ->
          """
        ubind
          cid : ContractId M:T <- create @M:T (M:T {party = sig, viewFails = False})
        in exercise @M:T ControllersCrash cid ()
    """,
        "exception thrown by the evaluation of the choice observers during exercise by template cannot be caught" ->
          """
        ubind
          cid : ContractId M:T <- create @M:T (M:T {party = sig, viewFails = False})
        in exercise @M:T ObserversCrash cid ()
    """,
        "exception thrown by the evaluation of the view during create by interface cannot be caught" ->
          """
        ubind
          cid : ContractId M:I <- create_by_interface @M:I (to_interface @M:I @M:T (M:T {party = sig, viewFails = True}))
        in ()
    """,
        "exception thrown by the evaluation of the view during fetch by interface cannot be caught" ->
          """
        ubind
          cid : ContractId M:T <- create @M:T (M:T {party = sig, viewFails = True});
          i: M:I <- fetch_interface @M:I (COERCE_CONTRACT_ID @M:T @M:I cid)
        in ()
    """,
        "exception thrown by the evaluation of the choice body during exercise by interface can be caught" ->
          """
        ubind
          cid : ContractId M:T <- create @M:T (M:T {party = sig, viewFails = False})
        in exercise_interface @M:I BodyCrash (COERCE_CONTRACT_ID @M:T @M:I cid) ()
    """,
        "exception thrown by the evaluation of the choice controllers during exercise by interface cannot be caught" ->
          """
        ubind
          cid : ContractId M:T <- create @M:T (M:T {party = sig, viewFails = False})
        in exercise_interface @M:I ControllersCrash (COERCE_CONTRACT_ID @M:T @M:I cid) ()
    """,
        "exception thrown by the evaluation of the choice observers during exercise by interface cannot be caught" ->
          """
        ubind
          cid : ContractId M:T <- create @M:T (M:T {party = sig, viewFails = False})
        in exercise_interface @M:I ObserversCrash (COERCE_CONTRACT_ID @M:T @M:I cid) ()
    """,
      )

      forEvery(testCases) { (description, update) =>
        val expr =
          e"""\(sig: Party) ->
          try @Unit ($update)
          catch e -> Some @(Update Unit) (upure @Unit ())
          """

        val res = Speedy.Machine
          .fromUpdateSExpr(pkgs, transactionSeed, applyToParty(pkgs, expr, alice), Set(alice))
          .run()
        if (description.contains("can be caught"))
          inside(res) { case SResultFinal(SUnit) =>
          }
        else if (description.contains("cannot be caught"))
          inside(res) { case SResultError(SErrorDamlException(err)) =>
            err shouldBe a[IE.UnhandledException]
          }
        else
          sys.error("the description should contains \"can be caught\" or \"cannot be caught\"")
      }
    }
  }

  "rollback of creates" - {

    val party = Party.assertFromString("Alice")
    val example: Expr = e"M:causeRollback"
    val transactionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("transactionSeed")

    "works as expected for a contract version POST-dating exceptions" - {

      val pkgs = mkPackagesAtVersion(majorLanguageVersion.dev)
      val res = Speedy.Machine
        .fromUpdateSExpr(
          pkgs,
          transactionSeed,
          applyToParty(pkgs, example, party),
          Set(party),
        )
        .run()
      inside(res) { case SResultFinal(SUnit) =>
      }
    }

    def mkPackagesAtVersion(languageVersion: LanguageVersion): PureCompiledPackages = {

      val parserParameters: parser.ParserParameters[this.type] =
        parser.ParserParameters(defaultPackageId, languageVersion)

      typeAndCompile(p""" metadata ( 'pkg' : '1.0.0' )

   module M {

    record @serializable AnException = { } ;
    exception AnException = { message \(e: M:AnException) -> "AnException" };

    record @serializable T1 = { party: Party, info: Int64 } ;
    template (record : T1) = {
      precondition True;
      signatories Cons @Party [M:T1 {party} record] (Nil @Party);
      observers Nil @Party;
      agreement "Agreement";
      choice MyChoice (self) (i : Unit) : Unit,
        controllers Cons @Party [M:T1 {party} record] (Nil @Party)
        to
          ubind
            x1: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 400 };
            x2: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 500 }
          in upure @Unit ();
    };

    val causeRollback : Party -> Update Unit = \(party: Party) ->
        ubind
          u1: Unit <-
            try @Unit
              ubind
                x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
                u: Unit <- exercise @M:T1 MyChoice x1 ();
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
              in throw @(Update Unit) @M:AnException (M:AnException {})
            catch e -> Some @(Update Unit) (upure @Unit ())
          ;
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

  } """ (parserParameters))
    }

  }

  // Section testing exceptions thrown when computing the metadata of a contract
  {
    val parserParameters =
      defaultParserParameters.copy(
        // TODO: revert to the default version once it supports upgrades
        languageVersion = LanguageVersion.Features.smartContractUpgrade
      )

    // A package that defines an interface, a key type, an exception, and a party to be used by
    // the packages defined below.
    val commonDefsPkgId = Ref.PackageId.assertFromString("-common-defs-v1-")
    val commonDefsPkg =
      p"""metadata ( '-common-defs-' : '1.0.0' )
          module Mod {
            record @serializable MyUnit = {};
            interface (this : Iface) = {
              viewtype Mod:MyUnit;

              method myChoiceControllers : List Party;
              method myChoiceObservers : List Party;

              choice @nonConsuming MyChoice (self) (u: Unit): Text
                  , controllers (call_method @Mod:Iface myChoiceControllers this)
                  , observers (call_method @Mod:Iface myChoiceObservers this)
                  to upure @Text "MyChoice was called";
            };

            record @serializable Key = { label: Text, maintainers: List Party };

            record @serializable Ex = { message: Text } ;
            exception Ex = {
              message \(e: Mod:Ex) -> Mod:Ex {message} e
            };

            val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
            val alice : Party = Mod:mkParty "Alice";
          }
      """ (parserParameters.copy(defaultPackageId = commonDefsPkgId))

    /** An abstract class whose [[templateDefinition]] method generates LF code that defines a template named
      * [[templateName]].
      * The class is meant to be extended by concrete case objects which override one the metadata's expressions with an
      * expression that throws an exception.
      */
    abstract class TemplateGenerator(val templateName: String) {
      def precondition = """True"""
      def signatories = s"""Cons @Party [Mod:${templateName} {p} this] (Nil @Party)"""
      def observers = """Nil @Party"""
      def agreement = """"agreement""""
      def key =
        s"""
           |  '$commonDefsPkgId':Mod:Key {
           |    label = "test-key",
           |    maintainers = (Cons @Party [Mod:${templateName} {p} this] (Nil @Party))
           |  }""".stripMargin
      def choiceControllers = s"""Cons @Party [Mod:${templateName} {p} this] (Nil @Party)"""
      def choiceObservers = """Nil @Party"""

      def maintainers =
        s"""\\(key: '$commonDefsPkgId':Mod:Key) -> ('$commonDefsPkgId':Mod:Key {maintainers} key)"""

      def templateDefinition: String =
        s"""
           |  record @serializable $templateName = { p: Party };
           |  template (this: $templateName) = {
           |    precondition $precondition;
           |    signatories $signatories;
           |    observers $observers;
           |    agreement $agreement;
           |
           |    choice @nonConsuming SomeChoice (self) (u: Unit): Text
           |      , controllers (Nil @Party)
           |      , observers (Nil @Party)
           |      to upure @Text "SomeChoice was called";
           |
           |    implements '$commonDefsPkgId':Mod:Iface {
           |      view = '$commonDefsPkgId':Mod:MyUnit {};
           |      method myChoiceControllers = $choiceControllers;
           |      method myChoiceObservers = $choiceObservers;
           |    };
           |
           |    key @'$commonDefsPkgId':Mod:Key ($key) ($maintainers);
           |  };""".stripMargin
    }

    case class ValidMetadata(override val templateName: String)
        extends TemplateGenerator(templateName)

    case object FailingPrecondition extends TemplateGenerator("Precondition") {
      override def precondition =
        s"""throw @Bool @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Precondition"})"""
    }
    case object FailingSignatories extends TemplateGenerator("Signatories") {
      override def signatories =
        s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Signatories"})"""
    }
    case object FailingObservers extends TemplateGenerator("Observers") {
      override def observers =
        s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Observers"})"""
    }
    case object FailingAgreement extends TemplateGenerator("Agreement") {
      override def agreement =
        s"""throw @Text @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Agreement"})"""
    }
    case object FailingKey extends TemplateGenerator("Key") {
      override def key =
        s"""throw @'$commonDefsPkgId':Mod:Key @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Key"})"""
    }
    case object FailingMaintainers extends TemplateGenerator("Maintainers") {
      override def maintainers =
        s"""throw @('$commonDefsPkgId':Mod:Key -> List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Maintainers"})"""
    }
    case object FailingChoiceControllers extends TemplateGenerator("ChoiceControllers") {
      override def choiceControllers =
        s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "ChoiceControllers"})"""
    }
    case object FailingChoiceObservers extends TemplateGenerator("ChoiceObservers") {
      override def choiceObservers =
        s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "ChoiceObservers"})"""
    }

    val templateDefsPkgName = Ref.PackageName.assertFromString("-template-defs-")

    /** A package that defines templates called Precondition, Signatories, ... whose metadata should evaluate without
      * throwing exceptions.
      */
    val templateDefsV1PkgId = Ref.PackageId.assertFromString("-template-defs-v1-id-")
    val templateDefsV1ParserParams = parserParameters.copy(defaultPackageId = templateDefsV1PkgId)
    val templateDefsV1Pkg =
      p"""metadata ( '$templateDefsPkgName' : '1.0.0' )
          module Mod {
            ${ValidMetadata("Precondition").templateDefinition}
            ${ValidMetadata("Signatories").templateDefinition}
            ${ValidMetadata("Observers").templateDefinition}
            ${ValidMetadata("Agreement").templateDefinition}
            ${ValidMetadata("Key").templateDefinition}
            ${ValidMetadata("Maintainers").templateDefinition}
            ${ValidMetadata("ChoiceControllers").templateDefinition}
            ${ValidMetadata("ChoiceObservers").templateDefinition}
          }
      """ (templateDefsV1ParserParams)

    /** Version 2 of the package above. It upgrades the previously defined templates such that:
      *   - the precondition in the Precondition template is changed to throw an exception
      *   - the signatories in the Signatories template is changed to throw an exception
      *   - etc.
      */
    val templateDefsV2PkgId = Ref.PackageId.assertFromString("-template-defs-v2-id-")
    val templateDefsV2ParserParams = parserParameters.copy(defaultPackageId = templateDefsV2PkgId)
    val templateDefsV2Pkg =
      p"""metadata ( '$templateDefsPkgName' : '2.0.0' )
          module Mod {
            ${FailingPrecondition.templateDefinition}
            ${FailingSignatories.templateDefinition}
            ${FailingObservers.templateDefinition}
            ${FailingAgreement.templateDefinition}
            ${FailingKey.templateDefinition}
            ${FailingMaintainers.templateDefinition}
            ${FailingChoiceControllers.templateDefinition}
            ${FailingChoiceObservers.templateDefinition}
          }
      """ (templateDefsV2ParserParams)

    /** Generates a series of expressions meant to test that:
      *   - When [[templateName]] is created, exceptions thrown when evaluating its metadata can be caught.
      *   - When an instance of [[templateName]] is fetched/exercised by id/key/interface, exceptions thrown when
      *     evaluating its metadata cannot be caught.
      */
    def globalContractTests(pkgId: Ref.PackageId, templateName: String): String = {
      val tplQualifiedName = s"'$pkgId':Mod:$templateName"
      s"""
         |  // Checks that the error thrown when creating a $templateName instance can be caught.
         |  val createAndCatchError${templateName}: Update Unit =
         |    try @Unit
         |      ubind _:(ContractId $tplQualifiedName) <-
         |          create @$tplQualifiedName ($tplQualifiedName { p = '$commonDefsPkgId':Mod:alice })
         |      in upure @Unit ()
         |    catch
         |      e -> Some @(Update Unit) (upure @Unit ());
         |
         |  // Tries to catch the error thrown by the contract info of $templateName when exercising a choice on
         |  // it, should fail to do so.
         |  val exerciseAndCatchErrorGlobal${templateName}: (ContractId $tplQualifiedName) -> Update Text =
         |    \\(cid: ContractId $tplQualifiedName) ->
         |      try @Text
         |        exercise @$tplQualifiedName SomeChoice cid ()
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  // Tries to catch the error thrown by the contract info of a $templateName contract when fetching it,
         |  // should fail to do so.
         |  val fetchAndCatchErrorGlobal${templateName}: (ContractId $tplQualifiedName) -> Update Text =
         |    \\(cid: ContractId $tplQualifiedName) ->
         |      try @Text
         |        ubind _:$tplQualifiedName <- fetch_template @$tplQualifiedName cid
         |        in upure @Text "unexpected: contract was fetched"
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  // Tries to catch the error thrown by the contract info of a $templateName contract when fetching it
         |  // by interface, should fail to do so.
         |  val fetchByInterfaceAndCatchErrorGlobal${templateName}: (ContractId $tplQualifiedName) -> Update Text =
         |    \\(cid: ContractId $tplQualifiedName) ->
         |      try @Text
         |        ubind _:'$commonDefsPkgId':Mod:Iface <-
         |            fetch_interface @'$commonDefsPkgId':Mod:Iface
         |                (COERCE_CONTRACT_ID @$tplQualifiedName @'$commonDefsPkgId':Mod:Iface cid)
         |        in upure @Text "unexpected: contract was fetched by interface"
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  // Tries to catch the error thrown by the contract info of a $templateName contract when fetching it
         |  // by key, should fail to do so.
         |  val fetchByKeyAndCatchErrorGlobal${templateName}: '$commonDefsPkgId':Mod:Key -> Update Text =
         |    \\(key: '$commonDefsPkgId':Mod:Key) ->
         |      try @Text
         |        ubind _:<contract: $tplQualifiedName, contractId: ContractId $tplQualifiedName> <-
         |            fetch_by_key @$tplQualifiedName key
         |        in upure @Text "unexpected: contract was fetched by key"
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  // Tries to catch the error thrown by the contract info of a $templateName contract when looking it up
         |  // by key, should fail to do so.
         |  val lookUpByKeyAndCatchErrorGlobal${templateName}: '$commonDefsPkgId':Mod:Key -> Update Text =
         |    \\(key: '$commonDefsPkgId':Mod:Key) ->
         |      try @Text
         |        ubind _:Option (ContractId $tplQualifiedName) <-
         |            lookup_by_key @$tplQualifiedName key
         |        in upure @Text "unexpected: contract was looked up by key"
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |""".stripMargin
    }

    /** Generates a series of expressions meant to test that when a locally created v1 instance of [[templateName]] is
      * fetched/exercised by id/key/interface as a v2 exceptions thrown when evaluating its metadata cannot be caught.
      */
    def localContractTests(
        v1PkgId: Ref.PackageId,
        v2PkgId: Ref.PackageId,
        templateName: String,
    ): String = {
      val v1TplQualifiedName = s"'$v1PkgId':Mod:$templateName"
      val v2TplQualifiedName = s"'$v2PkgId':Mod:$templateName"
      s"""
         |  // Tries to catch the error thrown by the contract info of $templateName when exercising a choice on
         |  // it, should fail to do so.
         |  val exerciseAndCatchErrorLocal${templateName}: Unit -> Update Text =
         |    \\(_:Unit) ->
         |      ubind cid: ContractId $v1TplQualifiedName <-
         |         create @$v1TplQualifiedName ($v1TplQualifiedName { p = '$commonDefsPkgId':Mod:alice })
         |      in try @Text
         |        exercise
         |            @$v2TplQualifiedName
         |            SomeChoice
         |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid)
         |            ()
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  // Tries to catch the error thrown by the contract info of a $templateName contract when fetching it,
         |  // should fail to do so.
         |  val fetchAndCatchErrorLocal${templateName}: Unit -> Update Text =
         |    \\(_:Unit) ->
         |      ubind cid: ContractId $v1TplQualifiedName <-
         |          create @$v1TplQualifiedName ($v1TplQualifiedName { p = '$commonDefsPkgId':Mod:alice })
         |      in try @Text
         |        ubind _:$v2TplQualifiedName <-
         |            fetch_template @$v2TplQualifiedName
         |                (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid)
         |        in upure @Text "unexpected: contract was fetched"
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  // Tries to catch the error thrown by the contract info of a $templateName contract when fetching it
         |  // by interface, should fail to do so.
         |  val fetchByInterfaceAndCatchErrorLocal${templateName}: Unit -> Update Text =
         |    \\(_:Unit) ->
         |      ubind cid: ContractId $v1TplQualifiedName <-
         |          create @$v1TplQualifiedName ($v1TplQualifiedName { p = '$commonDefsPkgId':Mod:alice })
         |      in try @Text
         |        ubind _:'$commonDefsPkgId':Mod:Iface <-
         |            fetch_interface @'$commonDefsPkgId':Mod:Iface
         |                (COERCE_CONTRACT_ID @$v1TplQualifiedName @'$commonDefsPkgId':Mod:Iface cid)
         |        in upure @Text "unexpected: contract was fetched by interface"
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  // Tries to catch the error thrown by the contract info of a $templateName contract when fetching it
         |  // by key, should fail to do so.
         |  val fetchByKeyAndCatchErrorLocal${templateName}: Unit -> Update Text =
         |    \\(_:Unit) ->
         |      ubind cid: ContractId $v1TplQualifiedName <-
         |          create @$v1TplQualifiedName ($v1TplQualifiedName { p = '$commonDefsPkgId':Mod:alice })
         |      in try @Text
         |        ubind _:<contract: $v2TplQualifiedName, contractId: ContractId $v2TplQualifiedName> <-
         |            fetch_by_key
         |                @$v2TplQualifiedName
         |                ('$commonDefsPkgId':Mod:Key {
         |                    label = "test-key",
         |                    maintainers = (Cons @Party ['$commonDefsPkgId':Mod:alice] (Nil @Party)) })
         |        in upure @Text "unexpected: contract was fetched by key"
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  // Tries to catch the error thrown by the contract info of a $templateName contract when looking it up
         |  // by key, should fail to do so.
         |  val lookUpByKeyAndCatchErrorLocal${templateName}: Unit -> Update Text =
         |    \\(_:Unit) ->
         |      ubind cid: ContractId $v1TplQualifiedName <-
         |          create @$v1TplQualifiedName ($v1TplQualifiedName { p = '$commonDefsPkgId':Mod:alice })
         |      in try @Text
         |        ubind _:Option (ContractId $v2TplQualifiedName) <-
         |            lookup_by_key
         |                @$v2TplQualifiedName
         |                ('$commonDefsPkgId':Mod:Key {
         |                    label = "test-key",
         |                    maintainers = (Cons @Party ['$commonDefsPkgId':Mod:alice] (Nil @Party)) })
         |        in upure @Text "unexpected: contract was looked up by key"
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |""".stripMargin
    }

    def dynamicChoiceTestsGlobal(templateName: String): String = {
      s"""
         |  // Tries to catch the error thrown by the dynamic exercise of a $templateName choice when fetching it
         |  // by interface, should fail to do so.
         |  val exerciseByInterfaceAndCatchErrorGlobal${templateName}:
         |      (ContractId '$commonDefsPkgId':Mod:Iface) -> Update Text =
         |    \\(cid: ContractId '$commonDefsPkgId':Mod:Iface) ->
         |      try @Text
         |        exercise_interface @'$commonDefsPkgId':Mod:Iface MyChoice cid ()
         |      catch
         |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
      """.stripMargin
    }

    def dynamicChoiceTestsLocal(v1PkgId: Ref.PackageId, templateName: String): String = {
      val v1TplQualifiedName = s"'$v1PkgId':Mod:$templateName"
      s"""
             |  // Tries to catch the error thrown by the dynamic exercise of a $templateName choice when fetching it
             |  // by interface, should fail to do so.
             |  val exerciseByInterfaceAndCatchErrorLocal${templateName}: Unit -> Update Text =
             |    \\(_:Unit) ->
             |      ubind cid: ContractId $v1TplQualifiedName <-
             |          create @$v1TplQualifiedName ($v1TplQualifiedName { p = '$commonDefsPkgId':Mod:alice })
             |      in try @Text
             |        exercise_interface @'$commonDefsPkgId':Mod:Iface
             |            MyChoice
             |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @'$commonDefsPkgId':Mod:Iface cid)
             |            ()
             |      catch
             |        e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
      """.stripMargin
    }

    val metadataTestsPkgId = Ref.PackageId.assertFromString("-metadata-tests-id-")
    val metadataTestsParserParams = parserParameters.copy(defaultPackageId = metadataTestsPkgId)
    val metadataTestsPkg =
      p"""metadata ( '-metadata-tests-' : '1.0.0' )
          module Mod {
            ${globalContractTests(templateDefsV2PkgId, "Precondition")}
            ${globalContractTests(templateDefsV2PkgId, "Signatories")}
            ${globalContractTests(templateDefsV2PkgId, "Observers")}
            ${globalContractTests(templateDefsV2PkgId, "Agreement")}
            ${globalContractTests(templateDefsV2PkgId, "Key")}
            ${globalContractTests(templateDefsV2PkgId, "Maintainers")}

            ${localContractTests(templateDefsV1PkgId, templateDefsV2PkgId, "Precondition")}
            ${localContractTests(templateDefsV1PkgId, templateDefsV2PkgId, "Signatories")}
            ${localContractTests(templateDefsV1PkgId, templateDefsV2PkgId, "Observers")}
            ${localContractTests(templateDefsV1PkgId, templateDefsV2PkgId, "Agreement")}
            ${localContractTests(templateDefsV1PkgId, templateDefsV2PkgId, "Key")}
            ${localContractTests(templateDefsV1PkgId, templateDefsV2PkgId, "Maintainers")}

            ${dynamicChoiceTestsGlobal("ChoiceControllers")}
            ${dynamicChoiceTestsGlobal("ChoiceObservers")}

            ${dynamicChoiceTestsLocal(templateDefsV1PkgId, "ChoiceControllers")}
            ${dynamicChoiceTestsLocal(templateDefsV1PkgId, "ChoiceObservers")}
          }
    """ (metadataTestsParserParams)

    val compiledPackages: PureCompiledPackages =
      PureCompiledPackages.assertBuild(
        Map(
          commonDefsPkgId -> commonDefsPkg,
          templateDefsV1PkgId -> templateDefsV1Pkg,
          templateDefsV2PkgId -> templateDefsV2Pkg,
          metadataTestsPkgId -> metadataTestsPkg,
        ),
        // TODO: revert to the default compiler config once it supports upgrades
        Compiler.Config.Dev(LanguageMajorVersion.V1),
      )

    sealed trait ContractOrigin {
      def description: String
      def testMethodSuffix: String
    }
    case object Global extends ContractOrigin {
      override def description: String = "global contract"
      override def testMethodSuffix: String = "Global"
    }
    case object Disclosure extends ContractOrigin {
      override def description: String = "disclosed contract"
      override def testMethodSuffix: String = "Global"
    }
    case object Local extends ContractOrigin {
      override def description: String = "local contract"
      override def testMethodSuffix: String = "Local"
    }

    val contractOrigins: List[ContractOrigin] = List(Global, Disclosure, Local)

    val failingTemplateMetadataTemplates: List[String] = List(
      FailingPrecondition.templateName,
      FailingSignatories.templateName,
      FailingObservers.templateName,
      FailingAgreement.templateName,
      FailingKey.templateName,
      FailingMaintainers.templateName,
    )

    val failingChoiceMetadataTemplates: List[String] = List(
      FailingChoiceControllers.templateName,
      FailingChoiceObservers.templateName,
    )

    s"metadata exceptions can be caught when creating a contract" - {
      for (templateName <- failingTemplateMetadataTemplates) {
        templateName in {
          runUpdateExpr(
            compiledPackages,
            e"Mod:createAndCatchError${templateName}" (metadataTestsParserParams),
          ) shouldBe Right(SUnit)
        }
      }
    }

    s"metadata exceptions cannot be caught" - {
      // A test case is a LF test method prefix, a function that provides the argument to the test method,
      // and a list of relevant template names to test.
      val testCases = List[(String, (Value.ContractId, SValue) => SValue, List[String])](
        (
          "exerciseAndCatchError",
          (cid, _) => SContractId(cid),
          failingTemplateMetadataTemplates,
        ),
        (
          "fetchAndCatchError",
          (cid, _) => SContractId(cid),
          failingTemplateMetadataTemplates,
        ),
        (
          "fetchByInterfaceAndCatchError",
          (cid, _) => SContractId(cid),
          failingTemplateMetadataTemplates,
        ),
        (
          "fetchByKeyAndCatchError",
          (_, key) => key,
          failingTemplateMetadataTemplates,
        ),
        (
          "lookUpByKeyAndCatchError",
          (_, key) => key,
          failingTemplateMetadataTemplates,
        ),
        (
          "exerciseByInterfaceAndCatchError",
          (cid, _) => SContractId(cid),
          failingChoiceMetadataTemplates,
        ),
      )

      for (testCase <- testCases) {
        val (prefix, argProvider, relevantTemplates) = testCase
        prefix - {
          for (templateName <- relevantTemplates) {
            templateName - {
              val alice = Ref.Party.assertFromString("Alice")
              val templateId =
                Ref.Identifier.assertFromString(s"-template-defs-v1-id-:Mod:$templateName")
              val cid = Value.ContractId.V1(Hash.hashPrivateKey("abc"))
              val key = SValue.SRecord(
                Ref.Identifier.assertFromString(s"$commonDefsPkgId:Mod:Key"),
                ImmArray(
                  Ref.Name.assertFromString("label"),
                  Ref.Name.assertFromString("maintainers"),
                ),
                ArrayList(
                  SValue.SText("test-key"),
                  SValue.SList(FrontStack(SValue.SParty(alice))),
                ),
              )
              val globalKey = GlobalKeyWithMaintainers.assertBuild(
                templateId,
                key.toUnnormalizedValue,
                Set(alice),
                KeyPackageName(Some(templateDefsPkgName), metadataTestsPkg.languageVersion),
              )
              val globalContract = Versioned(
                version = TransactionVersion.minUpgrade,
                Value.ContractInstance(
                  packageName = templateDefsV1Pkg.metadata.map(_.name),
                  template = templateId,
                  arg = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
                ),
              )
              val disclosedContract = Speedy.ContractInfo(
                version = TransactionVersion.minUpgrade,
                Some(templateDefsPkgName),
                templateId,
                SValue.SRecord(
                  templateId,
                  ImmArray(Ref.Name.assertFromString("p")),
                  ArrayList(SValue.SParty(alice)),
                ),
                "agreement",
                Set(alice),
                Set.empty,
                None,
              )

              for (origin <- contractOrigins) {
                origin.description in {
                  inside {
                    runUpdateApp(
                      compiledPackages,
                      packageResolution = Map(templateDefsPkgName -> templateDefsV2PkgId),
                      e"Mod:${prefix}${origin.testMethodSuffix}$templateName" (
                        metadataTestsParserParams
                      ),
                      Array(argProvider(cid, key)),
                      getContract = origin match {
                        case Global => Map(cid -> globalContract)
                        case Disclosure => Map.empty
                        case Local => Map.empty
                      },
                      getKey = Map(globalKey -> cid),
                      disclosures = origin match {
                        case Global => Map.empty
                        case Disclosure => Map(cid -> disclosedContract)
                        case Local => Map.empty
                      },
                    )
                  } {
                    case Left(
                          SError.SErrorDamlException(
                            IE.UnhandledException(
                              _,
                              Value.ValueRecord(_, ImmArray((_, Value.ValueText(msg)))),
                            )
                          )
                        ) =>
                      msg shouldBe templateName
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  // These tests only make sense for V1 as all V2 versions support exceptions.
  if (majorLanguageVersion == LanguageMajorVersion.V1) {
    "rollback of creates (mixed versions)" - {

      val oldPid: PackageId = Ref.PackageId.assertFromString("OldPackage")
      val newPid: PackageId = Ref.PackageId.assertFromString("Newpackage")

      val oldPackage = {
        val parserParameters: parser.ParserParameters[this.type] = {
          parser.ParserParameters(
            defaultPackageId = oldPid,
            languageVersion = LanguageVersion.v1_11, // version pre-dating exceptions
          )
        }
        p""" metadata ( 'oldPackage' : '1.0.0' )
module OldM {
record @serializable OldT = { party: Party } ;
template (record : OldT) = {
  precondition True;
  signatories Cons @Party [OldM:OldT {party} record] (Nil @Party);
  observers Nil @Party;
  agreement "Agreement";
};
} """ (parserParameters)
      }
      val newPackage = {
        val parserParameters: parser.ParserParameters[this.type] = {
          parser.ParserParameters(
            defaultPackageId = newPid,
            languageVersion = majorLanguageVersion.dev,
          )
        }
        p""" metadata ( 'newPackage' : '1.0.0' )
module NewM {
record @serializable AnException = { } ;
exception AnException = { message \(e: NewM:AnException) -> "AnException" };

record @serializable NewT = { party: Party } ;
template (record : NewT) = {
  precondition True;
  signatories Cons @Party [NewM:NewT {party} record] (Nil @Party);
  observers Nil @Party;
  agreement "Agreement";
  choice MyChoiceCreateJustNew (self) (i : Unit) : Unit,
    controllers Cons @Party [NewM:NewT {party} record] (Nil @Party)
    to
      ubind
        new1: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record };
        new2: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record }
      in upure @Unit ();
  choice MyChoiceCreateOldAndNew (self) (i : Unit) : Unit,
    controllers Cons @Party [NewM:NewT {party} record] (Nil @Party)
    to
      ubind
        new1: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record };
        old: ContractId 'OldPackage':OldM:OldT <- create @'OldPackage':OldM:OldT 'OldPackage':OldM:OldT { party = NewM:NewT {party} record };
        new2: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record }
      in upure @Unit ();
  choice MyChoiceCreateOldAndNewThenThrow (self) (i : Unit) : Unit,
    controllers Cons @Party [NewM:NewT {party} record] (Nil @Party)
    to
      ubind
        new1: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record };
        old: ContractId 'OldPackage':OldM:OldT <- create @'OldPackage':OldM:OldT 'OldPackage':OldM:OldT { party = NewM:NewT {party} record };
        new2: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record };
        u: Unit <- throw @(Update Unit) @NewM:AnException (NewM:AnException {})
      in upure @Unit ();
};

val causeRollback : Party -> Update Unit = \(party: Party) ->
    ubind
      // OK TO CREATE AN OLD VERSION CREATE OUT SIDE THE SCOPE OF THE ROLLBACK
      x1: ContractId 'OldPackage':OldM:OldT <- create @'OldPackage':OldM:OldT 'OldPackage':OldM:OldT { party = party };
      u1: Unit <-
        try @Unit
          ubind
            x2: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = party };
            u: Unit <- exercise @NewM:NewT MyChoiceCreateJustNew x2 ()
          in throw @(Update Unit) @NewM:AnException (NewM:AnException {})
        catch e -> Some @(Update Unit) (upure @Unit ())
      ;
      x3: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = party }
    in upure @Unit ();

val causeUncatchable : Party -> Update Unit = \(party: Party) ->
    ubind
      u1: Unit <-
        try @Unit
          ubind
            x2: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = party };
            // THIS EXERCISE CREATES AN OLD VERSION CONTRACT, AND SO CANNOT BE NESTED IN A ROLLBACK
            u: Unit <- exercise @NewM:NewT MyChoiceCreateOldAndNew x2 ()
          in throw @(Update Unit) @NewM:AnException (NewM:AnException {})
        catch e -> Some @(Update Unit) (upure @Unit ())
      ;
      x3: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = party }
    in upure @Unit ();

val causeUncatchable2 : Party -> Update Unit = \(party: Party) ->
    ubind
      u1: Unit <-
        try @Unit
          ubind
            x2: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = party };
            // THIS EXERCISE CREATES AN OLD VERSION CONTRACT, THEN THROWS, AND SO CANNOT BE NESTED IN A ROLLBACK
            u: Unit <- exercise @NewM:NewT MyChoiceCreateOldAndNewThenThrow x2 ()
          in upure @Unit ()
        catch e -> Some @(Update Unit) (upure @Unit ())
      ;
      x3: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = party }
    in upure @Unit ();

} """ (parserParameters)
      }
      val pkgs =
        SpeedyTestLib.typeAndCompile(
          majorLanguageVersion,
          Map(oldPid -> oldPackage, newPid -> newPackage),
        )

      val parserParameters: parser.ParserParameters[this.type] = {
        parser.ParserParameters(
          defaultPackageId = newPid,
          languageVersion = majorLanguageVersion.dev,
        )
      }

      def transactionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("transactionSeed")

      val causeRollback: SExpr =
        applyToParty(pkgs, e"NewM:causeRollback" (parserParameters), alice)

      "create rollback when old contacts are not within try-catch context" in {
        val res =
          Speedy.Machine
            .fromUpdateSExpr(pkgs, transactionSeed, causeRollback, Set(alice))
            .run()
        inside(res) { case SResultFinal(SUnit) =>
        }
      }
    }
  }
}
