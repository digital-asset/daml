// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Party, PackageId}
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.Compiler.FullStackTrace
import com.daml.lf.speedy.SResult.{SResultError, SResultFinalValue}
import com.daml.lf.speedy.SError.DamlEUnhandledException
import com.daml.lf.speedy.SValue.SUnit
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.validation.Validation
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ExceptionTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  // TODO https://github.com/digital-asset/daml/issues/8020
  //   Turn this into a test for to-any-exception / from-any-exception
  //   using only user-defined types, since builtin exception types are going away.

  // "builtin exceptions: to/from-any-exception" should {

  //   // Convertion to/from AnyException works as expected:
  //   // specifically: we can only extract the inner value if the
  //   // expected-type (on: from_any_exception) matches the actual-type (on: to_any_exception)

  //   val pkgs: PureCompiledPackages = typeAndCompile(p"""
  //      module M {
  //        val G : AnyException = to_any_exception @GeneralError (MAKE_GENERAL_ERROR "G");
  //        val A : AnyException = to_any_exception @ArithmeticError (MAKE_ARITHMETIC_ERROR "A");
  //        val C : AnyException = to_any_exception @ContractError (MAKE_CONTRACT_ERROR "C");
  //        val fromG : AnyException -> Text = \(e:AnyException) -> case from_any_exception @GeneralError e of None -> "NONE" | Some x -> GENERAL_ERROR_MESSAGE x;
  //        val fromA : AnyException -> Text = \(e:AnyException) -> case from_any_exception @ArithmeticError e of None -> "NONE" | Some x -> ARITHMETIC_ERROR_MESSAGE x;
  //        val fromC : AnyException -> Text = \(e:AnyException) -> case from_any_exception @ContractError e of None -> "NONE" | Some x -> CONTRACT_ERROR_MESSAGE x;
  //      }
  //     """)

  //   val testCases = Table[String, String](
  //     ("expression", "string-result"),
  //     ("M:fromG M:G", "G"),
  //     ("M:fromA M:A", "A"),
  //     ("M:fromC M:C", "C"),
  //     ("M:fromG M:A", "NONE"),
  //     ("M:fromG M:C", "NONE"),
  //     ("M:fromA M:G", "NONE"),
  //     ("M:fromA M:C", "NONE"),
  //     ("M:fromC M:G", "NONE"),
  //     ("M:fromC M:A", "NONE"),
  //   )

  //   forEvery(testCases) { (exp: String, res: String) =>
  //     s"""eval[$exp] --> "$res"""" in {
  //       val expected: SResult = SResultFinalValue(SValue.SText(res))
  //       runExpr(pkgs)(e"$exp") shouldBe expected
  //     }
  //   }
  // }

  // TODO https://github.com/digital-asset/daml/issues/8020
  //   Add builtin errors back into this test (ArithmeticError and ContractError).
  //   In these cases the message should probably just be "ArithmeticError" and "ContractError".

  "ANY_EXCEPTION_MESSAGE" should {

    // Application of ANY_EXCEPTION_MESSAGE for various cases:
    // 2 user-defined exceptions.
    // MyException1 contains the message text directly
    // MyException2 composes the message string from two text contained in the payload

    val pkgs: PureCompiledPackages = typeAndCompile(p"""
       module M {

         record @serializable MyException1 = { message: Text } ;
         exception MyException1 = {
           message \(e: M:MyException1) -> M:MyException1 {message} e
         };

        val e1 : AnyException = to_any_exception @M:MyException1 (M:MyException1 { message = "Message1" });
        val t1 : Text = ANY_EXCEPTION_MESSAGE M:e1;

         record @serializable MyException2 = { front: Text, back: Text } ;

         exception MyException2 = {
           message \(e: M:MyException2) -> APPEND_TEXT (M:MyException2 {front} e) (M:MyException2 {back} e)
         };

        val e2 : AnyException = to_any_exception @M:MyException2 (M:MyException2 { front = "Mess", back = "age2" });
        val t2 : Text = ANY_EXCEPTION_MESSAGE M:e2;
       }
      """)

    val testCases = Table[String, String](
      ("expression", "expected-string"),
      ("M:t1", "Message1"),
      ("M:t2", "Message2"),
    )

    forEvery(testCases) { (exp: String, res: String) =>
      s"""eval[$exp] --> "$res"""" in {
        val expected: SResult = SResultFinalValue(SValue.SText(res))
        runExpr(pkgs)(e"$exp") shouldBe expected
      }
    }
  }

  "unhandled throw" should {

    // TODO https://github.com/digital-asset/daml/issues/8020
    //   Add some builtin errors to this test.

    // Behaviour when no handler catches a throw:
    // 1: User Exception thrown; no try-catch in scope
    // 2. User Exception thrown; try catch in scope, but handler does not catch
    // 3. User Exception thrown; no handler in scope
    // 4. User Exception thrown; no handler in scope; secondary throw from the message function of the 1st exception

    val pkgs: PureCompiledPackages = typeAndCompile(p"""
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

       }
      """)

    val List(e1, e2) =
      List("M:E1", "M:E2")
        .map(id =>
          data.Ref.Identifier.assertFromString(s"${defaultParserParameters.defaultPackageId}:$id")
        )
        .map(tyCon =>
          SValue.SAny(
            TTyCon(tyCon),
            SValue.SRecord(tyCon, data.ImmArray.empty, new util.ArrayList()),
          )
        )

    val testCases = Table[String, SResult](
      ("expression", "expected"),
      ("M:unhandled1", SResultError(DamlEUnhandledException(e1))),
      ("M:unhandled2", SResultError(DamlEUnhandledException(e1))),
      ("M:unhandled3", SResultError(DamlEUnhandledException(e1))),
      ("M:unhandled4", SResultError(DamlEUnhandledException(e2))),
    )

    forEvery(testCases) { (exp: String, expected: SResult) =>
      s"eval[$exp] --> $expected" in {
        runUpdateExpr(pkgs)(e"$exp") shouldBe expected
      }
    }
  }

  "throw/catch (UserDefined)" should {

    // Basic throw/catch example a user defined exception

    val pkgs: PureCompiledPackages = typeAndCompile(p"""
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
        val expected: SResult = SResultFinalValue(SValue.SInt64(num))
        runUpdateExpr(pkgs)(e"$exp") shouldBe expected
      }
    }
  }

  "throw/catch (UserDefined, with integer payload)" should {

    // Throw/catch example of a user defined exception, passing an integer payload.

    val pkgs: PureCompiledPackages = typeAndCompile(p"""
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
        val expected: SResult = SResultFinalValue(SValue.SInt64(num))
        runUpdateExpr(pkgs)(e"$exp") shouldBe expected
      }
    }
  }

  "selective exception handling" should {

    // Example which nests two try-catch block around an expression which throws:
    // 3 variants: (with one source line changed,marked)
    // -- The inner handler catches (some)
    // -- The inner handler does not catch (none); allowing the outer handler to catch
    // -- The inner handler throws while deciding whether to catch

    val pkgs: PureCompiledPackages = typeAndCompile(p"""
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
        val expected: SResult = SResultFinalValue(SValue.SInt64(num))
        runUpdateExpr(pkgs)(e"$exp") shouldBe expected
      }
    }
  }

  "selective exception handling (using payload)" should {

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

    val pkgs: PureCompiledPackages = typeAndCompile(p"""
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
        val expected: SResult = SResultFinalValue(SValue.SInt64(num))
        runUpdateExpr(pkgs)(e"$exp") shouldBe expected
      }
    }
  }

  "throw/catch (control flow)" should {

    // Another example allowing dynamic control of different test paths. Novelty here:
    // - uses GeneralError, with Text payload encoding an int
    // - variations 1..4 selected by an integer arg
    // - final result contains elements which demonstrate the control flow taken

    val pkgs: PureCompiledPackages = typeAndCompile(p"""
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
        val expected: SResult = SResultFinalValue(SValue.SInt64(num))
        runUpdateExpr(pkgs)(e"$exp") shouldBe expected
      }
    }
  }

  "throws from various places" should {

    // Example define a common "wrap" function which handles just a specific user exception:
    // Series of tests which may throw within its context:
    //
    // - example1 -- no thow
    // - example2 -- throw handled exception
    // - example3 -- throw unhandled exception
    // - example4 -- throw handled exception on left & right of a binary op
    // - example5 -- throw handled exception which computing the payload of an outer throw

    val pkgs: PureCompiledPackages = typeAndCompile(p"""
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
      ("M:example4", "HANDLED: left"),
      ("M:example5", "HANDLED: throw-in-throw"),
    )

    forEvery(testCases) { (exp: String, str: String) =>
      s"eval[$exp] --> $str" in {
        val expected: SResult = SResultFinalValue(SValue.SText(str))
        runUpdateExpr(pkgs)(e"$exp") shouldBe expected
      }
    }
  }

  "rollback of creates" should {

    val party = Party.assertFromString("Alice")
    val example: Expr = EApp(e"M:causeRollback", EPrimLit(PLParty(party)))
    def transactionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("transactionSeed")

    val anException = {
      val id = "M:AnException"
      val tyCon =
        data.Ref.Identifier.assertFromString(s"${defaultParserParameters.defaultPackageId}:$id")
      SValue.SAny(
        TTyCon(tyCon),
        SValue.SRecord(tyCon, data.ImmArray.empty, new util.ArrayList()),
      )
    }

    "works as expected for a contract version POST-dating exceptions" in {
      val pkgs = mkPackagesAtVersion(LanguageVersion.v1_dev)
      val res = Speedy.Machine.fromUpdateExpr(pkgs, transactionSeed, example, party).run()
      res shouldBe SResultFinalValue(SUnit)
    }

    "causes an uncatchable exception to be thrown for a contract version PRE-dating exceptions" in {
      val pkgs = mkPackagesAtVersion(LanguageVersion.v1_11)
      val res = Speedy.Machine.fromUpdateExpr(pkgs, transactionSeed, example, party).run()
      res shouldBe SResultError(DamlEUnhandledException(anException))
    }

    def mkPackagesAtVersion(languageVersion: LanguageVersion): PureCompiledPackages = {

      implicit val defaultParserParameters: ParserParameters[this.type] = {
        ParserParameters(
          defaultPackageId = Ref.PackageId.assertFromString("-pkgId-"),
          languageVersion,
        )
      }

      typeAndCompile(p"""
      module M {

        record @serializable AnException = { } ;
        exception AnException = { message \(e: M:AnException) -> "AnException" };

        record @serializable T1 = { party: Party, info: Int64 } ;
        template (record : T1) = {
          precondition True,
          signatories Cons @Party [M:T1 {party} record] (Nil @Party),
          observers Nil @Party,
          agreement "Agreement",
          choices {
            choice MyChoice (self) (i : Unit) : Unit
            , controllers Cons @Party [M:T1 {party} record] (Nil @Party)
            to
              ubind
                x1: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 400 };
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 500 }
              in upure @Unit ()
          }
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

      } """)
    }

  }

  private def typeAndCompile(pkg: Package): PureCompiledPackages = {
    val rawPkgs = Map(defaultParserParameters.defaultPackageId -> pkg)
    Validation.checkPackage(rawPkgs, defaultParserParameters.defaultPackageId, pkg)
    data.assertRight(
      PureCompiledPackages(rawPkgs, Compiler.Config.Dev.copy(stacktracing = FullStackTrace))
    )
  }

  private def runExpr(pkgs1: PureCompiledPackages)(e: Expr): SResult = {
    Speedy.Machine.fromPureExpr(pkgs1, e).run()
  }

  private def runUpdateExpr(pkgs1: PureCompiledPackages)(e: Expr): SResult = {
    def transactionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("ExceptionTest.scala")
    Speedy.Machine.fromScenarioExpr(pkgs1, transactionSeed, e).run()
  }

  "rollback of creates (mixed versions)" should {

    val party = Party.assertFromString("Alice")

    val oldPid: PackageId = Ref.PackageId.assertFromString("OldPackage")
    val newPid: PackageId = Ref.PackageId.assertFromString("Newpackage")

    val oldPackage = {
      implicit val defaultParserParameters: ParserParameters[this.type] = {
        ParserParameters(
          defaultPackageId = oldPid,
          languageVersion = LanguageVersion.v1_11, //version pre-dating exceptions
        )
      }
      p"""
      module OldM {
        record @serializable OldT = { party: Party } ;
        template (record : OldT) = {
          precondition True,
          signatories Cons @Party [OldM:OldT {party} record] (Nil @Party),
          observers Nil @Party,
          agreement "Agreement",
          choices {}
        };
      } """
    }
    val newPackage = {
      implicit val defaultParserParameters: ParserParameters[this.type] = {
        ParserParameters(
          defaultPackageId = newPid,
          languageVersion = LanguageVersion.v1_dev,
        )
      }
      p"""
      module NewM {
        record @serializable AnException = { } ;
        exception AnException = { message \(e: NewM:AnException) -> "AnException" };

        record @serializable NewT = { party: Party } ;
        template (record : NewT) = {
          precondition True,
          signatories Cons @Party [NewM:NewT {party} record] (Nil @Party),
          observers Nil @Party,
          agreement "Agreement",
          choices {

            choice MyChoiceCreateJustNew (self) (i : Unit) : Unit
            , controllers Cons @Party [NewM:NewT {party} record] (Nil @Party)
            to
              ubind
                new1: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record };
                new2: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record }
              in upure @Unit (),

            choice MyChoiceCreateOldAndNew (self) (i : Unit) : Unit
            , controllers Cons @Party [NewM:NewT {party} record] (Nil @Party)
            to
              ubind
                new1: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record };
                old: ContractId 'OldPackage':OldM:OldT <- create @'OldPackage':OldM:OldT 'OldPackage':OldM:OldT { party = NewM:NewT {party} record };
                new2: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record }
              in upure @Unit (),

            choice MyChoiceCreateOldAndNewThenThrow (self) (i : Unit) : Unit
            , controllers Cons @Party [NewM:NewT {party} record] (Nil @Party)
            to
              ubind
                new1: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record };
                old: ContractId 'OldPackage':OldM:OldT <- create @'OldPackage':OldM:OldT 'OldPackage':OldM:OldT { party = NewM:NewT {party} record };
                new2: ContractId NewM:NewT <- create @NewM:NewT NewM:NewT { party = NewM:NewT {party} record };
                u: Unit <- throw @(Update Unit) @NewM:AnException (NewM:AnException {})
              in upure @Unit ()
          }
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

      } """
    }
    val pkgs = {
      val rawPkgs: Map[PackageId, GenPackage[Expr]] = Map(
        oldPid -> oldPackage,
        newPid -> newPackage,
      )
      data.assertRight(
        PureCompiledPackages(rawPkgs, Compiler.Config.Dev.copy(stacktracing = FullStackTrace))
      )
    }

    implicit val defaultParserParameters: ParserParameters[this.type] = {
      ParserParameters(
        defaultPackageId = newPid,
        languageVersion = LanguageVersion.v1_dev,
      )
    }

    val anException = {
      val id = "NewM:AnException"
      val tyCon =
        data.Ref.Identifier.assertFromString(s"$newPid:$id")
      SValue.SAny(
        TTyCon(tyCon),
        SValue.SRecord(tyCon, data.ImmArray.empty, new util.ArrayList()),
      )
    }

    def transactionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("transactionSeed")

    val causeRollback: Expr = EApp(e"NewM:causeRollback", EPrimLit(PLParty(party)))
    val causeUncatchable: Expr = EApp(e"NewM:causeUncatchable", EPrimLit(PLParty(party)))
    val causeUncatchable2: Expr = EApp(e"NewM:causeUncatchable2", EPrimLit(PLParty(party)))

    "create rollback when old contacts are not within try-catch context" in {
      val res = Speedy.Machine.fromUpdateExpr(pkgs, transactionSeed, causeRollback, party).run()
      res shouldBe SResultFinalValue(SUnit)
    }

    "causes uncatchable exception when an old contract is within a new-exercise within a try-catch" in {
      val res = Speedy.Machine.fromUpdateExpr(pkgs, transactionSeed, causeUncatchable, party).run()
      res shouldBe SResultError(DamlEUnhandledException(anException))
    }

    "causes uncatchable exception when an old contract is within a new-exercise which aborts" in {
      val res = Speedy.Machine.fromUpdateExpr(pkgs, transactionSeed, causeUncatchable2, party).run()
      res shouldBe SResultError(DamlEUnhandledException(anException))
    }

  }
}
