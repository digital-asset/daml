// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref.DottedName
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.LanguageVersion
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.{ParserParameters, defaultPackageId}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SerializabilitySpec extends AnyWordSpec with TableDrivenPropertyChecks with Matchers {

  "Serializability checking" should {

    "accept serializable types" in {

      val testCases = Table(
        "type",
        t"serializableType",
        t"Mod:T",
        t"List serializableType",
        t"serializableType serializableType",
        t"Int64",
        t"(Numeric 10)",
        t"Text",
        t"Timestamp",
        t"Date",
        t"Party",
        t"Bool",
        t"Unit",
      )

      forEvery(testCases) { typ =>
        Serializability
          .Env(defaultFlags, defaultPkgInterface, Context.None, SRDataType, typ)
          .introVar(n"serializableType" -> k"*")
          .checkType()
      }

    }

    "reject unserializable types" in {

      val testCases = Table(
        "type",
        t"unserializableType0",
        t"Numeric",
        t"10",
        t"Mod:R",
        t"Mod:f",
        t"List unserializableType",
        t"unserializableType serializableType",
        t"List unserializableType",
        t"Update",
        t"Scenario",
        t"ContractId",
        t"Arrow",
        t"< f: serializableType >",
      )

      forEvery(testCases) { typ =>
        an[EExpectedSerializableType] should be thrownBy
          Serializability
            .Env(defaultFlags, defaultPkgInterface, Context.None, SRDataType, typ)
            .introVar(n"serializableType" -> k"*")
            .checkType()
      }

    }

    "reject unserializable record definition " in {

      val pkg =
        p"""
          module Mod {
            record @serializable SerializableType = {};
            record UnserializableType = {};
          }

          // well-formed module
          module NegativeTestCase {
            record @serializable R = {
              f: Mod:SerializableType
            };
          }

          // well-formed module
          module PositiveTestCase {
            record @serializable R = {
              f: Mod:UnserializableType                    // disallowed unserializable type
            };
          }
       """

      check(pkg, "NegativeTestCase")
      an[EExpectedSerializableType] shouldBe thrownBy(check(pkg, "PositiveTestCase"))

    }

    "reject unserializable variant definition " in {

      val pkg =
        p"""
          module Mod {
            record @serializable SerializableType = {};
            record UnserializableType = {};
          }

          // well-formed module
          module NegativeTestCase {
            variant @serializable V =
              V1: Mod:SerializableType |
              V2: Mod:SerializableType ;
          }

          // ill-formed module
          module PositiveTestCase1 {
            variant @serializable V = ;                     // disallow empty variant
          }

          // ill-formed module
          module PositiveTestCase2 {
            variant @serializable V =
              V1: Mod:SerializableType |
              V2: Mod:UnserializableType ;                 // disallowed unserializable type
          }

       """

      check(pkg, "NegativeTestCase")
      an[EExpectedSerializableType] shouldBe thrownBy(check(pkg, "PositiveTestCase1"))
      an[EExpectedSerializableType] shouldBe thrownBy(check(pkg, "PositiveTestCase2"))

    }

    "reject unserializable template" in {

      val pkg =
        p"""
          module Mod {
            record @serializable SerializableType = {};
            record UnserializableType = {};
          }

          // well-formed module
          module NegativeTestCase {
            record @serializable SerializableRecord = { alice: Party };

            template (this : SerializableRecord) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              choice Ch (self) (i : Mod:SerializableType) : Mod:SerializableType, controllers ${partiesAlice(
            "NegativeTestCase:SerializableRecord"
          )} to upure @Mod:SerializableType (Mod:SerializableType {});
            } ;
          }

          module PositiveTestCase1 {
            record UnserializableRecord = { alice: Party };

            template (this : UnserializableRecord) =  {    // disallowed unserializable type
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              choice Ch (self) (i : Mod:SerializableType) :
                Mod:SerializableType, controllers ${partiesAlice(
            "PositiveTestCase1:UnserializableRecord"
          )}
                  to upure @Mod:SerializableType (Mod:SerializableType {});
            } ;
          }

          module PositiveTestCase2 {
            record @serializable SerializableRecord = { alice: Party };

            template (this : SerializableRecord) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              choice Ch (self) (i : Mod:UnserializableType) :     // disallowed unserializable type
               Unit, controllers ${partiesAlice("PositiveTestCase2:SerializableRecord")} to
                   upure @Unit ();
            } ;
          }

          module PositiveTestCase3 {
            record @serializable SerializableRecord = { alice: Party };

            template (this : SerializableRecord) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              choice Ch (self) (i : Mod:SerializableType) :
                Mod:UnserializableType, controllers ${partiesAlice(
            "PositiveTestCase3:SerializableRecord"
          )} to       // disallowed unserializable type
                   upure @Mod:UnserializableType (Mod:UnserializableType {});
            } ;
          }
         """

      val positiveTestCases = Table(
        "module",
        "PositiveTestCase1",
        "PositiveTestCase2",
        "PositiveTestCase3",
      )

      check(pkg, "NegativeTestCase")
      forEvery(positiveTestCases) { modName =>
        an[EExpectedSerializableType] shouldBe thrownBy(check(pkg, modName))
      }

    }

    "reject unserializable exception definitions" in {

      val pkg =
        p"""
          // well-formed module
          module NegativeTestCase {
            record @serializable SerializableRecord = { message: Text } ;

            exception SerializableRecord = {
              message \(e: NegativeTestCase:SerializableRecord) -> NegativeTestCase:SerializableRecord {message} e
            } ;
          }

          module PositiveTestCase {
            record UnserializableRecord = { message: Text } ;

            exception UnserializableRecord = {
              message \(e: PositiveTestCase:UnserializableRecord) -> PositiveTestCase:UnserializableRecord {message} e
            } ;
          }
        """

      check(pkg, "NegativeTestCase")
      an[EExpectedSerializableType] shouldBe thrownBy(check(pkg, "PositiveTestCase"))

    }

    "reject unserializable contract for LF =< 1.14" in {

      val pkg14 = {

        implicit val defaultParserParameters: ParserParameters[this.type] = ParserParameters(
          defaultPackageId,
          LanguageVersion.v1_14,
        )
        p"""
          // well-formed module
          module NegativeTestCase1 {
            record @serializable SerializableRecord = {};

            template (this : SerializableRecord) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
            } ;

            record @serializable SerializableContractId = { cid : ContractId NegativeTestCase1:SerializableRecord };
          }

          module NegativeTestCase2 {
            record @serializable SerializableContractId = { cid : ContractId NegativeTestCase1:SerializableRecord };
          }

          module NegativeTestCase3 {
            record @serializable SerializableRecord = {};

            record @serializable OnceUnserializableContractId = { cid : ContractId NegativeTestCase3:SerializableRecord };
          }

          module NegativeTestCase4 {
            record @serializable OnceUnserializableContractId = { cid : ContractId Int64 };
          }

          module NegativeTestCase5 {
            record @serializable OnceUnserializableContractId (a : *) = { cid : ContractId a };
          }

          module PositiveTestCase1 {
            record SerializableRecord = {};

            record @serializable UnserializableContractId = { cid : ContractId PositiveTestCase1:SerializableRecord };
          }

          module PositiveTestCase2 {
            record @serializable UnserializableContractId = { cid : ContractId (Int64 -> Int64) };
          }
         """
      }

      val negativeTestCases = Table(
        "module",
        "NegativeTestCase1",
        "NegativeTestCase2",
        "NegativeTestCase3",
        "NegativeTestCase4",
        "NegativeTestCase5",
      )
      val positiveTestCases = Table(
        "module",
        "PositiveTestCase1",
        "PositiveTestCase2",
      )

      val pkg15 = pkg14.copy(languageVersion = LanguageVersion.v1_15)

      forEvery(negativeTestCases) { modName =>
        check(pkg14, modName)
        check(pkg15, modName)
      }
      forEvery(positiveTestCases) { modName =>
        an[EExpectedSerializableType] shouldBe thrownBy(check(pkg14, modName))
        check(pkg15, modName)
      }

    }

    "reject unserializable interface definitions" in {

      implicit val defaultParserParameters: ParserParameters[this.type] = ParserParameters(
        defaultPackageId,
        LanguageVersion.Features.basicInterfaces,
      )

      val pkg =
        p"""
          module Mod {
            record @serializable MyUnit = {};
          }

          module NegativeTestCase1 {
            interface (this: Token) = {
              viewtype Mod:MyUnit;
              choice GetContractId (self) (u:Unit) : ContractId NegativeTestCase1:Token
                , controllers Nil @Party
                to upure @(ContractId NegativeTestCase1:Token) self;
            } ;
          }

          module NegativeTestCase2 {
            interface (this: Token) = {
              viewtype Mod:MyUnit;
              choice ReturnContractId (self) (u:ContractId NegativeTestCase2:Token) : ContractId NegativeTestCase2:Token
                , controllers Nil @Party
                to upure @(ContractId NegativeTestCase2:Token) self;
            } ;
          }

          module NegativeTestCase3 {
            record @serializable TokenId = {unTokenId : ContractId NegativeTestCase3:Token};

            interface (this: Token) = {
              viewtype Mod:MyUnit;
              choice ReturnContractId (self) (u:NegativeTestCase3:TokenId) : ContractId NegativeTestCase3:Token
                , controllers Nil @Party
                to upure @(ContractId NegativeTestCase3:Token) self;
            } ;
          }

          module PositiveTestCase {
            interface (this: Token) = {
              viewtype Mod:MyUnit;
              choice GetToken (self) (u:Unit) : PositiveTestCase:Token
                , controllers Nil @Party
                to upure @(PositiveTestCase:Token) this;
            } ;
          }
        """

      check(pkg, "NegativeTestCase1")
      check(pkg, "NegativeTestCase2")
      check(pkg, "NegativeTestCase3")
      an[EExpectedSerializableType] shouldBe thrownBy(check(pkg, "PositiveTestCase"))
    }

    "reject unserializable interface view" in {

      implicit val defaultParserParameters: ParserParameters[this.type] = ParserParameters(
        defaultPackageId,
        LanguageVersion.Features.basicInterfaces,
      )

      val pkg =
        p"""
          module Mod {
            record @serializable MyUnit = {};
            record Unserializable = {};
          }

          module NegativeTestCase {
            interface (this: Token) = {
              viewtype Mod:MyUnit;
            } ;
          }

          module PositiveTestCase {
            interface (this: Token) = {
              viewtype Mod:Unserializable;
            } ;
          }
        """

      check(pkg, "NegativeTestCase")
      an[EExpectedSerializableType] shouldBe thrownBy(check(pkg, "PositiveTestCase"))
    }
  }

  private val defaultPkg =
    p"""
      module Mod {

        record R (a: *) (b: *) = {f: a -> b };

        record @serializable T = {alice: Party, bob: Party};
          template (this : T) =  {
            precondition True;
            signatories Cons @Party [bob] (Nil @Party);
            observers Cons @Party [alice] (Nil @Party);
            agreement "Agreement";
            choice Ch (self) (x: Int64) : Decimal, controllers bob to upure @Int64 (DECIMAL_TO_INT64 x);
          } ;

        val f : Int64 -> Int64  =  ERROR @(Int64 -> Int64) "not implemented";

      }
     """

  private val defaultFlags = Serializability.Flags.fromVersion(LanguageVersion.default)
  private val defaultPkgInterface = pkgInterface(defaultPkg)
  private def pkgInterface(pkg: Package) = language.PackageInterface(Map(defaultPackageId -> pkg))

  private def check(pkg: Package, modName: String): Unit = {
    val w = pkgInterface(pkg)
    val longModName = DottedName.assertFromString(modName)
    val mod = pkg.modules(longModName)
    Typing.checkModule(w, defaultPackageId, mod)
    Serializability.checkModule(w, defaultPackageId, mod)
  }

  private def partiesAlice(r: String) = s"(Cons @Party [$r {alice} this] (Nil @Party))"

}
