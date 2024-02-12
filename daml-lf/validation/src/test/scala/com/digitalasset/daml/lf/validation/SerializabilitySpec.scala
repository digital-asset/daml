// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref.DottedName
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser.ParserParameters
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SerializabilitySpecV2 extends SerializabilitySpec(LanguageMajorVersion.V2)

class SerializabilitySpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with TableDrivenPropertyChecks
    with Matchers {

  implicit val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor(majorLanguageVersion)
  val defaultPackageId = defaultParserParameters.defaultPackageId

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
          .Env(defaultPkgInterface, Context.None, SRDataType, typ)
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
            .Env(defaultPkgInterface, Context.None, SRDataType, typ)
            .introVar(n"serializableType" -> k"*")
            .checkType()
      }

    }

    "reject unserializable record definition " in {

      val pkg =
        p"""
          metadata ( 'pkg' : '1.0.0' )

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
          metadata ( 'pkg' : '1.0.0' )

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
          metadata ( 'pkg' : '1.0.0' )

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
          metadata ( 'pkg' : '1.0.0' )

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

    "reject unserializable interface definitions" in {

      val pkg =
        p"""
          metadata ( 'pkg' : '1.0.0' )

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

      val pkg =
        p"""
          metadata ( 'pkg' : '1.0.0' )

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
      metadata ( 'pkg' : '1.0.0' )

      module Mod {

        record R (a: *) (b: *) = {f: a -> b };

        record @serializable T = {alice: Party, bob: Party};
          template (this : T) =  {
            precondition True;
            signatories Cons @Party [bob] (Nil @Party);
            observers Cons @Party [alice] (Nil @Party);
            choice Ch (self) (x: Int64) : Numeric 10, controllers bob to upure @Int64 (NUMERIC_TO_INT64 @10 x);
          } ;

        val f : Int64 -> Int64  =  ERROR @(Int64 -> Int64) "not implemented";

      }
     """

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
