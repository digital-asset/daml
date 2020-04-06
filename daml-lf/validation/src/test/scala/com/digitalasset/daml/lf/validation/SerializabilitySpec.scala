// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref.DottedName
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.defaultPackageId
import com.daml.lf.validation.SpecUtil._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

class SerializabilitySpec extends WordSpec with TableDrivenPropertyChecks with Matchers {

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
        t"Unit"
      )

      forEvery(testCases) { typ =>
        Serializability
          .Env(LanguageVersion.default, defaultWorld, NoContext, SRDataType, typ)
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
        t"< f: serializableType >"
      )

      forEvery(testCases) { typ =>
        an[EExpectedSerializableType] should be thrownBy
          Serializability
            .Env(LanguageVersion.default, defaultWorld, NoContext, SRDataType, typ)
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
            variant @serializable V = ;                       // disallow empty variant
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
            record @serializable SerializableRecord = {};

            template (this : SerializableRecord) =  {
              precondition True,
              signatories Nil @Party,
              observers Nil @Party,
              agreement "Agreement",
              choices {
                choice Ch (self) (i : Mod:SerializableType) : Mod:SerializableType by $partiesAlice to upure @Mod:SerializableType (Mod:SerializableType {})
              }
            } ;
          }

          module PositiveTestCase1 {
              record UnserializableRecord = {};

              template (this : UnserializableRecord) =  {    // disallowed unserializable type
                precondition True,
                signatories Nil @Party,
                observers Nil @Party,
                agreement "Agreement",
                choices {
                  choice Ch (self) (i : Mod:SerializableType) :
                    Mod:SerializableType by $partiesAlice
                      to upure @Mod:SerializableType (Mod:SerializableType {})
                }
              } ;
            }


          module PositiveTestCase2 {
              record @serializable SerializableRecord = {};

              template (this : SerializableRecord) =  {
                precondition True,
                signatories Nil @Party,
                observers Nil @Party,
                agreement "Agreement",
                choices {
                  choice Ch (self) (i : Mod:UnserializableType) :     // disallowed unserializable type
                   Unit by $partiesAlice to
                       upure @Unit ()
                }
              } ;
            }

          module PositiveTestCase3 {
              record @serializable SerializableRecord = {};

              template (this : SerializableRecord) =  {
                precondition True,
                signatories Nil @Party,
                observers Nil @Party,
                agreement "Agreement",
                choices {
                  choice Ch (self) (i : Mod:SerializableType) :
                    Mod:UnserializableType by $partiesAlice to       // disallowed unserializable type
                       upure @Mod:UnserializableType (Mod:UnserializableType {})
                }
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

    "reject unserializable contract id" in {

      val pkg0 =
        p"""
          // well-formed module
          module NegativeTestCase1 {
            record @serializable SerializableRecord = {};

            template (this : SerializableRecord) =  {
              precondition True,
              signatories Nil @Party,
              observers Nil @Party,
              agreement "Agreement",
              choices {
              }
            } ;

            record @serializable SerializableContractId = { cid : ContractId NegativeTestCase1:SerializableRecord };
          }

          module NegativeTestCase2 {
            record @serializable SerializableContractId = { cid : ContractId NegativeTestCase1:SerializableRecord };
          }

          module OncePositiveTestCase1 {
            record @serializable SerializableRecord = {};

            record @serializable OnceUnserializableContractId = { cid : ContractId OncePositiveTestCase1:SerializableRecord };
          }

          module OncePositiveTestCase2 {
            record @serializable OnceUnserializableContractId = { cid : ContractId Int64 };
          }

          module OncePositiveTestCase3 {
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

      val version1_4 = LanguageVersion(LanguageMajorVersion.V1, "4")
      val version1_5 = LanguageVersion(LanguageMajorVersion.V1, "5")
      val versions = Table("version", version1_4, version1_5)

      val neverFail = (_: LanguageVersion) => false
      val failBefore1_5 =
        (version: LanguageVersion) => LanguageVersion.ordering.lteq(version, version1_4)
      val alwaysFail = (_: LanguageVersion) => true
      val testCases = Table[String, LanguageVersion => Boolean](
        "module" -> "should fail",
        "NegativeTestCase1" -> neverFail,
        "NegativeTestCase2" -> neverFail,
        "OncePositiveTestCase1" -> failBefore1_5,
        "OncePositiveTestCase2" -> failBefore1_5,
        "OncePositiveTestCase3" -> failBefore1_5,
        "PositiveTestCase1" -> alwaysFail,
        "PositiveTestCase2" -> alwaysFail,
      )

      forEvery(versions) { version =>
        val pkg = pkg0.updateVersion(version)
        forEvery(testCases) { (modName: String, shouldFail: LanguageVersion => Boolean) =>
          if (shouldFail(version)) {
            an[EExpectedSerializableType] shouldBe thrownBy(check(pkg, modName))
            ()
          } else {
            check(pkg, modName)
          }
        }
      }
    }
  }

  private val defaultPkg =
    p"""
      module Mod {

        record R (a: *) (b: *) = {f: a -> b };

        record @serializable T = {};
          template (this : T) =  {
            precondition True,
            signatories Cons @Party ['Bob'] (Nil @Party),
            observers Cons @Party ['Alice'] (Nil @Party),
            agreement "Agreement",
            choices {
              choice Ch (self) (x: Int64) : Decimal by 'Bob' to upure @Int64 (DECIMAL_TO_INT64 x)
            }
          } ;

        val f : Int64 -> Int64  =  ERROR @(Int64 -> Int64) "not implemented";

      }
     """

  private val defaultWorld =
    world(defaultPkg)
  private def world(pkg: Package) =
    new World(Map(defaultPackageId -> pkg))

  private def check(pkg: Package, modName: String): Unit = {
    val w = world(pkg)
    val longModName = DottedName.assertFromString(modName)
    val mod = w.lookupModule(NoContext, defaultPackageId, longModName)
    require(Try(Typing.checkModule(w, defaultPackageId, mod)).isSuccess)
    Serializability.checkModule(w, defaultPackageId, mod)
  }

  private val partiesAlice = "(Cons @Party ['Alice'] (Nil @Party))"

}
