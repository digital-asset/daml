// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.Ref.DottedName
import com.digitalasset.daml.lf.lfpackage.Ast.Package
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.testing.parser._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

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
        t"Decimal",
        t"Text",
        t"Timestamp",
        t"Date",
        t"Party",
        t"Bool",
        t"Unit"
      )

      forEvery(testCases) { typ =>
        Serializability
          .Env(defaultWorld, NoContext, SRDataType, typ)
          .introVar(n"serializableType" -> k"*")
          .checkType()
      }

    }

    "reject unserializable types" in {

      val testCases = Table(
        "type",
        t"unserializableType0",
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
            .Env(defaultWorld, NoContext, SRDataType, typ)
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
                choice Ch (i : Mod:SerializableType) : Mod:SerializableType by 'Alice' to upure @Unit ()
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
                  choice Ch (i : Mod:SerializableType) :
                    Mod:SerializableType by 'Alice'
                      to upure @Unit ()
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
                  choice Ch (i : Mod:UnserializableType) :     // disallowed unserializable type
                    Mod:SerializableType by 'Alice' to
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
                  choice Ch (i : Mod:SerializableType) :
                    Mod:UnserializableType by 'Alice' to       // disallowed unserializable type
                       upure @Unit ()
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
              choice Ch (x: Int64) : Decimal by 'Bob' to upure @Int64 (DECIMAL_TO_INT64 x)
            }
          } ;

        val f : Int64 -> Int64  =  ERROR @(Int64 -> Int64) "not implemented";

      }
     """

  private val defaultWorld =
    world(defaultPkg)
  private def world(pkg: Package) =
    new World(Map(defaultPkgId -> pkg))

  private def check(pkg: Package, modName: String) = {
    val w = world(pkg)
    val longModName = DottedName.assertFromString(modName)
    val mod = w.lookupModule(NoContext, defaultPkgId, longModName)
    Serializability.checkModule(w, defaultPkgId, mod)
  }

}
