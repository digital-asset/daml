// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref.DottedName
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.defaultPackageId
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartyLiteralsSpec extends AnyWordSpec with TableDrivenPropertyChecks with Matchers {

  import PartyLiterals._

  "Party Literals validation" should {

    "disallows bad party literals" in {

      val pkg =
        p"""
          module Mod {
            record R = {};
            val isAlice: Party -> Bool = EQUAL_PARTY 'Alice';
            val @noPartyLiterals v: Unit = ();
          }

          // a well-formed module
          module NegativeTestCase {
            val @noPartyLiterals v: Unit = Mod:v;

            val @noPartyLiterals isAlice: Party -> Bool =
              \ (party: Party) -> EQUAL_TEXT (PARTY_TO_TEXT party) "'Alice'";

            record R = { party: Party };
            template (this : R) =  {
                precondition True;
                signatories Cons @Party [party] (Nil @Party);
                observers Cons @Party [party] (Nil @Party);
                agreement "Agreement";
                choice Ch (self) (i : Unit) : Unit, controllers party to
                  upure @Unit ();
            };
          }

          module PositiveTestCase1 {
            val @noPartyLiterals bob : Party = 'Bob';          // disallowed party literal 'Bob'
          }

          module PositiveTestCase2 {
            val @noPartyLiterals isAlice : Party -> Bool =
              Mod:isAlice;                                     // disallowed value ref `Mod:isAllice`
          }


          module PositiveTestCase3 {
            record R = { party: Party };
            template (this : R) =  {
                precondition EQUAL_PARTY party 'Alice';        // disallowed party literal 'Alice'
                signatories Cons @Party [party] (Nil @Party);
                observers Cons @Party [party] (Nil @Party);
                agreement "Agreement";
                choice Ch (self) (i : Mod:R): Unit, controllers party to
                  upure @Unit ();
            } ;
          }

          module PositiveTestCase4 {
            record R = { party: Party };
            template (this : R) =  {
                precondition True;
                signatories Cons @Party ['Alice'] (Nil @Party);  // disallowed party literal 'Alice'
                observers Cons @Party [party] (Nil @Party);
                agreement "Agreement";
                choice Ch (self) (i : Mod:R): Unit, controllers party to
                  upure @Unit ();
            } ;
          }

          module PositiveTestCase5 {
            record R = { party: Party };
            template (this : R) =  {
                precondition True;
                signatories Cons @Party [party] (Nil @Party);
                observers Cons @Party ['Alice'] (Nil @Party);    // disallowed party literal 'Alice'
                agreement "Agreement";
                choice Ch (self) (i : Mod:R): Unit, controllers 'Alice' to
                  upure @Unit ();
            } ;
          }

          module PositiveTestCase6 {
              record R = { party: Party };
              template (this : R) =  {
                  precondition True;
                  signatories Cons @Party [party] (Nil @Party);
                  observers Cons @Party [party] (Nil @Party);
                  agreement PARTY_TO_TEXT 'Alice';               // disallowed party literal 'Alice'
                  choice Ch (self) (i : Mod:R): Unit, controllers 'Alice' to
                    upure @Unit ();
              } ;
            }

          module PositiveTestCase7 {
            record R = { party: Party };
            template (this : R) =  {
                precondition True;
                signatories Cons @Party [party] (Nil @Party);
                observers Cons @Party [party] (Nil @Party);
                agreement "Agreement";
                choice Ch (self) (i : Mod:R): Party, controllers party to
                   upure @Party 'Alice';                       // disallowed party literal 'Alice'
            } ;
          }

          module @noPartyLiterals PositiveTestCase8 {
            val bob : Party = Error "not implememted";          // disallowed value ref
          }

        """

      val positiveTestCases = Table(
        "module",
        "PositiveTestCase1",
        "PositiveTestCase2",
        "PositiveTestCase3",
        "PositiveTestCase4",
        "PositiveTestCase5",
        "PositiveTestCase6",
        "PositiveTestCase7",
        "PositiveTestCase8",
      )

      val interface = language.PackageInterface(Map(defaultPackageId -> pkg))

      checkModule(
        interface,
        defaultPackageId,
        pkg.modules(DottedName.assertFromString("NegativeTestCase")),
      )
      forEvery(positiveTestCases) { modName =>
        an[EForbiddenPartyLiterals] should be thrownBy
          checkModule(
            interface,
            defaultPackageId,
            pkg.modules(DottedName.assertFromString(modName)),
          )
      }
    }

  }

}
