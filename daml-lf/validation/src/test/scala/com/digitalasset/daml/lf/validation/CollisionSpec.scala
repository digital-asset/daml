// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.language.Ast.Package
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.defaultPackageId
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class CollisionSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  def check(pkg: Package): Unit =
    Collision.checkPackage(defaultPackageId, pkg)

  // TODO https://github.com/digital-asset/daml/issues/12051
  //   Add test for collision of interface names, interface choices, and methods.
  "Collision validation" should {

    "detect collisions of record fields" in {

      // a record definition without collision
      val negativeTestCase = p"""
         module Mod {                  // fully resolved name: "Mod"
           record R = {                // fully resolved name: "Mod.R."
             field1: Int64,            // fully resolved name: "Mod.R.field1"
             field2: Decimal           // fully resolved name: "Mod.R.field2"
           };
         }
         """

      val positiveTestCases = Table(
        "module",
        // a record definition with collision
        p"""
          module Mod {                  // fully resolved name: "Mod"
            record R = {                // fully resolved name: "Mod.R."
              field: Int64,             // fully resolved name: "Mod.R.field"  (collision)
              field: Decimal            // fully resolved name: "Mod.R.field"  (collision)
            };
          }
          """,
        // a record definition with case-insensitive collision
        p"""
          module Mod {                  // fully resolved name: "Mod"
            record R = {                  // fully resolved name: "Mod.R."
              field: Int64,             // fully resolved name: "Mod.R.field"  (collision)
              FiElD: Decimal            // fully resolved name: "Mod.R.FiElD"  (collision)
            };
          }
          """,
      )

      check(negativeTestCase)
      forEvery(positiveTestCases)(positiveTestCase =>
        an[ECollision] shouldBe thrownBy(check(positiveTestCase))
      )
    }

    "detect collisions of variant constructor" in {

      // a variant definition without collision
      val negativeTestCase = p"""
         module Mod {                  // fully resolved name: "Mod"
           variant V =                 // fully resolved name: "Mod.V"
             Variant1: Int64           // fully resolved name: "Mod.V.Variant1"
           | Variant2: Decimal;        // fully resolved name: "Mod.V.Variant2"
         }
         """

      val positiveTestCases = Table(
        "module",
        // a variant definition with case sensitive collision
        p"""
          module Mod {                 // fully resolved name: "Mod"
            variant V =                // fully resolved name: "Mod.V"
              Variant: Int64           // fully resolved name: "Mod.V.Variant" (collision)
            | Variant: Decimal;        // fully resolved name: "Mod.V.Variant" (collision)
          }
          """,
        // a variant definition with case insensitive collision
        p"""
          module Mod {                 // fully resolved name: "Mod"
            variant V =                   // fully resolved name: "Mod.V"
              Variant: Int64           // fully resolved name: "Mod.V.Variant" (collision)
            | VARIANT: Decimal;        // fully resolved name: "Mod.V.VARIANT" (collision)
          }
          """,
      )

      check(negativeTestCase)
      forEvery(positiveTestCases)(positiveTestCase =>
        an[ECollision] shouldBe thrownBy(check(positiveTestCase))
      )
    }

    "detect collisions of between module and type" in {

      // a collision-free package
      val negativeTestCase = p"""
          module A {                    // fully resolved name: "A"
            record B = {};              // fully resolved name: "A.B"
          }

          module A.C {                  // fully resolved name: "A.C"
          }
        """

      val positiveTestCases = Table(
        "module",
        // a package with collision: two constructs have the same fully resoled name "A.B"
        p"""
          module A {                    // fully resolved name: "A"
            record B = {};              // fully resolved name: "A.B" (collision)
          }

          module A.B {                  // fully resolved name: "A.B" (collision)

          }
        """,
        // a package with collision: two constructs have the same fully resoled name "A.B"
        p"""
          module A {                    // fully resolved name: "A"
            record B = {};                // fully resolved name: "A.B" (collision)
          }

          module a.B {                  // fully resolved name: "a.B" (collision)

          }
        """,
        // a package with collision: two constructs have the same fully resoled name "A.B"
        p"""
          module A {                    // fully resolved name: "A"
            synonym B = |Mod:A|;        // fully resolved name: "A.B" (collision)
          }

          module a.B {                  // fully resolved name: "a.B" (collision)

          }
        """,
      )

      check(negativeTestCase)
      forEvery(positiveTestCases)(positiveTestCase =>
        an[ECollision] shouldBe thrownBy(check(positiveTestCase))
      )
    }

    "detect complex collisions between definitions" in {

      // a collision-free package
      val negativeTestCase = p"""
          module A {                      // fully resolved name: "A"
            record B.C = {};              // fully resolved name: "A.B.C"
          }

          module A.B {                    // fully resolved name: "A.B"
            record D = {};                // fully resolved name: "A.B.D"
          }
        """

      val positiveTestCases = Table(
        "module",
        // a package with collision: two constructs have the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            record B.C = {};              // fully resolved name: "A.B.C" (collision)
          }

          module A.B {                    // fully resolved name: "A"
            record C = {};                // fully resolved name: "A.B.C" (collision)
          }
        """,
        // a package with case insensitive collision: two constructs have the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            record B.C = {};              // fully resolved name: "A.B.C" (collision)
          }

          module a.b {                    // fully resolved name: "a.b"
            record c = {};                // fully resolved name: "a.b.c" (collision)
          }
        """,
        // a package with case insensitive collision: a record and a variant have the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            record B.C = {};              // fully resolved name: "A.B.C" (collision)
          }

          module a.b {                    // fully resolved name: "A.B"
            variant c =;                  // fully resolved name: "A.B.C" (collision)
          }
        """,
        // a package with case insensitive collision: a record and a enum the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            record B.C = {};              // fully resolved name: "A.B.C" (collision)
          }

          module a.b {                    // fully resolved name: "A.B"
            enum c =;                  // fully resolved name: "A.B.C" (collision)
          }
        """,
        // a package with case insensitive collision: a variant and a enum have the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            variant B.C =;              // fully resolved name: "A.B.C" (collision)
          }

          module a.b {                    // fully resolved name: "A.B"
            enum c =;                  // fully resolved name: "A.B.C" (collision)
          }
        """,
        // a package with case insensitive collision: a record and a synonym have the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            record B.C = {};              // fully resolved name: "A.B.C" (collision)
          }

          module a.b {                    // fully resolved name: "A.B"
            synonym c = |Mod:T|;          // fully resolved name: "A.B.C" (collision)
          }
        """,
        // a package with case insensitive collision: a variant and a synonym have the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            variant B.C =;              // fully resolved name: "A.B.C" (collision)
          }

          module a.b {                    // fully resolved name: "A.B"
            synonym c = |Mod:T|;          // fully resolved name: "A.B.C" (collision)
          }
        """,
        // a package with case insensitive collision: a variant and a synonym have the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            variant B.C =;              // fully resolved name: "A.B.C" (collision)
          }

          module a.b {                    // fully resolved name: "A.B"
            synonym c = |Mod:T|;          // fully resolved name: "A.B.C" (collision)
          }
        """,
        // a package with case insensitive collision: two constructs have the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            synonym B.C = |Mod:T|;        // fully resolved name: "A.B.C"
          }

          module a.b {                    // fully resolved name: "A.B"
            synonym c = |Mod:T|;          // fully resolved name: "A.B.c"
          }
        """,
        // a package with case insensitive collision: two constructs have the same fully resoled name "A.B.C"
        p"""
          module A {                      // fully resolved name: "A"
            synonym B.C = |Mod:T|;        // fully resolved name: "A.B.C"
          }

          module a.b {                    // fully resolved name: "A.B"
            synonym c = |Mod:T|;          // fully resolved name: "A.B.c"
          }
        """,
      )

      check(negativeTestCase)
      forEvery(positiveTestCases)(positiveTestCase =>
        an[ECollision] shouldBe thrownBy(check(positiveTestCase))
      )
    }

    "allow collision between variant and record in the same module" in {

      // a package containing a allowed collision between variant constructor and record type
      val negativeTestCase = p"""
          module Mod {                     // fully resolved name: "Mod"
            variant Tree (a: * ) =         // fully resolved name: "Mod.Tree"
              Leaf : Unit                  // fully resolved name: "Mod.Tree.Leaf"
            | Node : Mod:Tree.Node a ;     // fully resolved name: "Mod.Tree.Node"       (allowed collision)

            record Tree.Node (a: *) = {    // fully resolved name: "Mod.Tree.Node"       (allowed collision)
               value: a,                   // fully resolved name: "Mod.Tree.Node.value"
               left : Mod:Tree a,          // fully resolved name: "Mod.Tree.Node.left"
               right : Mod:Tree a          // fully resolved name: "Mod.Tree.Node.right"
            };
          }
        """

      val positiveTestCases = Table(
        "module",
        // a package containing a disallowed collision between variant constructor and record type
        // variant and record are from different modules
        p"""
          module Mod {                    // fully resolved name: "Mod"
            variant Tree (a: * ) =        // fully resolved name: "Mod.Tree"
              Leaf : Unit                 // fully resolved name: "Mod.Tree.Leaf"
            | Node : Mod.Tree:Node a ;    // fully resolved name: "Mod.Tree.Node"       (disallowed collision)
          }

          module Mod.Tree {               // fully resolved name: "Mod.Tree"
            record Node (a: *) = {        // fully resolved name: "Mod.Tree.Node"       (disallowed collision)
              value: a,                   // fully resolved name: "Mod.Tree.Node.value"
              left : Mod:Tree a,          // fully resolved name: "Mod.Tree.Node.left"
              right : Mod:Tree a          // fully resolved name: "Mod.Tree.Node.right"
            };
          }
        """,
        // a package containing a disallowed collision between variant constructor and record type
        // variant and record have different cases.
        p"""
          module Mod {                     // fully resolved name: "Mod"
            variant Tree (a: * ) =         // fully resolved name: "Mod.Tree"
              Leaf : Unit                  // fully resolved name: "Mod.Tree.Leaf"
            | Node : Mod:Tree.Node a ;     // fully resolved name: "Mod.Tree.Node"       (disallowed collision)

            record Tree.node (a: *) = {    // fully resolved name: "Mod.Tree.node"       (disallowed collision)
               value: a,                   // fully resolved name: "Mod.Tree.node.value"
               left : Mod:Tree a,          // fully resolved name: "Mod.Tree.node.left"
               right : Mod:Tree a          // fully resolved name: "Mod.Tree.node.right"
            };
          }
        """,
      )

      check(negativeTestCase)
      forEvery(positiveTestCases)(positiveTestCase =>
        an[ECollision] shouldBe thrownBy(check(positiveTestCase))
      )

    }

    "detect collision between template choices" in {

      val negativeTestCase =
        p"""
        module Mod {                     // fully resolved name: "Mod"

          template (this: T) = {
            precondition True;
            signatories Nil @Party;
            observers Nil @Party;
            agreement "Agreement";
            choice Choice1 (self) (u:Unit) : Unit  // fully resolved name: "Mod.T.Choice1"
              , controllers Nil @Party
              to upure @Unit ();
            choice Choice2 (self) (u:Unit) : Unit  // fully resolved name: "Mod.T.Choice2"
              , controllers Nil @Party
              to upure @Unit ();
          } ;

        }
        """

      val positiveTestCase =
        p"""
        module Mod {                     // fully resolved name: "Mod"

         template (this: T) = {
            precondition True;
            signatories Nil @Party;
            observers Nil @Party;
            agreement "Agreement";
            choice Choice (self) (u:Unit) : Unit  // fully resolved name: "Mod.T.Choice"
              , controllers Nil @Party
              to upure @Unit ();
            choice CHOICE (self) (u:Unit) : Unit  // fully resolved name: "Mod.T.CHOICE"
              , controllers Nil @Party
              to upure @Unit ();
          } ;

        }
        """

      check(negativeTestCase)
      an[ECollision] shouldBe thrownBy(check(positiveTestCase))

    }

    "detect collision between interface choices" in {

      val negativeTestCase =
        p"""
        module Mod {                     // fully resolved name: "Mod"

          record @serializable MyUnit = {};

          interface (this: I) = {
            viewtype Mod:MyUnit;
             choice Choice1 (self) (u:Unit) : Unit  // fully resolved name: "Mod.I.Choice1"
              , controllers Nil @Party
              to upure @Unit ();
            choice Choice2 (self) (u:Unit) : Unit  // fully resolved name: "Mod.I.Choice2"
              , controllers Nil @Party
              to upure @Unit ();
          } ;

        }
        """

      val positiveTestCase =
        p"""
        module Mod {                     // fully resolved name: "Mod"

          record @serializable MyUnit = {};

          interface (this: I) = {
            viewtype Mod:MyUnit;
             choice CHOICE (self) (u:Unit) : Unit  // fully resolved name: "Mod.I.Choice"
              , controllers Nil @Party
              to upure @Unit ();
            choice Choice (self) (u:Unit) : Unit  // fully resolved name: "Mod.I.CHOICE"
              , controllers Nil @Party
              to upure @Unit ();
          } ;

        }
        """

      check(negativeTestCase)
      an[ECollision] shouldBe thrownBy(check(positiveTestCase))

    }

    "do not consider inherited choices for collision" in {

      val testCase = p"""
        module Mod {

          record @serializable MyUnit = {};

          interface (this: I1) = {
             viewtype Mod:MyUnit;
             choice Choice (self) (u:Unit) : Unit
              , controllers Nil @Party
              to upure @Unit ();
          };

          interface (this: I2) = {
             viewtype Mod:MyUnit;
             choice Choice (self) (u:Unit) : Unit
              , controllers Nil @Party
              to upure @Unit ();
          };

          template (this: T) = {
            precondition True;
            signatories Nil @Party;
            observers Nil @Party;
            agreement "Agreement";
            choice Choice (self) (u:Unit) : Unit
              , controllers Nil @Party
              to upure @Unit ();
            implements Mod:I1{
              view = Mod:MyUnit {};
            };
            implements Mod:I2{
              view = Mod:MyUnit {};
            };
          } ;

        }
        """

      check(testCase)

    }

  }

}
