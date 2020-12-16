// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.language.Ast.Package
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.defaultPackageId
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CollisionSpec extends AnyWordSpec with Matchers {

  def check(pkg: Package): Unit =
    Collision.checkPackage(defaultPackageId, pkg)

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

      // a record definition with collision
      val positiveTestCase1 = p"""
          module Mod {                  // fully resolved name: "Mod"
            record R = {                // fully resolved name: "Mod.R."
              field: Int64,             // fully resolved name: "Mod.R.field"  (collision)
              field: Decimal            // fully resolved name: "Mod.R.field"  (collision)
            };
          }
          """

      // a record definition with case-insensitive collision
      val positiveTestCase2 = p"""
          module Mod {                  // fully resolved name: "Mod"
            record R = {                  // fully resolved name: "Mod.R."
              field: Int64,             // fully resolved name: "Mod.R.field"  (collision)
              FiElD: Decimal            // fully resolved name: "Mod.R.FiElD"  (collision)
            };
          }
          """

      check(negativeTestCase)
      an[ECollision] shouldBe thrownBy(check(positiveTestCase1))
      an[ECollision] shouldBe thrownBy(check(positiveTestCase2))
      2
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

      // a variant definition with case sensitive collision
      val positiveTestCase1 = p"""
          module Mod {                 // fully resolved name: "Mod"
            variant V =                // fully resolved name: "Mod.V"
              Variant: Int64           // fully resolved name: "Mod.V.Variant" (collision)
            | Variant: Decimal;        // fully resolved name: "Mod.V.Variant" (collision)
          }
          """

      // a variant definition with case insensitive collision
      val positiveTestCase2 = p"""
          module Mod {                 // fully resolved name: "Mod"
            variant V =                   // fully resolved name: "Mod.V"
              Variant: Int64           // fully resolved name: "Mod.V.Variant" (collision)
            | VARIANT: Decimal;        // fully resolved name: "Mod.V.VARIANT" (collision)
          }
          """

      check(negativeTestCase)
      an[ECollision] shouldBe thrownBy(check(positiveTestCase1))
      an[ECollision] shouldBe thrownBy(check(positiveTestCase2))

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

      // a package with collision: two constructs have the same fully resoled name "A.B"
      val positiveTestCase1 = p"""
          module A {                    // fully resolved name: "A"
            record B = {};              // fully resolved name: "A.B" (collision)
          }

          module A.B {                  // fully resolved name: "A.B" (collision)

          }
        """

      // a package with collision: two constructs have the same fully resoled name "A.B"
      val positiveTestCase2 = p"""
          module A {                    // fully resolved name: "A"
            record B = {};                // fully resolved name: "A.B" (collision)
          }

          module a.B {                  // fully resolved name: "a.B" (collision)

          }
        """

      check(negativeTestCase)
      an[ECollision] shouldBe thrownBy(check(positiveTestCase1))
      an[ECollision] shouldBe thrownBy(check(positiveTestCase2))
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

      // a package with collision: two constructs have the same fully resoled name "A.B.C"
      val positiveTestCase1 = p"""
          module A {                      // fully resolved name: "A"
            record B.C = {};              // fully resolved name: "A.B.C" (collision)
          }

          module A.B {                    // fully resolved name: "A"
            record C = {};                // fully resolved name: "A.B.C" (collision)
          }
        """

      // a package with case insensitive collision: two constructs have the same fully resoled name "A.B.C"
      val positiveTestCase2 = p"""
          module A {                      // fully resolved name: "A"
            record B.C = {};              // fully resolved name: "A.B.C" (collision)
          }

          module a.b {                    // fully resolved name: "a.b"
            record c = {};                // fully resolved name: "a.b.c" (collision)
          }
        """

      check(negativeTestCase)
      an[ECollision] shouldBe thrownBy(check(positiveTestCase1))
      an[ECollision] shouldBe thrownBy(check(positiveTestCase2))
    }

    "allow collision between variant and record in the same module" in {

      // a package containing a disallowed collision between variant constructor and record type
      // variant and record are from different modules
      val positiveTestCase1 = p"""
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
        """

      // a package containing a disallowed collision between variant constructor and record type
      // variant and record have different cases.
      val positiveTestCase2 = p"""
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
        """

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

      an[ECollision] shouldBe thrownBy(check(positiveTestCase1))
      an[ECollision] shouldBe thrownBy(check(positiveTestCase2))
      check(negativeTestCase)

    }

  }

}
