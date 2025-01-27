// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import com.google.common.annotations.VisibleForTesting
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

class EnforceVisibleForTestingTest extends AnyWordSpec with Matchers {

  def assertIsErrorEnforceVisibleForTesting(result: WartTestTraverser.Result): Assertion = {
    result.errors.length shouldBe 1
    result.errors.foreach(_ should include(EnforceVisibleForTesting.message))
    succeed
  }

  "EnforceVisibleForTesting" should {
    "permit valid usage of definitions marked with @VisibleForTesting" in {
      val result = WartTestTraverser(EnforceVisibleForTesting) {
        class Foo {

          @VisibleForTesting
          def visibleForTesting: Int = 3

          // Permit usage of visibleForTesting within the same scope
          protected def withinScope: Int = this.visibleForTesting

          // Permit usage of visibleForTesting from the companion object
          def fromCompanionObject: Int = Foo.visibleForTesting
        }

        object Foo {
          @VisibleForTesting
          def visibleForTesting = 3

          // Permit usage of visibleForTesting within the same scope
          def withinScope: Int = visibleForTesting

          // Permit usage of visibleForTesting from the companion class
          def fromCompanionClass: Int = new Foo().visibleForTesting
        }

        object Bar {
          @VisibleForTesting
          def withinVisibleForTesting: Int = Foo.visibleForTesting + new Foo().visibleForTesting

          @SuppressWarnings(Array("com.digitalasset.canton.EnforceVisibleForTesting"))
          def withSuppressWarnings: Int = Foo.visibleForTesting + new Foo().visibleForTesting
        }
        // this is just to avoid the warning "local object Bar in value result is never used
        Bar
      }
      result.errors shouldBe empty
    }

    "permit usage of definitions marked with @VisibleForTesting in the same enclosing scope" in {
      val result = WartTestTraverser(EnforceVisibleForTesting) {
        object Outer {
          @VisibleForTesting
          class InnerVisibleForTesting extends RuntimeException

          class Inner {
            def innerForTesting: Exception = new InnerVisibleForTesting
          }
        }

        new Outer.Inner().innerForTesting
      }
      result.errors shouldBe empty
    }

    "not allow usage of @VisibleForTesting definitions of class members" in {
      val result = WartTestTraverser(EnforceVisibleForTesting) {
        class Foo {
          @VisibleForTesting
          def visibleForTesting: Int = 3

        }
        class Bar {
          def notAllowed = new Foo().visibleForTesting
        }
        // this is just to avoid the warning "local object Bar in value result is never used
        new Bar()
      }
      assertIsErrorEnforceVisibleForTesting(result)
    }

    "not allow usage of @VisibleForTesting definitions of object members" in {
      val result = WartTestTraverser(EnforceVisibleForTesting) {
        object Foo {
          @VisibleForTesting
          def visibleForTesting: Int = 3
        }
        class Bar {
          def notAllowed = Foo.visibleForTesting
        }
        // this is just to avoid the warning "local object Bar in value result is never used
        new Bar()
      }
      assertIsErrorEnforceVisibleForTesting(result)
    }

    "not allow usage of classes annotated with @VisibleForTesting" in {
      val result = WartTestTraverser(EnforceVisibleForTesting) {
        @VisibleForTesting
        class Foo {}
        object Bar {
          def notAllowed = new Foo()
        }
        // this is just to avoid the warning "local object Bar in value result is never used
        Bar
      }
      assertIsErrorEnforceVisibleForTesting(result)
    }

    // Missing test cases because we currently don't have them
    // * @VisibleForTesting type Foo = Int
    // * @VisibleForTesting object Foo

  }
}
