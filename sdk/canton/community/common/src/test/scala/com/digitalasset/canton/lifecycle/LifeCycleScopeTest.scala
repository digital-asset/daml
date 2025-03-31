// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, AssertionsUtil}

import scala.concurrent.duration.DurationInt

class LifeCycleScopeTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with UnmanagedLifeCycle {
  import LifeCycleScopeTest.*

  "LifeCycleScope" should {
    "accumulate managers" in {
      val managerA = LifeCycleManager.root("managerA", 1.second, loggerFactory)
      val managerB = LifeCycleManager.root("managerB", 1.second, loggerFactory)
      val a = new ManagedA(managerA)
      val b = new ManagedB(managerB)
      val unmanaged = new Unmanaged

      implicit val scope: this.ContextLifeCycleScope = freshLifeCycleScope

      a.call(_.managers) shouldBe Set(managerA)
      b.call(_.managers) shouldBe Set(managerB)

      a.callB(b, _.managers) shouldBe Set(managerA, managerB)

      a.callBIndirect(b, _.managers) shouldBe Set(managerA, managerB)

      b.call { bScope =>
        implicit val scope: this.ContextLifeCycleScope =
          bScope.coerce[ContextLifeCycleScopeDiscriminator]
        a.call(_.managers)
      } shouldBe Set(managerA, managerB)

      unmanaged.call(_.managers) shouldBe Set.empty
      unmanaged.callB(b, _.managers) shouldBe Set(managerB)
      unmanaged.callAB(a, b, _.managers) shouldBe Set(managerA, managerB)

      a.callBViaUnmanaged(unmanaged, b, _.managers) shouldBe Set(managerA, managerB)
    }

    "include own manager for checks around closing" in {
      val managerA = LifeCycleManager.root("managerA", 1.second, loggerFactory)
      val managerB = LifeCycleManager.root("managerB", 1.second, loggerFactory)
      val a = new ManagedA(managerA)
      val b = new ManagedB(managerB)
      managerA.closeAsync().futureValue

      implicit val scope: this.ContextLifeCycleScope = freshLifeCycleScope

      a.callIsClosing shouldBe true
      b.callIsClosing shouldBe false

      a.call { aScope =>
        implicit val scope: this.ContextLifeCycleScope =
          aScope.coerce[ContextLifeCycleScopeDiscriminator]
        b.callIsClosing
      } shouldBe true
    }

    "forbid implicit coercions to own life cycle scope of another object inside UnmanagedLifeCycle" in {
      val manager = LifeCycleManager.root("managerA", 1.second, loggerFactory)
      val a = new ManagedA(manager)
      implicit val scope: this.ContextLifeCycleScope = freshLifeCycleScope

      a.discard
      scope.discard

      assertCompiles("implicitly[a.ContextLifeCycleScope]")
      AssertionsUtil.assertOnTypeError("implicitly[a.OwnLifeCycleScope]")(
        assertImplicitLifeCycleScopeError("a.OwnLifeCycleScopeDiscriminator")
      )
    }

    "forbid implicit coercions to own life cycle scope of another object inside ManagedLifeCycle" in {
      val managerA = LifeCycleManager.root("managerA", 1.second, loggerFactory)
      val a = new ManagedA(managerA)
      a.discard

      object test extends ManagedLifeCycle {
        override protected def manager: LifeCycleManager = managerA

        def test()(implicit scope: ContextLifeCycleScope): Unit = {
          assertCompiles("implicitly[a.ContextLifeCycleScope]")
          AssertionsUtil.assertOnTypeError("implicitly[a.OwnLifeCycleScope]")(
            assertImplicitLifeCycleScopeError("a.OwnLifeCycleScopeDiscriminator")
          )

          testOwn()
        }

        def testOwn()(implicit scope: OwnLifeCycleScope): Unit = {
          scope.discard
          assertCompiles("implicitly[a.ContextLifeCycleScope]")
          AssertionsUtil.assertOnTypeError("implicitly[a.OwnLifeCycleScope]")(
            assertImplicitLifeCycleScopeError("a.OwnLifeCycleScopeDiscriminator")
          )
        }
      }

      implicit val scope: this.ContextLifeCycleScope = freshLifeCycleScope
      test.test()
    }

    "not allow calling a method from the outside that expects its own scope" in {
      val managerB = LifeCycleManager.root("managerB", 1.second, loggerFactory)
      val b = new ManagedB(managerB)
      b.discard

      AssertionsUtil.assertOnTypeError("b.expectOwnScope")(
        assertImplicitLifeCycleScopeError("b.OwnLifeCycleScopeDiscriminator")
      )
    }
  }

  private def assertImplicitLifeCycleScopeError(
      expectedDiscriminator: String
  )(typeError: String): Assertion =
    typeError should include(
      s"Could not find a suitable LifeCycleScope for discriminator $expectedDiscriminator"
    )
}

object LifeCycleScopeTest {

  private final class ManagedA(override protected val manager: LifeCycleManager)
      extends ManagedLifeCycle {

    /** Public methods should take a [[ContextLifeCycleScope]]. */
    def call[A](k: OwnLifeCycleScope => A)(implicit scope: ContextLifeCycleScope): A =
      k(implicitly[OwnLifeCycleScope])

    def callB[A](b: ManagedB, k: LifeCycleScope[?] => A)(implicit
        scope: ContextLifeCycleScope
    ): A =
      b.call(k)

    def callBIndirect[A](b: ManagedB, k: LifeCycleScope[?] => A)(implicit
        scope: ContextLifeCycleScope
    ): A =
      // Transforms the context life cycle scope into the own life cycle scope.
      callBIndirectInternal(b, k)

    /** Private methods should take a [[OwnLifeCycleScope]]. */
    private def callBIndirectInternal[A](b: ManagedB, k: LifeCycleScope[?] => A)(implicit
        scope: OwnLifeCycleScope
    ): A =
      // Coerces the own life cycle scope into b's context lifecycle scope
      b.call(k)

    def callBViaUnmanaged[A](unmanaged: Unmanaged, b: ManagedB, k: LifeCycleScope[?] => A)(implicit
        scope: ContextLifeCycleScope
    ): A =
      unmanaged.callB(b, k)

    def callIsClosing(implicit scope: ContextLifeCycleScope): Boolean = {
      // Unnecessary unit statement to prevent our Scala formatter from removing the block braces.
      // Without block braces, IntelliJ can't find the implicit for `ownScope`, as of March 2025.
      ()
      ownScope.isClosing
    }
  }

  private final class ManagedB(override protected val manager: LifeCycleManager)
      extends ManagedLifeCycle {
    def call[A](k: OwnLifeCycleScope => A)(implicit scope: ContextLifeCycleScope): A =
      k(implicitly[OwnLifeCycleScope])

    def callIsClosing(implicit scope: ContextLifeCycleScope): Boolean = {
      // Unnecessary unit statement to prevent our Scala formatter from removing the block braces.
      // Without block braces, IntelliJ can't find the implicit for `ownScope`, as of March 2025
      ()
      ownScope.isClosing
    }

    def expectOwnScope(implicit scope: OwnLifeCycleScope): Unit =
      scope.discard
  }

  private final class Unmanaged extends UnmanagedLifeCycle {
    def call[A](k: OwnLifeCycleScope => A)(implicit scope: ContextLifeCycleScope): A =
      k(implicitly[OwnLifeCycleScope])

    def callB[A](b: ManagedB, k: LifeCycleScope[?] => A)(implicit scope: ContextLifeCycleScope): A =
      b.call(k)

    def callAB[A](a: ManagedA, b: ManagedB, k: LifeCycleScope[?] => A)(implicit
        scope: ContextLifeCycleScope
    ): A =
      a.callB(b, k)

    def expectOwnScope(implicit scope: OwnLifeCycleScope): Unit =
      scope.discard
  }
}
