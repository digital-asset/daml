// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.concurrent

import scala.{concurrent => sc}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import shapeless.test.illTyped

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ExecutionContextSpec extends AnyWordSpec with Matchers {
  import ExecutionContextSpec._

  // In these tests, you can think of the type argument to `theEC` as being like
  // the EC on a Future whose ExecutionContext lookup behavior you are wondering
  // about; the implicit resolution behaves the same.

  "importing only untyped" should {
    import TestImplicits.untyped

    "disallow lookup" in {
      illTyped("theEC[Animal]", "could not find implicit value.*")
    }

    "disallow lookup, even of Any" in {
      illTyped("theEC[Any]", "could not find implicit value.*")
    }

    "allow lookup only for untyped" in {
      implicitly[sc.ExecutionContext] should ===(untyped)
    }
  }

  "importing supertype and subtype" should {
    import TestImplicits.{animal1, Elephant}

    "always prefer the subtype" in {
      theEC[Any] should ===(Elephant)
      theEC[Animal] should ===(Elephant)
      theEC[Elephant] should ===(Elephant)
    }

    "refuse to resolve a separate subtype" in {
      illTyped("theEC[Cat]", "could not find implicit value.*")
    }
  }

  "importing everything" should {
    import TestImplicits._

    "always prefer Nothing" in {
      theEC[Any] should ===(nothing)
      theEC[Animal] should ===(nothing)
    }
  }

  "importing two types with LUB, one lower in hierarchy" should {
    import TestImplicits.{Elephant, Tabby}

    "consider neither more specific" in {
      illTyped("theEC[Animal]", "ambiguous implicit values.*")
    }
  }

  "importing a type and a related singleton type" should {
    import TestImplicits.{Tabby, chiefMouserEC}

    "prefer the singleton" in {
      theEC[Tabby] should ===(chiefMouserEC)
      theEC[ChiefMouser.type] should ===(chiefMouserEC)
    }
  }

  "using intersections" should {
    import TestImplicits.{Elephant, cryptozoology}

    "prefer the intersection" in {
      theEC[Elephant] should ===(cryptozoology)
      theEC[Cat] should ===(cryptozoology)
    }

    "be symmetric" in {
      theEC[Elephant with Cat] should ===(cryptozoology)
      theEC[Cat with Elephant] should ===(cryptozoology)
    }
  }
}

object ExecutionContextSpec {
  def theEC[EC](implicit ec: ExecutionContext[EC]): ec.type = ec

  def fakeEC[EC](name: String): ExecutionContext[EC] =
    ExecutionContext(new sc.ExecutionContext {
      override def toString = s"<the $name fakeEC>"
      override def execute(runnable: Runnable) = sys.error("never use this")
      override def reportFailure(cause: Throwable) = sys.error("could never have failed")
    })

  sealed trait Animal
  sealed trait Elephant extends Animal
  sealed trait Cat extends Animal
  sealed trait Tabby extends Cat
  val ChiefMouser: Tabby = new Tabby {}

  object TestImplicits {
    implicit val untyped: sc.ExecutionContext = fakeEC[Any]("untyped")
    implicit val any: ExecutionContext[Any] = fakeEC[Any]("any")
    implicit val animal1: ExecutionContext[Animal] = fakeEC("animal1")
    implicit val animal2: ExecutionContext[Animal] = fakeEC("animal2")
    implicit val Elephant: ExecutionContext[Elephant] = fakeEC("Elephant")
    implicit val Cat: ExecutionContext[Cat] = fakeEC("Cat")
    implicit val cryptozoology: ExecutionContext[Elephant with Cat] = fakeEC("cryptozoology")
    implicit val Tabby: ExecutionContext[Tabby] = fakeEC("Tabby")
    implicit val chiefMouserEC: ExecutionContext[ChiefMouser.type] = fakeEC("chiefMouserEC")
    implicit val nothing: ExecutionContext[Nothing] = fakeEC("Nothing")
  }
}
