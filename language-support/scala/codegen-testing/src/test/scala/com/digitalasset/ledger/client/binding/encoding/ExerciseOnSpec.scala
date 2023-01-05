// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding
package encoding

import com.daml.ledger.client.binding.{Primitive => P}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

@nowarn("msg=local method exer .* is never used") // testing typechecking only
class ExerciseOnSpec extends AnyWordSpec with Matchers {
  import ExerciseOnSpec._

  val owner: P.Party = P.Party("owner")

  "ids" should {
    "select an instance" in {
      def exer(id: P.ContractId[Sth]) = id.exerciseFoo(owner)
    }

    "select an instance, even if subtype" in {
      def exer[T <: Sth.ContractId](id: T) = id.exerciseFoo(owner)
    }

    "select an instance, even if singleton type" in {
      def exer(id: Sth.ContractId) =
        Sth.`Sth syntax`(id).exerciseFoo(owner)
    }

    "still select an instance if imported" in {
      import Sth._
      def exer(id: Sth.ContractId) = id.exerciseFoo(owner)
    }

    "discriminate among template types" in {
      import LfTypeEncodingSpec.CallablePayout
      import Sth._
      def exer(id: CallablePayout.ContractId) =
        id: `Sth syntax`[CallablePayout.ContractId]
      // ^ works, but...
      "(id: CallablePayout.ContractId) => id.exerciseFoo(owner)" shouldNot typeCheck
    }
  }

  "untyped ids" should {
    "not find an instance, even if syntax imported" in {
      import Sth._
      (): `Sth syntax`[Unit] // suppress unused import warning
      "(id: P.ContractId[Any]) => id.exerciseFoo(owner)" shouldNot typeCheck
    }
  }

  "templates" should {
    "not have an instance (createAnd call required)" in {
      "(sth: Sth) => sth.exerciseFoo(owner)" shouldNot typeCheck
    }
  }

  "createAnds" should {
    "select an instance" in {
      Sth().createAnd.exerciseFoo(owner) shouldBe (())
    }

    "select an instance, even if subtype" in {
      def exer[T <: Sth](id: Template[T]) = id.createAnd.exerciseFoo(owner)
    }
  }
}

object ExerciseOnSpec {
  final case class Sth() extends Template[Sth] {
    protected[this] override def templateCompanion(implicit d: DummyImplicit) = Sth
  }

  object Sth extends TemplateCompanion.Empty[Sth] with (() => Sth) {
    override val id = ` templateId`("Foo", "Bar", "Sth")
    override val onlyInstance = Sth()
    override val consumingChoices = Set()

    /** An example of generated code so we can see how implicit resolution will
      * behave. Do *not* import `Sth syntax`; the whole point is to make sure
      * this all works without doing that.
      */
    implicit final class `Sth syntax`[+ ` ExOn`](private val id: ` ExOn`) extends AnyVal {
      @nowarn(
        "msg=parameter value (controller| exOn) .* is never used"
      ) // used only for arg typechecking
      def exerciseFoo(controller: P.Party)(implicit ` exOn`: ExerciseOn[` ExOn`, Sth]): Unit = ()
    }
  }
}
