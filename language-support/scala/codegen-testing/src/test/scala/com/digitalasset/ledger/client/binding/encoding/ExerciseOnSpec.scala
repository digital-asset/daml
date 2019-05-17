// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding
package encoding

import com.digitalasset.ledger.client.binding.{Primitive => P}

import org.scalatest.{Matchers, WordSpec}

class ExerciseOnSpec extends WordSpec with Matchers {
  import ExerciseOnSpec._

  val owner: P.Party = P.Party("owner")

  "ids" should {
    "select an instance" in {
      (id: Sth.ContractId) => id.exerciseFoo(owner)
    }

    "select an instance, even if subtype" in {
      def exer[T <: Sth.ContractId] = (id: T) => id.exerciseFoo(owner)
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
    implicit final class `Sth syntax`[+` ExOn`](private val id: ` ExOn`) extends AnyVal {
      def exerciseFoo(controller: P.Party)(implicit ` exOn`: ExerciseOn[` ExOn`, Sth]): Unit = ()
    }
  }
}
