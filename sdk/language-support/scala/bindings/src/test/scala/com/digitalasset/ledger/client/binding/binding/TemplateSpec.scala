// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import shapeless.test.illTyped

import com.daml.ledger.client.binding.{Primitive => P}

class TemplateSpec extends AnyWordSpec with Matchers {
  import TemplateSpec._
  // avoid importing CNA to test that implicit resolution is working properly

  "Template subclasses" when {
    "having colliding names" should {
      val cna = Tmpls.CNA("id", 1, 2, 3, 4)
      "resolve 'create' properly" in {
        val c = cna.create
        identity[P.Int64](c)
        illTyped("cna.create: Primitive.Update[Tmpls.CNA]", "(?s).*found[^\n]*Int64.*")
        val u = (cna: Template[Tmpls.CNA]).create
        identity[P.Update[P.ContractId[Tmpls.CNA]]](u)
      }
    }

    "a companion is defined" should {
      "resolve Value implicitly" in {
        implicitly[Value[Tmpls.CNA]]
      }

      "resolve the companion implicitly" in {
        implicitly[TemplateCompanion[Tmpls.CNA]]
      }
    }
  }
}

object TemplateSpec {
  object Tmpls {
    final case class CNA(
        id: P.Text,
        template: P.Int64,
        create: P.Int64,
        namedArguments: P.Int64,
        archive: P.Int64,
    ) extends Template[CNA] {
      protected[this] override def templateCompanion(implicit d: DummyImplicit) = CNA
    }
    // We've already passed the first test, by scalac checking that
    // these don't conflict with inherited names post-erasure.

    object CNA extends TemplateCompanion.Empty[CNA] {
      override protected val onlyInstance = CNA("", 0, 0, 0, 0)
      val id = P.TemplateId("hi", "there", "you")
      val consumingChoices = Set()
    }
  }
}
