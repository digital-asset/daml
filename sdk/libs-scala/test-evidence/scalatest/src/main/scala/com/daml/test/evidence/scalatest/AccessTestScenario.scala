// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.scalatest

import org.scalactic.source
import org.scalatest.Tag
import org.scalatest.wordspec.FixtureAnyWordSpec

/** Provides alternative versions `when_`, `can_`, `should_`, `must_` of `when`, `can`, `should`, `must` that expose the description through a variable. I.e.:
  * <pre>
  * "my condition" when_ { condition =>
  *   ...
  * }
  * </pre>
  *
  * This is helpful to avoid duplicating test descriptions when annotating reliability/security/operability tests.
  */
trait AccessTestScenario extends FixtureAnyWordSpec {
  implicit class ScenarioWrapper(str: String) {

    def when_(f: String => Unit)(implicit pos: source.Position): Unit = {
      new WordSpecStringWrapper(str).when(f(str))
    }

    def can_(f: String => Unit)(implicit pos: source.Position): Unit = {
      convertToStringCanWrapper(str).can(f(str))
    }

    def should_(f: String => Unit)(implicit pos: source.Position): Unit = {
      import org.scalatest.matchers.should.Matchers._
      convertToStringShouldWrapper(str).should(f(str))
    }

    def must_(f: String => Unit)(implicit pos: source.Position): Unit = {
      convertToStringMustWrapperForVerb(str).must(f(str))
    }

    def taggedAs_(f: String => Tag): ResultOfTaggedAsInvocationOnString = {
      convertToWordSpecStringWrapper(str).taggedAs(f(str))
    }
  }
}
