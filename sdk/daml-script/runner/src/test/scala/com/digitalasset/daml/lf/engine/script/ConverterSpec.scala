// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import org.scalatest.freespec.AsyncFreeSpec
import com.digitalasset.daml.lf.engine.script.v2.Converter
import org.scalatest.matchers.should.Matchers

import scala.util.Random

final class ConverterTest extends AsyncFreeSpec with Matchers {
  "toPartyIdHint" - {
    "with a non-empty hint" - {
      val hint = "foo"

      "and non-empty different requested name, fails" in {
        Converter.toPartyIdHint(
          givenIdHint = hint,
          requestedDisplayName = "bar",
          new Random(0),
        ) should be(Left(s"Requested name 'bar' cannot be different from id hint '$hint'"))
      }
      "and same requested name, uses the hint" in {
        Converter.toPartyIdHint(
          givenIdHint = hint,
          requestedDisplayName = hint,
          new Random(0),
        ) should be(Right(hint))
      }
      "generates the same hint on subsequent calls" in {
        val random = new Random(0)
        val first = Converter.toPartyIdHint(hint, "", random)
        val second = Converter.toPartyIdHint(hint, "", random)
        first should be(second)
      }
    }
    "with an empty hint" - {
      "and non-empty requested name, uses the name as a prefix" in {
        Converter.toPartyIdHint(
          givenIdHint = "",
          requestedDisplayName = "bar",
          new Random(0),
        ) should be(Right("bar-d4d95138"))
      }
      "and empty requested name, makes up a random hint" in {
        Converter.toPartyIdHint(
          givenIdHint = "",
          requestedDisplayName = "",
          new Random(0),
        ) should be(Right("party-d4d95138"))
      }
      "generates unique hints on subsequent calls" in {
        val random = new Random(0)
        val first = Converter.toPartyIdHint("", "", random)
        val second = Converter.toPartyIdHint("", "", random)
        first should not be second
      }
    }
  }
}
