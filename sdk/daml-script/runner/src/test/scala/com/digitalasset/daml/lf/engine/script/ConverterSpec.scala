// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset
package daml.lf
package engine.script

import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.preprocessing.ValueTranslator
import com.digitalasset.daml.lf.engine.script.v2.Converter
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient.TransactionTree
import com.digitalasset.daml.lf.language.PackageInterface
import com.digitalasset.daml.lf.speedy.SValue._
import org.scalatest.{Assertion, Inside}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

final class ConverterSpec extends AsyncFreeSpec with Matchers with Inside {
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

  "translateTransactionTree" - {
    "with an empty time bound" in {
      val timeBoundaries = None

      assertTimeBoundaries(timeBoundaries)
    }

    "with a non-empty time bound" - {
      val t0 = Time.Timestamp.now()
      val t1 = t0.addMicros(1)

      "Timestamp.min == lower bound < upper bound" in {
        val timeBoundaries = Some(Time.Range(Time.Timestamp.MinValue, t0))

        assertTimeBoundaries(timeBoundaries)
      }

      "lower bound < upper bound == Timestamp.max" in {
        val timeBoundaries = Some(Time.Range(t0, Time.Timestamp.MaxValue))

        assertTimeBoundaries(timeBoundaries)
      }

      "Timestamp.min == lower bound < upper bound == Timestamp.max" in {
        val timeBoundaries = Some(Time.Range(Time.Timestamp.MinValue, Time.Timestamp.MaxValue))

        assertTimeBoundaries(timeBoundaries)
      }

      "lower bound == upper bound" in {
        val timeBoundaries = Some(Time.Range(t0, t0))

        assertTimeBoundaries(timeBoundaries)
      }

      "lower bound < upper bound" in {
        val timeBoundaries = Some(Time.Range(t0, t1))

        assertTimeBoundaries(timeBoundaries)
      }
    }

    def assertTimeBoundaries(timeBoundaries: Option[Time.Range]): Assertion = {
      val timeBoundariesName = Ref.Name.assertFromString("timeBoundaries")
      val svalue = Converter.translateTransactionTree(
        { case _: (Ref.Identifier, Option[Ref.Identifier], Ref.ChoiceName) =>
          Left("Unimplemented")
        },
        new ValueTranslator(
          new PackageInterface(PartialFunction.empty),
          requireV1ContractIdSuffix = false,
        ),
        ScriptIds(Ref.PackageId.assertFromString("ledgerTimePackageId"), isLegacy = false),
        TransactionTree(List.empty, timeBoundaries),
      )

      inside(svalue) { case Right(SRecord(_, ImmArray(_, `timeBoundariesName`), boundaryValues)) =>
        inside(boundaryValues.get(1)) {
          case SOptional(None) =>
            timeBoundaries should be(None)

          case SOptional(Some(SRecord(_, ImmArray(minName, maxName), rangeValues))) =>
            minName should be(Ref.Name.assertFromString("min"))
            maxName should be(Ref.Name.assertFromString("max"))

            inside((rangeValues.get(0), rangeValues.get(1))) {
              case (STimestamp(min), STimestamp(max)) =>
                timeBoundaries should be(Some(Time.Range(min, max)))
            }
        }

      }
    }
  }
}
