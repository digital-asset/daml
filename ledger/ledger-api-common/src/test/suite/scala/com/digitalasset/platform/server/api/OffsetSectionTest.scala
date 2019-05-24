// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.ledger.api.domain.LedgerOffset
import com.digitalasset.ledger.api.testing.utils.IsStatusException
import com.digitalasset.platform.server.services.transaction.{OffsetHelper, OffsetSection}
import io.grpc.Status
import org.scalatest.{Inside, Matchers, WordSpec}

import scala.util.{Failure, Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class OffsetSectionTest extends WordSpec with Matchers with Inside {

  private val intOH: OffsetHelper[Int] = new OffsetHelper[Int] {
    def fromOpaque(opaque: LedgerString): Try[Int] = Try(opaque.toInt)

    def getLedgerBeginning: Int = 0

    def getLedgerEnd: Int = Int.MaxValue

    def compare(o1: Int, o2: Int): Int = o1.compareTo(o2)
  }

  private val parse: (LedgerOffset, Option[LedgerOffset]) => Try[OffsetSection[Int]] =
    OffsetSection(_, _)(intOH)

  private val begin = LedgerOffset.LedgerBegin
  private val absolute0 = LedgerOffset.Absolute(Ref.LedgerString.fromLong(0))
  private val absolute1 = LedgerOffset.Absolute(Ref.LedgerString.fromLong(1))
  private val absoluteMax =
    LedgerOffset.Absolute(Ref.LedgerString.assertFromString(Int.MaxValue.toString))
  private val end = LedgerOffset.LedgerEnd

  "OffsetSection" when {

    "passed equal values" should {

      "return an empty section (begin-begin)" in {

        parse(begin, Some(begin)) shouldEqual Success(OffsetSection.Empty)
      }

      "return an empty section (absolute-absolute)" in {

        parse(absolute1, Some(absolute1)) shouldEqual Success(OffsetSection.Empty)
      }

      "return an empty section (end-end)" in {

        parse(end, Some(end)) shouldEqual Success(OffsetSection.Empty)
      }

      "return an empty section (begin-absolute)" in {

        parse(absolute0, Some(begin)) shouldEqual Success(OffsetSection.Empty)
      }

      "return an empty section (absolute-end)" in {

        parse(absoluteMax, Some(end)) shouldEqual Success(OffsetSection.Empty)
      }
    }

    "passed a single value" should {

      "return an open-ended section (begin)" in {

        parse(begin, None) shouldEqual Success(OffsetSection.NonEmpty(0, None))
      }

      "return an open-ended section (absolute)" in {

        parse(LedgerOffset.Absolute(Ref.LedgerString.fromLong(5)), None) shouldEqual Success(
          OffsetSection.NonEmpty(5, None))
      }

      "return an open-ended section (end)" in {

        parse(end, None) shouldEqual Success(OffsetSection.NonEmpty(Int.MaxValue, None))
      }
    }

    "passed decreasing values" should {

      "return a Failure (end-begin)" in {

        inside(parse(end, Some(begin))) {
          case Failure(t) => IsStatusException(Status.Code.INVALID_ARGUMENT)(t)
        }
      }
      "return a Failure (end-absolute)" in {

        inside(parse(end, Some(absolute1))) {
          case Failure(t) => IsStatusException(Status.Code.INVALID_ARGUMENT)(t)
        }
      }
      "return a Failure (absolute-absolute)" in {

        inside(parse(absolute1, Some(absolute0))) {
          case Failure(t) => IsStatusException(Status.Code.INVALID_ARGUMENT)(t)
        }
      }
      "return a Failure (absolute-begin)" in {

        inside(parse(absolute1, Some(begin))) {
          case Failure(t) => IsStatusException(Status.Code.INVALID_ARGUMENT)(t)
        }
      }
    }

    "passed increasing values" should {

      "return the closed section (begin-end)" in {

        parse(begin, Some(end)) shouldEqual Success(OffsetSection.NonEmpty(0, Some(Int.MaxValue)))
      }

      "return the closed section (begin-absolute)" in {

        parse(begin, Some(absolute1)) shouldEqual Success(OffsetSection.NonEmpty(0, Some(1)))
      }

      "return the closed section (absolute-absolute)" in {

        parse(absolute0, Some(absolute1)) shouldEqual Success(OffsetSection.NonEmpty(0, Some(1)))
      }

      "return the closed section (absolute-end)" in {

        parse(absolute1, Some(end)) shouldEqual Success(
          OffsetSection.NonEmpty(1, Some(Int.MaxValue)))
      }
    }
  }

}
