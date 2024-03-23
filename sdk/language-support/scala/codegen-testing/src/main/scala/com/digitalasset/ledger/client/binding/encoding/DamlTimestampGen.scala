// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import java.time.Instant

import com.daml.ledger.client.binding.{Primitive => P}
import org.scalacheck.Gen

object DamlTimestampGen {

  private lazy val genRandomDamlTimestamp: Gen[P.Timestamp] = P.Timestamp.subst(
    Gen.zip(
      Gen.choose(P.Timestamp.MIN.getEpochSecond, P.Timestamp.MAX.getEpochSecond),
      Gen.choose(0L, 999999),
    ) map { case (s, n) => Instant.ofEpochSecond(s, n * 1000) }
  )

  private lazy val genSpecificDamlTimestamp: Gen[P.Timestamp] =
    Gen.oneOf(
      ts("6813-11-03T05:41:04Z"),
      ts("4226-11-05T05:07:48Z"),
      ts("8202-11-07T05:51:35Z"),
      ts("2529-11-06T05:57:36.498937000Z"),
      ts("2529-11-06T05:57:36.498937Z"),
    )

  private def ts(s: String): P.Timestamp =
    P.Timestamp
      .discardNanos(Instant.parse(s))
      .getOrElse(sys.error("expected `P.Timestamp` friendly `Instant`"))

  lazy val genDamlTimestamp: Gen[P.Timestamp] =
    Gen.frequency((5, genRandomDamlTimestamp), (2, genSpecificDamlTimestamp))
}
