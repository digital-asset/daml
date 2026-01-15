// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.*
import com.digitalasset.transcode.schema.DynamicValue as DV

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

trait Primitives extends TestCase:
  addCase(
    unit,
    DV.Unit,
  )
  addCase(
    bool,
    DV.Bool(true),
    DV.Bool(false),
  )
  addCase(
    text,
    DV.Text(""),
    DV.Text("Text"),
    DV.Text("\u0001"),
    DV.Text("1\n2\t3\r4"),
    DV.Text((for (i <- 1 to 255) yield i.toChar).mkString),
  )
  addCase(
    int64,
    DV.Int64(0),
    DV.Int64(Long.MinValue),
    DV.Int64(Long.MaxValue),
    DV.Int64(-6332396039700514374L),
    DV.Int64(585828734761349480L),
  )
  addCase(
    numeric(10),
    DV.Numeric("0.0000000000"),
    DV.Numeric("0.1000000000"),
  )
  addCase(
    numeric(1),
    DV.Numeric("0.0"),
    DV.Numeric("0.1"),
  )
  addCase(
    timestamp,
    DV.Timestamp(0),
    DV.Timestamp(1),
    DV.Timestamp(-1),
    DV.Timestamp(
      LocalDateTime.of(1, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC).longValue * 1_000_000
    ),
    DV.Timestamp(
      LocalDateTime.of(10000, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC).longValue * 1_000_000 - 1
    ),
    DV.Timestamp(
      LocalDateTime
        .of(1969, 12, 31, 23, 59, 59)
        .toEpochSecond(ZoneOffset.UTC)
        .longValue * 1_000_000 + 1
    ),
    DV.Timestamp(
      LocalDateTime.of(1969, 12, 31, 23, 59, 59).toEpochSecond(ZoneOffset.UTC).longValue * 1_000_000
    ),
    DV.Timestamp(
      LocalDateTime.of(2000, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC).longValue * 1_000_000 + 1
    ),
  )
  addCase(
    date,
    DV.Date(0),
    DV.Date(1),
    DV.Date(-1),
    DV.Date(LocalDate.of(1, 1, 1).toEpochDay.toInt),
    DV.Date(LocalDate.of(9999, 12, 31).toEpochDay.toInt),
    DV.Date(LocalDate.of(2000, 1, 1).toEpochDay.toInt),
    DV.Date(LocalDate.of(2038, 1, 20).toEpochDay.toInt),
    DV.Date(LocalDate.of(2100, 1, 1).toEpochDay.toInt),
  )
  addCase(
    party,
    DV.Party("Alice"),
  )
  addCase(
    contractId(constructor(RoundtripId, record("party" -> party))),
    DV.ContractId("0" * 2 + "1" * 136),
  )
