// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

import java.time.{Clock, Instant, ZoneId}
import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.ledger.api.v1.testing.time_service.{GetTimeRequest, GetTimeResponse}
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver

private[acceptance] final class LedgerClock(ledgerId: String, service: TimeService) extends Clock {

  private[this] val ledgerTime = new AtomicReference[Timestamp]()

  service.getTime(
    new GetTimeRequest(ledgerId),
    new StreamObserver[GetTimeResponse] {
      override def onNext(value: GetTimeResponse): Unit = ledgerTime.set(value.currentTime.get)
      override def onError(t: Throwable): Unit = ledgerTime.set(null)
      override def onCompleted(): Unit = ledgerTime.set(null)
    }
  )

  override def getZone: ZoneId = ZoneId.of("UTC")

  override def withZone(zoneId: ZoneId): Clock =
    throw new UnsupportedOperationException("The ledger clock timezone cannot be changed")

  override def instant(): Instant =
    Option(ledgerTime.get()).fold(Clock.systemUTC().instant())(t =>
      Instant.EPOCH.plusSeconds(t.seconds).plusNanos(t.nanos.toLong))
}
