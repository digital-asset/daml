// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

import java.time.{Clock, Instant, ZoneId}
import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.ledger.api.v1.testing.time_service.{GetTimeRequest, GetTimeResponse}
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future, Promise}

private[testtool] object LedgerClock {

  private def timestampToInstant(t: Timestamp): Instant =
    Instant.EPOCH.plusSeconds(t.seconds).plusNanos(t.nanos.toLong)

  def apply(ledgerId: String, service: TimeService)(
      implicit ec: ExecutionContext): Future[Clock] = {

    val ledgerTime = new AtomicReference[Instant]()
    val clockPromise = Promise[Clock]

    service.getTime(
      new GetTimeRequest(ledgerId),
      new StreamObserver[GetTimeResponse] {
        override def onNext(value: GetTimeResponse): Unit = {
          ledgerTime.set(LedgerClock.timestampToInstant(value.currentTime.get))
          val _ = clockPromise.trySuccess(new LedgerClock(ledgerTime))
        }
        override def onError(t: Throwable): Unit = {
          val _ = clockPromise.trySuccess(Clock.systemUTC())
        }
        override def onCompleted(): Unit = {
          val _ = clockPromise.trySuccess(Clock.systemUTC())
        }
      }
    )

    clockPromise.future

  }

}

private[testtool] final class LedgerClock private (t: AtomicReference[Instant]) extends Clock {

  override def getZone: ZoneId = ZoneId.of("UTC")

  override def withZone(zoneId: ZoneId): Clock =
    throw new UnsupportedOperationException("The ledger clock timezone cannot be changed")

  override def instant(): Instant = t.get()
}
