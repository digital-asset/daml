// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

import java.time.{Clock, Instant, ZoneId}
import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.digitalasset.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  SetTimeRequest
}
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

private[testtool] object LedgerClock {

  private def timestampToInstant(t: Timestamp): Instant =
    Instant.EPOCH.plusSeconds(t.seconds).plusNanos(t.nanos.toLong)

  private def instantToTimestamp(t: Instant): Timestamp =
    new Timestamp(t.getEpochSecond, t.getNano)

  def apply(ledgerId: String, service: TimeService)(
      implicit ec: ExecutionContext): Future[LedgerClock] = {

    val ledgerTime = new AtomicReference[Option[Instant]](None)
    val clockPromise = Promise[LedgerClock]

    service.getTime(
      new GetTimeRequest(ledgerId),
      new StreamObserver[GetTimeResponse] {
        override def onNext(value: GetTimeResponse): Unit = {
          val previous =
            ledgerTime.getAndUpdate(_ => Some(timestampToInstant(value.currentTime.get)))
          if (previous.isEmpty) {
            val _ = clockPromise.trySuccess(new LedgerClock {
              override def passTime(t: Duration): Future[Unit] = {
                val now = ledgerTime.get()
                val currentTime = now.map(instantToTimestamp)
                val newTime = now.map(_.plusNanos(t.toNanos)).map(instantToTimestamp)
                service.setTime(new SetTimeRequest(ledgerId, currentTime, newTime)).map(_ => ())
              }

              override def instant(): Instant = ledgerTime.get().get
            })
          }
        }
        override def onError(t: Throwable): Unit = {
          val _ = clockPromise.trySuccess(SystemUTC)
        }
        override def onCompleted(): Unit = {
          val _ = clockPromise.trySuccess(SystemUTC)
        }
      }
    )

    clockPromise.future

  }

  object SystemUTC extends LedgerClock {

    override def instant(): Instant = Clock.systemUTC().instant()

    def passTime(t: Duration): Future[Unit] =
      Future.failed(
        new RuntimeException(
          "Time travel is allowed exclusively if the ledger exposes a TimeService"))

  }

}

private[testtool] abstract class LedgerClock private () extends Clock {

  override final def getZone: ZoneId = ZoneId.of("UTC")

  override final def withZone(zoneId: ZoneId): Clock =
    throw new UnsupportedOperationException("The ledger clock timezone cannot be changed")

  def passTime(t: Duration): Future[Unit]

}
