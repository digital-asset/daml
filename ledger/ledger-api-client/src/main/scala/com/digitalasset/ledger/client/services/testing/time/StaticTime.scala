// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.testing.time

import akka.stream.UniqueKillSwitch
import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.ledger.api.v1.testing.time_service.SetTimeRequest
import java.time.Instant

import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink}
import akka.stream.{ClosedShape, KillSwitches, Materializer}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.v1.testing.time_service.GetTimeRequest
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.grpc.adapter.ExecutionSequencerFactory

import scala.concurrent.{ExecutionContext, Future}

class StaticTime(
    timeService: TimeService,
    clock: AtomicReference[Instant],
    killSwitch: UniqueKillSwitch,
    ledgerId: String)
    extends TimeProvider
    with AutoCloseable {

  def getCurrentTime: Instant = clock.get

  def timeRequest(instant: Instant) =
    SetTimeRequest(
      ledgerId,
      Some(TimestampConversion.fromInstant(getCurrentTime)),
      Some(TimestampConversion.fromInstant(instant)))

  def setTime(instant: Instant)(implicit ec: ExecutionContext): Future[Unit] = {
    timeService.setTime(timeRequest(instant)).map { _ =>
      val _ = StaticTime.advanceClock(clock, instant)
    }
  }

  override def close(): Unit = killSwitch.shutdown()
}

object StaticTime {
  def advanceClock(clock: AtomicReference[Instant], instant: Instant): Instant = {
    clock.updateAndGet {
      case current if instant isAfter current => instant
      case current => current
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def updatedVia(timeService: TimeService, ledgerId: String)(
      implicit m: Materializer,
      esf: ExecutionSequencerFactory): Future[StaticTime] = {
    val clockRef = new AtomicReference[Instant](Instant.EPOCH)
    val killSwitchExternal = KillSwitches.single[Instant]
    val sinkExternal = Sink.head[Instant]

    RunnableGraph
      .fromGraph {
        GraphDSL.create(killSwitchExternal, sinkExternal) {
          case (killSwitch, futureOfFirstElem) =>
            // We serve this in a future which completes when the first element has passed through.
            // Thus we make sure that the object we serve already received time data from the ledger.
            futureOfFirstElem.map(_ => new StaticTime(timeService, clockRef, killSwitch, ledgerId))(
              DirectExecutionContext)
        } { implicit b => (killSwitch, sinkHead) =>
          import GraphDSL.Implicits._
          val instantSource = b.add(
            ClientAdapter
              .serverStreaming(GetTimeRequest(ledgerId), timeService.getTime)
              .map(r => toInstant(r.getCurrentTime)))

          val updateClock = b.add(Flow[Instant].map { i =>
            advanceClock(clockRef, i)
            i
          })

          val broadcastTimes = b.add(Broadcast[Instant](2))

          val ignore = b.add(Sink.ignore)

          // format: OFF
          instantSource ~> killSwitch ~> updateClock ~> broadcastTimes.in
                                                        broadcastTimes.out(0) ~> sinkHead
                                                        broadcastTimes.out(1) ~> ignore
          // format: ON

          ClosedShape
        }
      }
      .run()
  }

}
