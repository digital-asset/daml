// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.digitalasset.canton.ledger.api.benchtool.util.ObserverWithResult
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object EventsObserver {

  def apply(expectedTemplateNames: Set[String]): EventsObserver =
    new EventsObserver(
      logger = LoggerFactory.getLogger(getClass),
      expectedTemplateNames = expectedTemplateNames,
    )

}

/** Collects information about create and exercise events.
  */
class EventsObserver(expectedTemplateNames: Set[String], logger: Logger)
    extends ObserverWithResult[GetUpdatesResponse, ObservedEvents](logger) {

  private val createEvents = collection.mutable.ArrayBuffer[ObservedCreateEvent]()
  private val exerciseEvents = collection.mutable.ArrayBuffer[ObservedExerciseEvent]()

  override def streamName: String = "dummy-stream-name"

  override def onNext(value: GetUpdatesResponse): Unit =
    for {
      transaction <- value.update.transaction
      allEvents = transaction.events
      event <- allEvents
    } {
      event.event.created.foreach(created =>
        createEvents.addOne(
          ObservedCreateEvent(created)
        )
      )
      event.event.exercised.foreach(exercised =>
        exerciseEvents.addOne(
          ObservedExerciseEvent(exercised, offset = transaction.offset)
        )
      )
    }

  override def completeWith(): Future[ObservedEvents] =
    Future.successful(
      ObservedEvents(
        expectedTemplateNames = expectedTemplateNames,
        createEvents = createEvents.toList,
        exerciseEvents = exerciseEvents.toList,
      )
    )
}
