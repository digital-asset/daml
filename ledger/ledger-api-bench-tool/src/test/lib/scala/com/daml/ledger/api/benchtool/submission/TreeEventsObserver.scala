// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.transaction_service.GetTransactionTreesResponse
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object TreeEventsObserver {

  def apply(expectedTemplateNames: Set[String]): TreeEventsObserver = new TreeEventsObserver(
    logger = LoggerFactory.getLogger(getClass),
    expectedTemplateNames = expectedTemplateNames,
  )

}

/** Collects information about create and exercise events.
  */
class TreeEventsObserver(expectedTemplateNames: Set[String], logger: Logger)
    extends ObserverWithResult[GetTransactionTreesResponse, ObservedEvents](logger) {

  private val createEvents = collection.mutable.ArrayBuffer[ObservedCreateEvent]()
  private val exerciseEvents = collection.mutable.ArrayBuffer[ObservedExerciseEvent]()

  override def streamName: String = "dummy-stream-name"

  override def onNext(value: GetTransactionTreesResponse): Unit = {
    for {
      transaction <- value.transactions
      allEvents = transaction.eventsById.values
      event <- allEvents
    } {
      event.kind.created.foreach(created => createEvents.addOne(ObservedCreateEvent(created)))
      event.kind.exercised.foreach(exercised =>
        exerciseEvents.addOne(ObservedExerciseEvent(exercised))
      )
    }
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
