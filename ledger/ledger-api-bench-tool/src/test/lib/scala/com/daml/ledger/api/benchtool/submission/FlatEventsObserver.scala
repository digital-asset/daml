// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.transaction_service.GetTransactionsResponse
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object FlatEventsObserver {
  def apply(expectedTemplateNames: Set[String]): FlatEventsObserver = new FlatEventsObserver(
    logger = LoggerFactory.getLogger(getClass),
    expectedTemplateNames = expectedTemplateNames,
  )
}

/** Collects information about create and exercise events.
  */
class FlatEventsObserver(expectedTemplateNames: Set[String], logger: Logger)
    extends ObserverWithResult[GetTransactionsResponse, ObservedEvents](logger) {

  private val createEvents = collection.mutable.ArrayBuffer[ObservedCreateEvent]()
  private val exerciseEvents = collection.mutable.ArrayBuffer[ObservedExerciseEvent]()

  override def streamName: String = "dummy-stream-name"

  override def onNext(value: GetTransactionsResponse): Unit =
    for {
      transaction <- value.transactions
      event <- transaction.events
      created <- event.event.created.toList
    } {
      createEvents.addOne(ObservedCreateEvent(created))
    }

  override def completeWith(): Future[ObservedEvents] = Future.successful(
    ObservedEvents(
      expectedTemplateNames = expectedTemplateNames,
      createEvents = createEvents.toList,
      exerciseEvents = exerciseEvents.toList,
    )
  )
}
