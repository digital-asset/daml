// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.digitalasset.canton.ledger.api.benchtool.util.ObserverWithResult
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object ActiveContractsObserver {
  def apply(expectedTemplateNames: Set[String]): ActiveContractsObserver =
    new ActiveContractsObserver(
      logger = LoggerFactory.getLogger(getClass),
      expectedTemplateNames = expectedTemplateNames,
    )
}

/** Collects information about create events from ACS.
  */
class ActiveContractsObserver(logger: Logger, expectedTemplateNames: Set[String])
    extends ObserverWithResult[GetActiveContractsResponse, ObservedEvents](logger) {

  private val createEvents = collection.mutable.ArrayBuffer[ObservedCreateEvent]()

  override def streamName: String = "dummy-stream-name"

  override def onNext(value: GetActiveContractsResponse): Unit =
    for {
      ac <- value.contractEntry.activeContract
      event <- ac.createdEvent
    } {
      createEvents.addOne(ObservedCreateEvent(event))
    }

  override def completeWith(): Future[ObservedEvents] = Future.successful(
    ObservedEvents(
      expectedTemplateNames = expectedTemplateNames,
      createEvents = createEvents.toList,
      exerciseEvents = List.empty,
    )
  )
}
