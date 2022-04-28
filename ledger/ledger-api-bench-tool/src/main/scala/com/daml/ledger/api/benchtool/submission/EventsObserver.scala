// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.submission.EventsObserver._
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.transaction_service.GetTransactionTreesResponse
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object EventsObserver {

  def apply(expectedTemplateNames: Set[String]): EventsObserver = new EventsObserver(
    logger = LoggerFactory.getLogger(getClass),
    expectedTemplateNames = expectedTemplateNames,
  )

  case class ObservedExerciseEvent(
      templateName: String,
      choiceName: String,
      choiceArgumentsSerializedSize: Int,
      consuming: Boolean,
  )

  case class ObservedCreateEvent(templateName: String, createArgumentsSerializedSize: Int)

  case class ObservedEvents(
      expectedTemplateNames: Set[String],
      createEvents: Seq[ObservedCreateEvent],
      exerciseEvents: Seq[ObservedExerciseEvent],
  ) {
    private val _actualTemplateNames =
      (createEvents.map(_.templateName) ++ exerciseEvents.map(_.templateName)).toSet
    require(
      _actualTemplateNames.subsetOf(expectedTemplateNames),
      s"${_actualTemplateNames} must be a subset of $expectedTemplateNames",
    )

    val consumingExercises: Seq[ObservedExerciseEvent] = exerciseEvents.filter(_.consuming)
    val nonConsumingExercises: Seq[ObservedExerciseEvent] = exerciseEvents.filterNot(_.consuming)

    val avgSizeOfConsumingExercise: Int = {
      if (consumingExercises.isEmpty) 0
      else consumingExercises.map(_.choiceArgumentsSerializedSize).sum / consumingExercises.size
    }

    val avgSizeOfNonconsumingExercise: Int = {
      if (nonConsumingExercises.isEmpty) 0
      else
        nonConsumingExercises.map(_.choiceArgumentsSerializedSize).sum / nonConsumingExercises.size
    }

    val numberOfCreatesPerTemplateName: Map[String, Int] = {
      val groups = createEvents.groupBy(_.templateName)
      expectedTemplateNames.map(name => name -> groups.get(name).fold(0)(_.size)).toMap
    }

    val avgSizeOfCreateEventPerTemplateName: Map[String, Int] = {
      val groups = createEvents.groupBy(_.templateName)
      expectedTemplateNames.map { name =>
        val avgSize = groups
          .get(name)
          .fold(0)(events =>
            if (events.isEmpty) 0 else events.map(_.createArgumentsSerializedSize).sum / events.size
          )
        name -> avgSize
      }.toMap
    }

  }

}

/** Collects information about create and exercise events.
  */
class EventsObserver(expectedTemplateNames: Set[String], logger: Logger)
    extends ObserverWithResult[GetTransactionTreesResponse, ObservedEvents](logger) {

  private val createEvents = collection.mutable.ArrayBuffer[ObservedCreateEvent]()
  private val exerciseEvents = collection.mutable.ArrayBuffer[ObservedExerciseEvent]()

  override def streamName: String = "dummy-stream-name"

  override def onNext(value: GetTransactionTreesResponse): Unit = {
    value.transactions.foreach { transaction: TransactionTree =>
      val rootEvents = transaction.rootEventIds.map(transaction.eventsById)
      rootEvents.foreach { event: TreeEvent =>
        event.kind.created.foreach { created: CreatedEvent =>
          val argsSize = created.createArguments.fold(0)(_.serializedSize)
          val templateName =
            created.templateId.getOrElse(sys.error(s"Expected templateId in $created")).entityName
          createEvents.addOne(
            ObservedCreateEvent(
              templateName = templateName,
              createArgumentsSerializedSize = argsSize,
            )
          )
        }
        event.kind.exercised.foreach { exercised =>
          val argsSize = exercised.choiceArgument.fold(0)(_.serializedSize)
          val templateName = exercised.templateId
            .getOrElse(sys.error(s"Expected templateId in $exercised"))
            .entityName
          val choiceName = exercised.choice
          exerciseEvents.addOne(
            ObservedExerciseEvent(
              templateName = templateName,
              choiceName = choiceName,
              choiceArgumentsSerializedSize = argsSize,
              consuming = exercised.consuming,
            )
          )
        }
      }
    }
  }

  override def completeWith(): Future[ObservedEvents] = Future.successful(
    ObservedEvents(
      expectedTemplateNames = expectedTemplateNames,
      createEvents = createEvents.toList,
      exerciseEvents = exerciseEvents.toList,
    )
  )
}
