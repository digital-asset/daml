// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.admin.pruning.v30
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.scheduler.Cron.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.util.TryUtil
import org.apache.logging.log4j.core.util.CronExpression
import slick.jdbc.{GetResult, SetParameter}

import java.util.TimeZone
import scala.util.control.NonFatal

/** The schedule captures the maintenance windows available for the associated scheduler.
  *
  * @param cron        Cron determining the start times of the schedule
  * @param maxDuration The maximum duration defines the end time relative to the start time.
  *                    In order to honor the maximum duration, scheduler's need to break up
  *                    long-running tasks in such a way that each "Runnable" task takes a small
  *                    amount of time relative to the maxDuration. The scheduler invokes its
  *                    task as long as the current time falls within maxDuration.
  */
class Schedule(val cron: Cron, val maxDuration: PositiveSeconds)

final case class PruningSchedule(
    override val cron: Cron,
    override val maxDuration: PositiveSeconds,
    retention: PositiveSeconds,
) extends Schedule(cron, maxDuration) {
  def toProtoV30: v30.PruningSchedule = v30.PruningSchedule(
    cron = cron.toProtoPrimitive,
    maxDuration = Some(maxDuration.toProtoPrimitive),
    retention = Some(retention.toProtoPrimitive),
  )
}

object PruningSchedule {
  def fromProtoV30(schedule: v30.PruningSchedule): ParsingResult[PruningSchedule] =
    for {
      cron <- Cron.fromProtoPrimitive(schedule.cron)
      maxDuration <- PositiveSeconds.fromProtoPrimitiveO("maxDuration")(schedule.maxDuration)
      retention <- PositiveSeconds.fromProtoPrimitiveO("retention")(schedule.retention)
    } yield PruningSchedule(cron, maxDuration, retention)

}

/** Canton wrapper for CronExpression
  */
class Cron(val unwrap: CronExpression) {
  override def toString: String = unwrap.getCronExpression
  def toProtoPrimitive: String = toString

  override def equals(other: Any): Boolean =
    other match {
      case that: Cron => this.unwrap.getCronExpression.equals(that.unwrap.getCronExpression)
      case _ => false
    }

  // Cron is meant to identify points in time at which to start activities (rather than time windows or
  // ranges). "getNextValidTimeAfter" returns the next valid to to start an activity at or after the
  // specified timestamp baseline.
  // This wrapper around CronExpression also encapsulates its use of java Date.
  def getNextValidTimeAfter(
      baselineTimestamp: CantonTimestamp
  ): Either[CronNextTimeAfterError, CantonTimestamp] = {
    for {
      jdate <- TryUtil
        .tryCatchInterrupted(baselineTimestamp.toDate)
        .toEither
        .leftMap(t => DateConversionError(t.getMessage))
      // Using `Option` here to convert potential `null` into `None`.
      // We get a `null` when the cron expression does not allow for any
      // next valid time after the specified baseline timestamp.
      nextJDate <- Option(unwrap.getNextValidTimeAfter(jdate))
        .toRight(NoNextValidTimeAfter(baselineTimestamp))
      nextCantonTimestamp <- CantonTimestamp
        .fromDate(nextJDate)
        .leftMap[CronNextTimeAfterError](DateConversionError)
    } yield nextCantonTimestamp
  }
}

object Cron {

  implicit val getResultCron: GetResult[Cron] =
    GetResult(r => new Cron(new CronExpression(r.nextString())))

  implicit def setParameterCron: SetParameter[Cron] =
    (cron, pp) => pp >> String300.tryCreate(cron.toString)

  def tryCreate(rawCron: String): Cron = {
    String300.tryCreate(rawCron).discard // ensure the max length criterion is satisfied
    val cronExpr = new CronExpression(rawCron)
    cronExpr.setTimeZone(TimeZone.getTimeZone("GMT"))
    new Cron(cronExpr)
  }

  def create(rawCron: String): Either[String, Cron] = {
    try {
      Right(tryCreate(rawCron))
    } catch {
      case NonFatal(t) =>
        Left(s"Invalid cron expression \"${rawCron}\": ${t.getMessage}")
    }
  }

  def fromProtoPrimitive(cronP: String): ParsingResult[Cron] =
    create(cronP).leftMap(ValueConversionError("cron", _))

  sealed trait CronNextTimeAfterError {
    def message: String
  }

  final case class NoNextValidTimeAfter(ts: CantonTimestamp) extends CronNextTimeAfterError {
    override def message = s"No next valid time after ${ts} exists."
  }

  final case class DateConversionError(message: String) extends CronNextTimeAfterError

}
