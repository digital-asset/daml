// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.writers.postgresql

import com.daml.extractor.config.{ExtractorConfig, SnapshotEndSetting, TemplateConfig}
import com.daml.extractor.json.JsonConverters._
import com.daml.extractor.targets.PostgreSQLTarget
import com.daml.extractor.writers.postgresql.DataFormatState.MultiTableState
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import doobie._
import doobie.implicits._
import cats.data.OptionT
import cats.implicits._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.parser._
import scalaz._
import Scalaz._

/**
  * Handles state so Extractor knows where it left off when restarted
  */
object StateHandler {

  case class StartUpParameters(
      target: PostgreSQLTarget,
      from: LedgerOffset,
      to: SnapshotEndSetting,
      party: String,
      templateConfigs: List[TemplateConfig])

  case class Status(
      ledgerId: String,
      startUpParameters: StartUpParameters,
      multiTableState: MultiTableState,
      witnessedPackages: Set[String]
  )

  implicit val statusEncoder: Encoder[Status] = Encoder.forProduct4(
    "ledgerId",
    "startUpParameters",
    "multiTableState",
    "witnessedPackages"
  )(s => (s.ledgerId, s.startUpParameters, s.multiTableState, s.witnessedPackages))

  implicit val statusDecoder: Decoder[Status] = Decoder.forProduct4(
    "ledgerId",
    "startUpParameters",
    "multiTableState",
    "witnessedPackages"
  )(Status.apply)

  import Queries._

  // The version of the state we're currently support.
  // Might change over time, so it's better to version it
  // and deal with backward compatibility specifically
  val version = 1

  def init(): ConnectionIO[Unit] = {
    createStateTable.update.run.void
  }

  def saveStatus(
      ledgerId: String,
      config: ExtractorConfig,
      target: PostgreSQLTarget,
      multiTableState: MultiTableState,
      witnessedPackages: Set[String]
  ): ConnectionIO[Unit] = {
    val currentState = Status(
      ledgerId,
      extractStartUpParams(config, target),
      multiTableState,
      witnessedPackages
    )

    setState("currentStatus", currentState.asJson.noSpaces).update.run *>
      setState("statusVersion", version.toString).update.run.void
  }

  def retrieveStatus: ConnectionIO[Option[String \/ Status]] = {
    (for {
      version <- OptionT(getState("statusVersion").query[Int].option)
      statusString <- OptionT(getState("currentStatus").query[String].option)
    } yield {
      version match {
        case 1 => decodeStatus(statusString)
        case _ =>
          "Invalid version found in the `state` table. The table's content is probably tempered with.".left
      }
    }).value
  }

  private def extractStartUpParams(
      config: ExtractorConfig,
      target: PostgreSQLTarget): StartUpParameters = {
    StartUpParameters(
      target.copy(connectUrl = "**masked**", user = "**masked**", password = "**masked**"),
      config.from,
      config.to,
      config.partySpec,
      config.templateConfigs.toList.sorted
    )
  }

  private def decodeStatus(json: String): String \/ Status = {
    decode[Status](json).fold(e => -\/(e.toString), \/.right)
  }

  def validateArgumentsAgainstStatus(
      previousStatus: Status,
      ledgerId: String,
      config: ExtractorConfig,
      target: PostgreSQLTarget
  ): String \/ Status = {
    val result = for {
      _ <- validateLedgerId(
        previousStatus.ledgerId,
        ledgerId
      )
      _ <- validateParam(
        previousStatus.startUpParameters.from,
        config.from,
        "snapshot start"
      )
      _ <- validateParam(
        previousStatus.startUpParameters.to,
        config.to,
        "snapshot end"
      )
      _ <- validateParam(
        previousStatus.startUpParameters.party,
        config.partySpec,
        "`--party`"
      )
      _ <- validateParam(
        previousStatus.startUpParameters.templateConfigs,
        config.templateConfigs.toList.sorted,
        "`--templates`"
      )
      _ <- validateParam(
        previousStatus.startUpParameters.target.outputFormat,
        target.outputFormat,
        "`--output-format`"
      )
      _ <- validateParam(
        previousStatus.startUpParameters.target.schemaPerPackage,
        target.schemaPerPackage,
        "`--schema-per-package`"
      )
      _ <- validateParam(
        previousStatus.startUpParameters.target.mergeIdentical,
        target.mergeIdentical,
        "`--merge-identical`"
      )
      _ <- validateParam(
        previousStatus.startUpParameters.target.stripPrefix,
        target.stripPrefix,
        "`--strip-prefix`"
      )
    } yield previousStatus

    result.leftMap(
      e =>
        s"Current startup parameters are incompatible with the ones used when Extractor was first started:\n${e}"
    )
  }

  private def validate[A](previous: A, current: A, error: => String): String \/ Unit = {
    if (previous != current) {
      error.left
    } else {
      ().right
    }
  }

  private def validateParam[A](previous: A, current: A, name: String): String \/ Unit = {
    validate(
      previous,
      current,
      s"The current ${name} parameter `${current}` is not equal " +
        s"to the one that was used when extraction started: `${previous}`"
    )
  }

  private def validateLedgerId(previous: String, current: String): String \/ Unit =
    validate(
      previous,
      current,
      s"The current ledger id `${current}` is not equal " +
        s"to the one previously extracted from: `${previous}`"
    )
}
