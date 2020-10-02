// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.services

import cats.data.NonEmptyList
import cats.implicits._

import doobie._
import io.circe._
import io.circe.parser._

import org.postgresql.util.PGobject

trait Types {
  implicit val jsonGet: Get[Json] =
    Get.Advanced.other[PGobject](NonEmptyList.of("jsonb")).tmap[Json] { o =>
      parse(o.getValue).leftMap[Json](throw _).merge
    }

  case class ContractResult(
      event_id: String,
      archived_by_event_id: Option[String],
      contract_id: String,
      transaction_id: String,
      archived_by_transaction_id: Option[String],
      is_root_event: Boolean,
      package_id: String,
      template: String,
      create_arguments: Json,
      stakeholders: Json
  )

  case class TransactionResult(
      transaction_id: String,
      seq: Int,
      workflow_id: String,
      effective_at: java.sql.Timestamp,
      extracted_at: java.sql.Timestamp,
      ledger_offset: String
  )

  case class ExerciseResult(
      event_id: String,
      transaction_id: String,
      is_root_event: Boolean,
      contract_id: String,
      package_id: String,
      template: String,
      choice: String,
      choice_argument: Json,
      acting_parties: Json,
      consuming: Boolean,
      witness_parties: Json,
      child_event_ids: Json
  )
}

object Types extends Types
