// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.dbbackend

import doobie._
import doobie.implicits._
import spray.json._

object Queries {
  import Implicits._

  def dropTableIfExists(table: String): Fragment = Fragment.const(s"DROP TABLE IF EXISTS ${table}")

  // NB: #, order of arguments must match createContractsTable
  final case class DBContract[CA, WP](
      contractId: String,
      packageId: String,
      templateModuleName: String,
      templateEntityName: String,
      createArguments: CA,
      witnessParties: WP)

  val createContractsTable: Fragment = sql"""
      CREATE TABLE
        contract
        (contract_id TEXT PRIMARY KEY NOT NULL
        ,package_id TEXT NOT NULL
        ,template_module_name TEXT NOT NULL
        ,template_entity_name TEXT NOT NULL
        ,create_arguments JSONB NOT NULL
        ,witness_parties JSONB NOT NULL
        )
    """

  val indexContractsTable: Fragment = sql"""
      CREATE INDEX ON contract (package_id, template_module_name, template_entity_name)
    """

  final case class DBOffset(
      party: String,
      packageId: String,
      templateModuleName: String,
      templateEntityName: String,
      lastOffset: String)

  val createOffsetTable: Fragment = sql"""
      CREATE TABLE
        ledger_offsets
        (party TEXT NOT NULL
        ,package_id TEXT NOT NULL
        ,template_module_name TEXT NOT NULL
        ,template_entity_name TEXT NOT NULL
        ,last_offset TEXT NOT NULL
        )
    """

  def insertContract[CA: JsonWriter, WP: JsonWriter](dbc: DBContract[CA, WP]): Fragment =
    Update[DBContract[JsValue, JsValue]]("""
        INSERT INTO contract
        VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb)
      """).toFragment(
      dbc.copy(
        createArguments = dbc.createArguments.toJson,
        witnessParties = dbc.witnessParties.toJson))

  object Implicits {
    implicit val `JsValue put`: Put[JsValue] =
      Put[String].tcontramap(_.compactPrint)
  }
}
