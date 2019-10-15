// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalaset.http.dbbackend

import doobie._
import doobie.implicits._
import spray.json._

object Queries {
  import Implicits._

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

  def insertContract[CA: JsonWriter, WP: JsonWriter](
      contractId: String,
      packageId: String,
      moduleName: String,
      entityName: String,
      createArguments: CA,
      witnessParties: WP): Fragment =
    sql"""
        INSERT INTO contract
        VALUES (
          $contractId,
          $packageId,
          $moduleName,
          $entityName,
          ${createArguments.toJson}::jsonb,
          ${witnessParties.toJson}::jsonb
        )
      """

  object Implicits {
    implicit val `JsValue put`: Put[JsValue] =
      Put[String].tcontramap(_.compactPrint)
  }
}
