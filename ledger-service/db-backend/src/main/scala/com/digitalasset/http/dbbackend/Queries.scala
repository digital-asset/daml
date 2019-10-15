// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalaset.http.dbbackend

import doobie._
import doobie.implicits._

object Queries {
  val createContractsTable: Fragment = sql"""
      CREATE TABLE
        contract
        (event_id TEXT PRIMARY KEY NOT NULL
        ,archived_by_event_id TEXT DEFAULT NULL
        ,contract_id TEXT NOT NULL
        ,transaction_id TEXT NOT NULL
        ,archived_by_transaction_id TEXT DEFAULT NULL
        ,is_root_event BOOLEAN NOT NULL
        ,package_id TEXT NOT NULL
        ,template TEXT NOT NULL
        ,create_arguments JSONB NOT NULL
        ,witness_parties JSONB NOT NULL
        )
    """
}
