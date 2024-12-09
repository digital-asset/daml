// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import anorm.SqlParser.array
import anorm.~
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.platform.Key
import com.digitalasset.canton.platform.store.backend.Conversions.{contractId, hashFromHexString}
import com.digitalasset.canton.platform.store.backend.common.ContractStorageBackendTemplate
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.platform.store.interning.StringInterning

import java.sql.Connection

class PostgresContractStorageBackend(
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
) extends ContractStorageBackendTemplate(PostgresQueryStrategy, ledgerEndCache, stringInterning) {

  override final def supportsBatchKeyStateLookups: Boolean = true

  override def keyStates(keys: Seq[Key], validAt: Offset)(
      connection: Connection
  ): Map[Key, KeyState] = if (keys.isEmpty) Map()
  else {
    val resultParser =
      (contractId("contract_id") ~ hashFromHexString("create_key_hash") ~ array[Int](
        "flat_event_witnesses"
      )).map { case cId ~ hash ~ stakeholders =>
        hash -> KeyAssigned(cId, stakeholders.view.map(stringInterning.party.externalize).toSet)
      }.*

    import com.digitalasset.canton.platform.store.backend.Conversions.HashToStatement
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement

    // efficient adaption of the "single lookup query" using postgres specific syntax
    // the unnest will expand the keys into a temporary table. the cross join lateral runs the
    // efficient single key lookup query on each row in the temporary table. this efficient
    // single key lookup will perform a backwards index scan
    // implementing this for oracle is left to the reader as an exercise.
    val res = anorm
      .SQL(s"""
  WITH last_contract_key_create AS (
      SELECT p.contract_id, p.create_key_hash, p.flat_event_witnesses
      FROM UNNEST(ARRAY[{keys}]) AS k(create_key_hash)
    CROSS JOIN LATERAL (
        SELECT *
        FROM participant_events_create p
        WHERE p.create_key_hash = k.create_key_hash
          AND p.event_offset <= {validAt}
        ORDER BY p.event_sequential_id DESC
        LIMIT 1
    ) p
  )
  SELECT contract_id, create_key_hash, flat_event_witnesses
  FROM last_contract_key_create
  WHERE NOT EXISTS (
      SELECT 1
      FROM participant_events_consuming_exercise
      WHERE contract_id = last_contract_key_create.contract_id
      AND event_offset <= {validAt}
  )""")
      .on("keys" -> keys.map(_.hash), "validAt" -> validAt)
      .as(resultParser)(connection)
      .toMap
    keys.map { case (key) =>
      (key, res.getOrElse(key.hash, KeyUnassigned))
    }.toMap
  }

}
