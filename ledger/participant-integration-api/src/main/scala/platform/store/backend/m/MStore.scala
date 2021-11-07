// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap

import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.{DbDto, ParameterStorageBackend}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

// TODO perf-opt: add multiple filters as in dbs based on create filter
// TODO purge memory in the before after
// TODO perfopt non compliant: drop the string interning population
// TODO perfopt non compliant: drop pruning

class MStore {
  @volatile var ledgerEnd: ParameterStorageBackend.LedgerEnd = _
  @volatile var identityParams: ParameterStorageBackend.IdentityParams = _
  @volatile var prunedUpToInclusive: Offset = _
  @volatile var allDivulgedContractsPrunedUpToInclusive: Offset = _
  @volatile var mData: MData = MData()
  val commandSubmissions: mutable.Map[String, Long] = mutable.Map.empty
}

case class MData(
    events: Vector[DbDto] = Vector.empty,
    contractIdIndex: Map[String, Vector[DbDto]] = Map.empty, // to all
    contractKeyIndex: Map[String, Vector[DbDto]] = Map.empty, // to creates
    transactionIdIndex: Map[String, Vector[DbDto]] = Map.empty, // to all

    configurations: Vector[DbDto.ConfigurationEntry] = Vector.empty,
    partyEntries: Vector[DbDto.PartyEntry] = Vector.empty,
    partyIndex: Map[String, Vector[DbDto.PartyEntry]] = Map.empty,
    packages: Map[String, DbDto.Package] = Map.empty,
    packageEntries: Vector[DbDto.PackageEntry] = Vector.empty,
    stringInternings: Vector[DbDto.StringInterningDto] = Vector.empty,
    commandCompletions: Vector[DbDto.CommandCompletion] = Vector.empty,
) {
  def archived(ledgerEndSequentialId: Long)(dto: DbDto): Boolean =
    contractIdIndex
      .get(dto match {
        case dto: DbDto.EventCreate => dto.contract_id
        case dto: DbDto.EventDivulgence => dto.contract_id
        case dto: DbDto.EventExercise => dto.contract_id
        case _ => throw new Exception
      })
      .forall(_.reverseIterator.exists {
        case archive: DbDto.EventExercise if archive.consuming =>
          archive.event_sequential_id <= ledgerEndSequentialId
        case _ => false
      })

  def eventRange(startExclusive: Long, endInclusive: Long): Iterator[DbDto] = {
    val fromInclusive = startExclusive
    // + 1 so it is inclusive
    // - 1 so it is matching the Vector index (the first event_sequential_id is 1)
    val toExclusive = endInclusive
    // + 1 so it is exclusive
    // - 1 so it is matching the Vector index (the first event_sequential_id is 1)

    Range(fromInclusive.toInt, toExclusive.toInt).iterator
      .filter(_ >= 0)
      .filter(_ < events.size)
      .map(events)
  }
}

object MStore {
  val globalMap: scala.collection.concurrent.Map[String, MStore] =
    new ConcurrentHashMap[String, MStore]().asScala
  def apply(connection: Connection): MStore = globalMap(connection.asInstanceOf[MConnection].id)
}
