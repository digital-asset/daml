// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore
import com.daml.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.daml.platform.store.backend.{DbDto, ParameterStorageBackend}

import scala.jdk.CollectionConverters._

// TODO perf-opt: add multiple filters as in dbs based on create filter
// TODO purge memory in the before after for persistence testing
// TODO perfopt non compliant: drop the string interning population?
// TODO perfopt non compliant: drop pruning?

case class MStore(
    ledgerEnd: ParameterStorageBackend.LedgerEnd = null,
    identityParams: ParameterStorageBackend.IdentityParams = null,
    prunedUpToInclusive: Offset = null,
    allDivulgedContractsPrunedUpToInclusive: Offset = null,
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
    meteringLedgerEnd: LedgerMeteringEnd = null,
    transactionMetering: Vector[DbDto.TransactionMetering] = Vector.empty,
    participantMetering: Vector[MeteringStore.ParticipantMetering] = Vector.empty,
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
  private class MStoreRef(@volatile var mStore: MStore)

  private val globalMap: scala.collection.concurrent.Map[String, MStoreRef] =
    new ConcurrentHashMap[String, MStoreRef]().asScala

  def apply[T](connection: Connection)(f: MStore => T): T =
    f(globalMap(idFrom(connection)).mStore)

  def update(connection: Connection)(f: MStore => MStore): Unit = {
    val ref = globalMap(idFrom(connection))
    ref.synchronized {
      ref.mStore = f(ref.mStore)
    }
  }

  def create(id: String): Unit = {
    globalMap.putIfAbsent(id, new MStoreRef(MStore()))
    ()
  }

  def remove(id: String): Unit = {
    globalMap.remove(id)
    ()
  }

  private def idFrom(connection: Connection): String = connection.asInstanceOf[MConnection].id
}
