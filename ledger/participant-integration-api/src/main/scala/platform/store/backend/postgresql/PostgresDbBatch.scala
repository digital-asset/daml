// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.DBDTOV1
import com.daml.scalautil.NeverEqualsOverride

case class PostgresDbBatch(
    eventsBatchDivulgence: Array[Array[_]],
    eventsBatchCreate: Array[Array[_]],
    eventsBatchConsumingExercise: Array[Array[_]],
    eventsBatchNonConsumingExercise: Array[Array[_]],
    configurationEntriesBatch: Array[Array[_]],
    packageEntriesBatch: Array[Array[_]],
    packagesBatch: Array[Array[_]],
    partiesBatch: Array[Array[_]],
    partyEntriesBatch: Array[Array[_]],
    commandCompletionsBatch: Array[Array[_]],
    commandDeduplicationBatch: Array[String],
) extends NeverEqualsOverride

object PostgresDbBatch {
  def apply(dbDtos: Vector[DBDTOV1]): PostgresDbBatch =
    PostgresDbBatch(
      eventsBatchDivulgence = PGSchema.eventsDivulgence.prepareData(dbDtos.collect {
        case dbDto: DBDTOV1.EventDivulgence => dbDto
      }),
      eventsBatchCreate = PGSchema.eventsCreate.prepareData(dbDtos.collect {
        case dbDto: DBDTOV1.EventCreate => dbDto
      }),
      eventsBatchConsumingExercise = PGSchema.eventsConsumingExercise.prepareData(dbDtos.collect {
        case dbDto: DBDTOV1.EventExercise if dbDto.consuming => dbDto
      }),
      eventsBatchNonConsumingExercise =
        PGSchema.eventsNonConsumingExercise.prepareData(dbDtos.collect {
          case dbDto: DBDTOV1.EventExercise if !dbDto.consuming => dbDto
        }),
      configurationEntriesBatch = PGSchema.configurationEntries.prepareData(dbDtos.collect {
        case dbDto: DBDTOV1.ConfigurationEntry => dbDto
      }),
      packageEntriesBatch = PGSchema.packageEntries.prepareData(dbDtos.collect {
        case dbDto: DBDTOV1.PackageEntry => dbDto
      }),
      packagesBatch = PGSchema.packages.prepareData(dbDtos.collect { case dbDto: DBDTOV1.Package =>
        dbDto
      }),
      partiesBatch = PGSchema.parties.prepareData(dbDtos.collect { case dbDto: DBDTOV1.Party =>
        dbDto
      }),
      partyEntriesBatch = PGSchema.partyEntries.prepareData(dbDtos.collect {
        case dbDto: DBDTOV1.PartyEntry => dbDto
      }),
      commandCompletionsBatch = PGSchema.commandCompletions.prepareData(dbDtos.collect {
        case dbDto: DBDTOV1.CommandCompletion => dbDto
      }),
      commandDeduplicationBatch = dbDtos.collect { case dbDto: DBDTOV1.CommandDeduplication =>
        dbDto.deduplication_key
      }.toArray,
    )
}
