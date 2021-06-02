// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.DbDto
import com.daml.scalautil.NeverEqualsOverride

import scala.reflect.ClassTag

private[postgresql] case class PostgresDbBatch(
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

private[postgresql] object PostgresDbBatch {
  def apply(dbDtos: Vector[DbDto]): PostgresDbBatch = {
    def collectWithFilter[T <: DbDto: ClassTag](filter: T => Boolean): Vector[T] =
      dbDtos.collect { case dbDto: T if filter(dbDto) => dbDto }
    def collect[T <: DbDto: ClassTag]: Vector[T] = collectWithFilter[T](_ => true)
    import DbDto._
    import PGSchema._
    PostgresDbBatch(
      eventsBatchDivulgence = eventsDivulgence.prepareData(collect[EventDivulgence]),
      eventsBatchCreate = eventsCreate.prepareData(collect[EventCreate]),
      eventsBatchConsumingExercise =
        eventsConsumingExercise.prepareData(collectWithFilter[EventExercise](_.consuming)),
      eventsBatchNonConsumingExercise =
        eventsConsumingExercise.prepareData(collectWithFilter[EventExercise](!_.consuming)),
      configurationEntriesBatch = configurationEntries.prepareData(collect[ConfigurationEntry]),
      packageEntriesBatch = packageEntries.prepareData(collect[PackageEntry]),
      packagesBatch = packages.prepareData(collect[Package]),
      partiesBatch = parties.prepareData(collect[Party]),
      partyEntriesBatch = partyEntries.prepareData(collect[PartyEntry]),
      commandCompletionsBatch = commandCompletions.prepareData(collect[CommandCompletion]),
      commandDeduplicationBatch = collect[CommandDeduplication].map(_.deduplication_key).toArray,
    )
  }

}
