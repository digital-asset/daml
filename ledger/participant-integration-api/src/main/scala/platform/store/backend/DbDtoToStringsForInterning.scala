// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.platform.store.interning.DomainStringIterators

object DbDtoToStringsForInterning {

  def apply(dbDtos: Iterable[DbDto]): DomainStringIterators =
    new DomainStringIterators(
      templateIds = dbDtos.iterator.flatMap(templateIdsOf),
      parties = dbDtos.iterator.flatMap(partiesOf),
    )

  private def templateIdsOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventDivulgence =>
        dbDto.template_id.iterator

      case dbDto: DbDto.EventExercise =>
        dbDto.template_id.iterator

      case dbDto: DbDto.EventCreate =>
        dbDto.template_id.iterator

      case _ => Iterator.empty
    }

  private def partiesOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventDivulgence =>
        dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.tree_event_witnesses.iterator

      case dbDto: DbDto.EventExercise =>
        dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.tree_event_witnesses.iterator ++
          dbDto.exercise_actors.getOrElse(Set.empty).iterator ++
          dbDto.flat_event_witnesses.iterator

      case dbDto: DbDto.EventCreate =>
        dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.tree_event_witnesses.iterator ++
          dbDto.flat_event_witnesses.iterator ++
          dbDto.create_observers.getOrElse(Set.empty).iterator ++
          dbDto.create_signatories.getOrElse(Set.empty).iterator

      case dbDto: DbDto.CommandCompletion =>
        dbDto.submitters.iterator

      case dbDto: DbDto.PartyEntry =>
        // Party identifiers not only interned on demand: we also intern as we see parties created,
        // since this information is stored in the party_entries as well
        dbDto.party.iterator

      case _ => Iterator.empty
    }

}
