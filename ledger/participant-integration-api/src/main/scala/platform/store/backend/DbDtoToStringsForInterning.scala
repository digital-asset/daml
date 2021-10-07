// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

object DbDtoToStringsForInterning {

  def apply(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventDivulgence =>
        dbDto.template_id.iterator ++
          dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.tree_event_witnesses.iterator

      case dbDto: DbDto.EventExercise =>
        dbDto.template_id.iterator ++
          dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.tree_event_witnesses.iterator ++
          dbDto.exercise_actors.getOrElse(Set.empty).iterator ++
          dbDto.flat_event_witnesses.iterator

      case dbDto: DbDto.EventCreate =>
        dbDto.template_id.iterator ++
          dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.tree_event_witnesses.iterator ++
          dbDto.flat_event_witnesses.iterator ++
          dbDto.create_observers.getOrElse(Set.empty).iterator ++
          dbDto.create_signatories.getOrElse(Set.empty).iterator

      case dbDto: DbDto.CommandCompletion =>
        dbDto.submitters.iterator

      case dbDto: DbDto.PartyEntry =>
        dbDto.party.iterator

      case _ => Iterator.empty
    }

}
