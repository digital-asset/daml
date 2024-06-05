// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.interning.DomainStringIterators

object DbDtoToStringsForInterning {

  def apply(dbDtos: Iterable[DbDto]): DomainStringIterators =
    new DomainStringIterators(
      templateIds = dbDtos.iterator.flatMap(templateIdsOf),
      parties = dbDtos.iterator.flatMap(partiesOf),
      domainIds = dbDtos.iterator.flatMap(domainIdsOf),
      packageNames = dbDtos.iterator.flatMap(packageNamesOf),
      packageVersions = dbDtos.iterator.flatMap(packageVersionsOf),
    )

  private def templateIdsOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventExercise =>
        Iterator(dbDto.template_id)

      case dbDto: DbDto.EventCreate =>
        Iterator(dbDto.template_id)

      case dbDto: DbDto.EventUnassign =>
        Iterator(dbDto.template_id)

      case dbDto: DbDto.EventAssign =>
        Iterator(dbDto.template_id)

      case _ => Iterator.empty
    }

  private def packageNamesOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventCreate => Iterator(dbDto.package_name)
      case dbDto: DbDto.EventAssign => Iterator(dbDto.package_name)
      case dbDto: DbDto.EventExercise => Iterator(dbDto.package_name)
      case dbDto: DbDto.EventUnassign => Iterator(dbDto.package_name)
      case _ => Iterator.empty
    }

  private def packageVersionsOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventCreate => dbDto.package_version.iterator
      case dbDto: DbDto.EventAssign => dbDto.package_version.iterator
      case _ => Iterator.empty
    }

  private def partiesOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventExercise =>
        dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.tree_event_witnesses.iterator ++
          dbDto.exercise_actors.iterator ++
          dbDto.flat_event_witnesses.iterator

      case dbDto: DbDto.EventCreate =>
        dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.tree_event_witnesses.iterator ++
          dbDto.flat_event_witnesses.iterator ++
          dbDto.create_observers.iterator ++
          // dbDto also contains key_maintainers. We don't internize these
          // as they're already included in the signatories set
          dbDto.create_signatories.iterator

      case dbDto: DbDto.EventUnassign =>
        dbDto.submitter.iterator ++
          dbDto.flat_event_witnesses.iterator

      case dbDto: DbDto.EventAssign =>
        dbDto.submitter.iterator ++
          dbDto.flat_event_witnesses.iterator ++
          dbDto.create_observers.iterator ++
          // dbDto also contains key_maintainers. We don't internize these
          // as they're already included in the signatories set
          dbDto.create_signatories.iterator

      case dbDto: DbDto.CommandCompletion =>
        dbDto.submitters.iterator

      case dbDto: DbDto.PartyEntry =>
        // Party identifiers not only interned on demand: we also intern as we see parties created,
        // since this information is stored in the lapi_party_entries as well
        dbDto.party.iterator

      case _ => Iterator.empty
    }

  private def domainIdsOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventExercise => Iterator(dbDto.domain_id)
      case dbDto: DbDto.EventCreate => Iterator(dbDto.domain_id)
      case dbDto: DbDto.EventUnassign => Iterator(dbDto.source_domain_id, dbDto.target_domain_id)
      case dbDto: DbDto.EventAssign => Iterator(dbDto.source_domain_id, dbDto.target_domain_id)
      case dbDto: DbDto.CommandCompletion => Iterator(dbDto.domain_id)
      case _ => Iterator.empty
    }
}
