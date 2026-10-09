// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.interning.DomainStringIterators
import com.digitalasset.canton.topology.SynchronizerId

object DbDtoToStringsForInterning {

  def apply(dbDtos: Iterable[DbDto]): DomainStringIterators =
    new DomainStringIterators(
      templateIds = dbDtos.iterator.flatMap(templateIdsOf),
      parties = dbDtos.iterator.flatMap(partiesOf),
      synchronizerIds = dbDtos.iterator.flatMap(synchronizerIdsOf),
      packageIds = dbDtos.iterator.flatMap(packageIdsOf),
      userIds = dbDtos.iterator.flatMap(userIdsOf),
      participantIds = dbDtos.iterator.flatMap(participantIdsOf),
      choiceNames = dbDtos.iterator.flatMap(choiceNamesOf),
      interfaceIds = dbDtos.iterator.flatMap(interfaceIdsOf),
    )

  private def templateIdsOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventDeactivate => Iterator(dbDto.template_id)
      case dbDto: DbDto.EventVariousWitnessed => dbDto.template_id.iterator
      case dbDto: DbDto.IdFilterDbDto => Iterator(dbDto.idFilter.template_id)
      case _ => Iterator.empty
    }

  private def packageIdsOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventActivate => Iterator(dbDto.representative_package_id)
      case dbDto: DbDto.EventDeactivate => Iterator(dbDto.package_id)
      case dbDto: DbDto.EventVariousWitnessed =>
        dbDto.package_id.iterator ++ dbDto.representative_package_id.iterator
      case _ => Iterator.empty
    }

  private def partiesOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventActivate =>
        dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.additional_witnesses.getOrElse(Set.empty).iterator

      case dbDto: DbDto.EventDeactivate =>
        dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.additional_witnesses.getOrElse(Set.empty).iterator ++
          dbDto.exercise_actors.getOrElse(Set.empty).iterator ++
          dbDto.stakeholders.iterator

      case dbDto: DbDto.EventVariousWitnessed =>
        dbDto.submitters.getOrElse(Set.empty).iterator ++
          dbDto.additional_witnesses.iterator ++
          dbDto.exercise_actors.getOrElse(Set.empty).iterator

      case dbDto: DbDto.IdFilterDbDto =>
        Iterator(dbDto.idFilter.party_id)

      case dbDto: DbDto.CommandCompletion =>
        dbDto.submitters.iterator

      case dbDto: DbDto.PartyEntry =>
        // Party identifiers not only interned on demand: we also intern as we see parties created,
        // since this information is stored in the lapi_party_entries as well
        dbDto.party.iterator

      case dbDto: DbDto.EventPartyToParticipant =>
        Iterator(dbDto.party_id)

      case _ => Iterator.empty
    }

  private def synchronizerIdsOf(dbDto: DbDto): Iterator[SynchronizerId] =
    dbDto match {
      case dbDto: DbDto.EventActivate =>
        Iterator(dbDto.synchronizer_id) ++ dbDto.source_synchronizer_id.iterator
      case dbDto: DbDto.EventDeactivate =>
        Iterator(dbDto.synchronizer_id) ++ dbDto.target_synchronizer_id.iterator
      case dbDto: DbDto.EventVariousWitnessed => Iterator(dbDto.synchronizer_id)
      case dbDto: DbDto.EventPartyToParticipant => Iterator(dbDto.synchronizer_id)
      case dbDto: DbDto.CommandCompletion => Iterator(dbDto.synchronizer_id)
      case dbDto: DbDto.SequencerIndexMoved => Iterator(dbDto.synchronizerId)
      case dbDto: DbDto.TransactionMeta => Iterator(dbDto.synchronizer_id)
      case _ => Iterator.empty
    }

  private def userIdsOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.CommandCompletion => Iterator(dbDto.user_id)
      case _ => Iterator.empty
    }

  private def participantIdsOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventPartyToParticipant => Iterator(dbDto.participant_id)
      case _ => Iterator.empty
    }

  private def choiceNamesOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventDeactivate => dbDto.exercise_choice.iterator
      case dbDto: DbDto.EventVariousWitnessed => dbDto.exercise_choice.iterator
      case _ => Iterator.empty
    }

  private def interfaceIdsOf(dbDto: DbDto): Iterator[String] =
    dbDto match {
      case dbDto: DbDto.EventDeactivate => dbDto.exercise_choice_interface_id.iterator
      case dbDto: DbDto.EventVariousWitnessed => dbDto.exercise_choice_interface_id.iterator
      case _ => Iterator.empty
    }
}
