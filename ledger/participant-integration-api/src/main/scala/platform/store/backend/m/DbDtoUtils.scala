// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import com.daml.platform.store.backend.DbDto

object DbDtoUtils {

  implicit class DbDtoOps(val dto: DbDto) extends AnyVal {
    def eventSeqId: Long = dto match {
      case dto: DbDto.EventExercise => dto.event_sequential_id
      case dto: DbDto.EventCreate => dto.event_sequential_id
      case dto: DbDto.EventDivulgence => dto.event_sequential_id
      case _ => throw new Exception
    }

    def offset: String = dto match {
      case dto: DbDto.EventExercise => dto.event_offset.get
      case dto: DbDto.EventCreate => dto.event_offset.get
      case dto: DbDto.EventDivulgence => dto.event_offset.get
      case _ => throw new Exception
    }

    def treeEventWitnesses: Set[String] = dto match {
      case dto: DbDto.EventExercise => dto.tree_event_witnesses
      case dto: DbDto.EventCreate => dto.tree_event_witnesses
      case dto: DbDto.EventDivulgence => dto.tree_event_witnesses
      case _ => throw new Exception
    }

    def flatEventWitnesses: Set[String] = dto match {
      case dto: DbDto.EventExercise => dto.flat_event_witnesses
      case dto: DbDto.EventCreate => dto.flat_event_witnesses
      case _ => throw new Exception
    }

    def templateId: String = dto match {
      case dto: DbDto.EventExercise => dto.template_id.get
      case dto: DbDto.EventCreate => dto.template_id.get
      case dto: DbDto.EventDivulgence => dto.template_id.get
      case _ => throw new Exception
    }
  }

}
