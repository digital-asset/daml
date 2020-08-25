// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.dao

import java.util.UUID

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.trigger.{RunningTrigger}

final class InMemoryTriggerDao extends RunningTriggerDao {
  private var triggers: Map[UUID, RunningTrigger] = Map.empty
  private var triggersByParty: Map[Party, Set[UUID]] = Map.empty

  override def addRunningTrigger(t: RunningTrigger): Either[String, Unit] = {
    triggers += t.triggerInstance -> t
    triggersByParty += t.triggerParty -> (triggersByParty.getOrElse(t.triggerParty, Set()) + t.triggerInstance)
    Right(())
  }

  override def removeRunningTrigger(triggerInstance: UUID): Either[String, Boolean] = {
    triggers.get(triggerInstance) match {
      case None => Right(false)
      case Some(t) =>
        triggers -= t.triggerInstance
        triggersByParty += t.triggerParty -> (triggersByParty(t.triggerParty) - t.triggerInstance)
        Right(true)
    }
  }

  override def listRunningTriggers(party: Party): Either[String, Vector[UUID]] = {
    Right(triggersByParty.getOrElse(party, Set()).toVector.sorted)
  }

  // This is only possible when running with persistence. For in-memory mode we do nothing.
  override def persistPackages(dar: Dar[(PackageId, DamlLf.ArchivePayload)]): Either[String, Unit] =
    Right(())

  override def close() = ()
}

object InMemoryTriggerDao {
  def apply(): InMemoryTriggerDao = new InMemoryTriggerDao
}
