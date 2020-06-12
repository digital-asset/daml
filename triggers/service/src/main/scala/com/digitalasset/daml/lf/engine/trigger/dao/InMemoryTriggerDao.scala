// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.dao

import java.util.UUID

import com.daml.lf.engine.trigger.{RunningTrigger, UserCredentials}

class InMemoryTriggerDao extends RunningTriggerDao {
  private var triggers: Map[UUID, RunningTrigger] = Map.empty
  private var triggersByParty: Map[UserCredentials, Set[UUID]] = Map.empty

  override def addRunningTrigger(t: RunningTrigger): Either[String, Unit] = {
    triggers += t.triggerInstance -> t
    triggersByParty += t.credentials -> (triggersByParty.getOrElse(t.credentials, Set()) + t.triggerInstance)
    Right(())
  }

  override def removeRunningTrigger(triggerInstance: UUID): Either[String, Boolean] = {
    triggers.get(triggerInstance) match {
      case None => Right(false)
      case Some(t) =>
        triggers -= t.triggerInstance
        triggersByParty += t.credentials -> (triggersByParty(t.credentials) - t.triggerInstance)
        Right(true)
    }
  }

  override def listRunningTriggers(credentials: UserCredentials): Either[String, Vector[UUID]] = {
    Right(triggersByParty.getOrElse(credentials, Set()).toVector.sorted)
  }
}

object InMemoryTriggerDao {
  def apply(): InMemoryTriggerDao = new InMemoryTriggerDao
}
