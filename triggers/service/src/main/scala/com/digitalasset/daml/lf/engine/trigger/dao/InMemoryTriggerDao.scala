// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.dao

import java.util.UUID

import com.daml.auth.middleware.api.Tagged.{AccessToken, RefreshToken}
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.trigger.RunningTrigger

import scala.concurrent.{ExecutionContext, Future}

final class InMemoryTriggerDao extends RunningTriggerDao {
  private var triggers: Map[UUID, RunningTrigger] = Map.empty
  private var triggersByParty: Map[Ref.Party, Set[UUID]] = Map.empty

  override def addRunningTrigger(t: RunningTrigger)(implicit ec: ExecutionContext): Future[Unit] =
    Future {
      triggers += t.triggerInstance -> t
      triggersByParty += t.triggerParty -> (triggersByParty.getOrElse(
        t.triggerParty,
        Set(),
      ) + t.triggerInstance)
      ()
    }

  override def getRunningTrigger(
      triggerInstance: UUID
  )(implicit ec: ExecutionContext): Future[Option[RunningTrigger]] = Future {
    triggers.get(triggerInstance)
  }

  override def updateRunningTriggerToken(
      triggerInstance: UUID,
      accessToken: AccessToken,
      refreshToken: Option[RefreshToken],
  )(implicit ec: ExecutionContext): Future[Unit] = Future {
    triggers.get(triggerInstance) match {
      case Some(t) =>
        triggers += (triggerInstance -> t
          .copy(triggerAccessToken = Some(accessToken), triggerRefreshToken = refreshToken))
      case None => ()
    }
  }

  override def removeRunningTrigger(
      triggerInstance: UUID
  )(implicit ec: ExecutionContext): Future[Boolean] = Future {
    triggers.get(triggerInstance) match {
      case None => false
      case Some(t) =>
        triggers -= t.triggerInstance
        triggersByParty += t.triggerParty -> (triggersByParty(t.triggerParty) - t.triggerInstance)
        true
    }
  }

  override def listRunningTriggers(
      party: Ref.Party
  )(implicit ec: ExecutionContext): Future[Vector[UUID]] = Future {
    triggersByParty.getOrElse(party, Set()).toVector.sorted
  }

  // This is only possible when running with persistence. For in-memory mode we do nothing.
  override def persistPackages(dar: Dar[(PackageId, DamlLf.ArchivePayload)])(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    Future.successful(())

  override def close() = ()
}

object InMemoryTriggerDao {
  def apply(): InMemoryTriggerDao = new InMemoryTriggerDao
}
