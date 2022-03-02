// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.dao

import java.io.Closeable
import java.util.UUID

import com.daml.auth.middleware.api.Tagged.{AccessToken, RefreshToken}
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.trigger.RunningTrigger

import scala.concurrent.{ExecutionContext, Future}

trait RunningTriggerDao extends Closeable {
  def addRunningTrigger(t: RunningTrigger)(implicit ec: ExecutionContext): Future[Unit]
  def getRunningTrigger(triggerInstance: UUID)(implicit
      ec: ExecutionContext
  ): Future[Option[RunningTrigger]]
  def updateRunningTriggerToken(
      triggerInstance: UUID,
      accessToken: AccessToken,
      refreshToken: Option[RefreshToken],
  )(implicit ec: ExecutionContext): Future[Unit]
  def removeRunningTrigger(triggerInstance: UUID)(implicit ec: ExecutionContext): Future[Boolean]
  def listRunningTriggers(party: Party)(implicit ec: ExecutionContext): Future[Vector[UUID]]
  def persistPackages(dar: Dar[(PackageId, DamlLf.ArchivePayload)])(implicit
      ec: ExecutionContext
  ): Future[Unit]
}
