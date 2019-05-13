// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.{PlatformApplications, RequestedLedgerAPIMode}
import com.digitalasset.platform.apitesting.LedgerFactories.SandboxStore
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import com.digitalasset.platform.testing.{LedgerBackend, MultiResourceBase}
import org.scalatest.AsyncTestSuite

trait MultiLedgerFixture
    extends MultiResourceBase[LedgerBackend, LedgerContext]
    with TestExecutionSequencerFactory {
  self: AsyncTestSuite =>

  protected type Config = PlatformApplications.Config

  protected def Config: PlatformApplications.Config.type = PlatformApplications.Config

  protected def config: Config

  protected def basePort = 6865

  // FIXME: move it to LedgerContext and make it "lazy"
  protected var cachedLedgerContext: Option[(LedgerContext, String)] = None
  def getLedgerId(ctx: LedgerContext): String = {
    cachedLedgerContext match {
      case None =>
        cachedLedgerContext = Some((ctx, ctx.ledgerId))
        cachedLedgerContext.get._2
      case Some((oldCtx, id)) =>
        assert(ctx == oldCtx)
        id
    }
  }

  def getConfiguredLedgerId: Option[String] = {
    config.ledgerId match {
      case RequestedLedgerAPIMode.Static(id) =>
        Some(id)
      case RequestedLedgerAPIMode.Random() =>
        None
      case RequestedLedgerAPIMode.Dynamic() =>
        None
    }
  }

  /** Overriding this provides an easy way to narrow down testing to a single implementation. */
  override protected def fixtureIdsEnabled: Set[LedgerBackend] =
    Set(LedgerBackend.SandboxInMemory, LedgerBackend.SandboxSql)

  override protected def constructResource(
      index: Int,
      fixtureId: LedgerBackend): Resource[LedgerContext] = {
    fixtureId match {
      case LedgerBackend.SandboxInMemory =>
        LedgerFactories.createSandboxResource(getConfiguredLedgerId, config, SandboxStore.InMemory)
      case LedgerBackend.SandboxSql =>
        LedgerFactories.createSandboxResource(getConfiguredLedgerId, config, SandboxStore.Postgres)
      case LedgerBackend.RemoteAPIProxy =>
        LedgerFactories.createRemoteAPIProxyResource(getConfiguredLedgerId, config)
    }
  }
}
