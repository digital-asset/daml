// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import java.time.Duration

import akka.actor.Scheduler
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.binding.DomainTransactionMapper.DecoderType
import com.digitalasset.ledger.client.binding.config.{LedgerClientConfig, Positive, RetryConfig}
import com.digitalasset.ledger.client.binding.retrying.RetryHelper
import com.digitalasset.ledger.client.configuration.{LedgerClientConfiguration, LedgerIdRequirement}
import com.digitalasset.sandbox.SandboxFixture.SandboxContext

import scala.concurrent.{ExecutionContext, Future}

object SandboxBinding {

  private val bindingRetryTimeout = Duration.ofMinutes(1)

  def binding(
      sandboxContext: SandboxContext,
      config: LedgerClientConfig,
      applicationId: String,
      decoder: DecoderType)(
      implicit ec: ExecutionContext,
      s: Scheduler,
      esf: ExecutionSequencerFactory): Future[LedgerClientBinding] = {
    val configWithoutLedgerId = config.toBindingConfig(applicationId)
    val host = sandboxContext.host
    val port = sandboxContext.port
    val channel = LedgerClientBinding.createChannel(host, port, configWithoutLedgerId.sslContext)

    for {
      ledgerId <- retry(LedgerClientBinding.askLedgerId(channel, configWithoutLedgerId))

      configWithLedgerId = configWithoutLedgerId.copy(
        ledgerIdRequirement = LedgerIdRequirement(ledgerId, enabled = true))

      ledgerClient <- retry(LedgerClient.forChannel(configWithLedgerId, channel))
    } yield
      new LedgerClientBinding(
        ledgerClient,
        configWithLedgerId,
        channel,
        bindingRetryTimeout,
        TimeProvider.UTC,
        decoder)
  }

  def ledgerClient(sandboxContext: SandboxContext, config: LedgerClientConfiguration)(
      implicit ec: ExecutionContext,
      s: Scheduler,
      esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    val host = sandboxContext.host
    val port = sandboxContext.port
    val channel = LedgerClientBinding.createChannel(host, port, config.sslContext)

    for {
      lid <- retry(LedgerClientBinding.askLedgerId(channel, config))
      c = config.copy(ledgerIdRequirement = config.ledgerIdRequirement.copy(ledgerId = lid))
      ledgerClient <- retry(LedgerClient.forChannel(c, channel))
    } yield ledgerClient
  }

  private def retry[T](f: => Future[T])(implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    RetryHelper.retry(RETRY_CONFIG_OPTION)(RetryHelper.always)(f)

  private val RETRY_CONFIG = RetryConfig(
    intervalMs = Positive.unsafe(500),
    timeoutMs = Positive.unsafe(30000)
  )
  private val RETRY_CONFIG_OPTION = Some(RETRY_CONFIG)

}
