// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.variations

import com.daml.tls.TlsVersion
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.tests.ledgerapi.LedgerApiConformanceBase.excludedTests
import com.digitalasset.canton.integration.tests.ledgerapi.SingleVersionLedgerApiConformanceBase
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.ApiUserManagementServiceSuppressionRule
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.config.{ParticipantNodeConfig, TestingTimeServiceConfig}
import com.digitalasset.canton.platform.apiserver.SeedService
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

// Conformance with default settings but with covering buffer size (i.e. big enough) for the in-memory fan-out,
// ensuring that all Ledger API read requests are served from the in-memory fan-out buffer.
sealed abstract class LedgerApiInMemoryFanOutConformanceTestShardedPostgres(shard: Int)
    extends SingleVersionLedgerApiConformanceBase {

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_ { (c: ParticipantNodeConfig) =>
          c
            .focus(_.ledgerApi.userManagementService.enabled)
            .replace(true)
            .focus(_.ledgerApi.indexService.maxTransactionsInMemoryFanOutBufferSize)
            .replace(20000)
            .focus(_.parameters.ledgerApiServer.contractIdSeeding)
            .replace(SeedService.Seeding.Weak)
        }
      )
      .withSetup(setupLedgerApiConformanceEnvironment)

  protected val numShards: Int = 3

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  "A participant with covering in-memory fan-out buffer" can {
    "pass integration tests" in { implicit env =>
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        ledgerApiTestToolPlugin.runShardedSuites(
          shard = shard,
          numShards = numShards,
          exclude = excludedTests,
          concurrentTestRuns = VariationsConformanceTestUtils.ConcurrentTestRuns,
        )
      }
    }
  }
}

class LedgerApiShard0InMemoryFanOutConformanceTestPostgres
    extends LedgerApiInMemoryFanOutConformanceTestShardedPostgres(shard = 0)

class LedgerApiShard1InMemoryFanOutConformanceTestPostgres
    extends LedgerApiInMemoryFanOutConformanceTestShardedPostgres(shard = 1)

class LedgerApiShard2InMemoryFanOutConformanceTestPostgres
    extends LedgerApiInMemoryFanOutConformanceTestShardedPostgres(shard = 2)

// By default, participants are tuned for performance. The buffers and caches used by the participant
// are by default so large that they are not filled by the small amount of data produced by the conformance test.
// We run one conformance test with small buffer/cache sizes to make sure we cover cases where data doesn't fit
// into a cache or where multiple buffers have to be combined.
sealed abstract class LedgerApiTinyBuffersConformanceShardedTestPostgres(shard: Int)
    extends SingleVersionLedgerApiConformanceBase {

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1") { (c: ParticipantNodeConfig) =>
          c
            .focus(_.ledgerApi.userManagementService.enabled)
            .replace(true)
            .focus(_.ledgerApi.userManagementService.maxCacheSize)
            .replace(2)
            .focus(_.parameters.ledgerApiServer.contractIdSeeding)
            .replace(SeedService.Seeding.Weak)
            .focus(_.ledgerApi.indexService.activeContractsServiceStreams.maxIdsPerIdPage)
            .replace(2)
            .focus(
              _.ledgerApi.indexService.activeContractsServiceStreams.maxPayloadsPerPayloadsPage
            )
            .replace(2)
            .focus(_.ledgerApi.indexService.maxContractKeyStateCacheSize)
            .replace(2)
            .focus(_.ledgerApi.indexService.maxContractStateCacheSize)
            .replace(2)
            .focus(_.ledgerApi.indexService.maxTransactionsInMemoryFanOutBufferSize)
            .replace(3)
            .focus(_.ledgerApi.indexService.bufferedStreamsPageSize)
            .replace(1)
        }
      )
      .withSetup(setupLedgerApiConformanceEnvironment)

  protected val numShards: Int = 4

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  "A participant with tiny buffers" can {
    "pass integration tests" in { implicit env =>
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        ledgerApiTestToolPlugin.runShardedSuites(
          shard = shard,
          numShards = numShards,
          exclude = excludedTests,
          concurrentTestRuns = VariationsConformanceTestUtils.ConcurrentTestRuns,
        )
      }
    }
  }
}

class LedgerApiShard0TinyBuffersConformanceTestPostgres
    extends LedgerApiTinyBuffersConformanceShardedTestPostgres(shard = 0)

class LedgerApiShard1TinyBuffersConformanceTestPostgres
    extends LedgerApiTinyBuffersConformanceShardedTestPostgres(shard = 1)

class LedgerApiShard2TinyBuffersConformanceTestPostgres
    extends LedgerApiTinyBuffersConformanceShardedTestPostgres(shard = 2)

class LedgerApiShard3TinyBuffersConformanceTestPostgres
    extends LedgerApiTinyBuffersConformanceShardedTestPostgres(shard = 3)

// Conformance test with the in-memory fan-out, mutable contract state cache and user management cache disabled
// (i.e. cache/buffer sizes set to 0).
trait LedgerApiCachesDisabledConformanceTest extends SingleVersionLedgerApiConformanceBase {

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1") { (c: ParticipantNodeConfig) =>
          c
            .focus(_.ledgerApi.userManagementService.enabled)
            .replace(true)
            .focus(_.ledgerApi.userManagementService.maxCacheSize)
            .replace(0)
            .focus(_.ledgerApi.userManagementService.maxRightsPerUser)
            .replace(100)
            .focus(_.parameters.ledgerApiServer.contractIdSeeding)
            .replace(SeedService.Seeding.Weak)
            .focus(_.ledgerApi.indexService.maxContractKeyStateCacheSize)
            .replace(0)
            .focus(_.ledgerApi.indexService.maxContractStateCacheSize)
            .replace(0)
            .focus(_.ledgerApi.indexService.maxTransactionsInMemoryFanOutBufferSize)
            .replace(0)
        }
      )
      .withSetup(setupLedgerApiConformanceEnvironment)

  "A participant with caches disabled" can {
    "pass integration tests" in { implicit env =>
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        ledgerApiTestToolPlugin.runSuites(
          suites = "SemanticTests,MultiPartySubmissionIT,ExplicitDisclosureIT,CommandServiceIT",
          exclude = Seq(
            // Following value normalisation (https://github.com/digital-asset/daml/pull/19912), this throws a different, equally correct, error
            "CommandServiceIT:CSRefuseBadParameter"
          ),
          concurrency = VariationsConformanceTestUtils.ConcurrentTestRuns,
        )
      }
    }
  }
}

class LedgerApiCachesDisabledConformanceTestPostgres
    extends LedgerApiCachesDisabledConformanceTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

trait LedgerApiStaticTimeConformanceTest extends SingleVersionLedgerApiConformanceBase {

  override def connectedSynchronizersCount = 1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1") { (c: ParticipantNodeConfig) =>
          c
            .focus(_.parameters.ledgerApiServer.contractIdSeeding)
            .replace(SeedService.Seeding.Weak)
            .focus(_.testingTime)
            .replace(
              Some(TestingTimeServiceConfig.MonotonicTime)
            )
        },
        c => c.focus(_.parameters.clock).replace(ClockConfig.SimClock),
      )
      .withSetup(setupLedgerApiConformanceEnvironment)

  "A participant with static time" can {
    "pass integration tests" in { implicit env =>
      // TODO(i12121): Exclusions due to timeouts because with sim clock time doesn't advance on its own
      val exclusions = Seq(
        "CommandServiceIT:CSduplicateSubmitAndWaitBasic",
        "CommandServiceIT:CSduplicateSubmitAndWaitForTransactionId",
        "CommandServiceIT:CSduplicateSubmitAndWaitForTransactionData",
        "CommandServiceIT:CSduplicateSubmitAndWaitForTransactionTree",
        "InteractiveSubmissionServiceIT:ISSPrepareSubmissionExecuteBasic",
        "InteractiveSubmissionServiceIT:ISSPrepareSubmissionFailExecuteOnInvalidSignature",
        "InteractiveSubmissionServiceIT:ISSPrepareSubmissionFailExecuteOnInvalidSignatory",
        "InteractiveSubmissionServiceIT:ISSExecuteSubmissionRequestWithInputContracts",
        "InteractiveSubmissionServiceIT:ISSExecuteSubmissionRequestFailOnEmptyInputContracts",
        "InteractiveSubmissionServiceIT:ISSExecuteSubmissionAndWaitBasic",
        "InteractiveSubmissionServiceIT:ISSPrepareSubmissionFailExecuteAndWaitOnInvalidSignature",
        "CommandDeduplicationIT:SimpleDeduplicationBasic",
        "CommandDeduplicationIT:SimpleDeduplicationCommandClient",
        "CommandDeduplicationIT:DeduplicateSubmitterBasic",
        "CommandDeduplicationIT:DeduplicateSubmitterCommandClient",
        "CommandDeduplicationIT:DeduplicateUsingDurations",
        "CommandDeduplicationIT:DeduplicateUsingOffsets",
        // suites that are irrelevant for the static time test
        "ActiveContractsServiceIT",
        "DeeplyNestedValueIT",
        "IdentityProviderConfigServiceIT",
        "Interface",
        "LimitsIT",
        "MultiPartySubmissionIT",
        "Package",
        "PartyManagement",
        "Transaction",
        "Upgrading",
        "UserManagement",
        "ValueLimitsIT",
        // Following value normalisation (https://github.com/digital-asset/daml/pull/19912), this throws a different, equally correct, error
        "CommandServiceIT:CSRefuseBadParameter",
        "VettingIT",
      )
      loggerFactory.suppress(LogSuppressionRule) {
        ledgerApiTestToolPlugin.runShardedSuites(
          shard = 0,
          numShards = 1,
          exclude = excludedTests ++ exclusions,
          concurrentTestRuns = VariationsConformanceTestUtils.ConcurrentTestRuns,
        )
      }
    }
  }

  // The CantonTimeServiceBackend can produce a warning:
  // WARN  c.d.c.p.l.a.CantonTimeServiceBackend:LedgerApiStaticTimeConformanceTestPostgres/participant=participant1 - Cannot advance clock: Specified current time 1970-01-01T00:00:31.200Z does not match ledger time 1970-01-01T00:00:30.200Z
  // from the test `TimeServiceIT` case `Time advancement can fail when current time is not accurate`
  val LogSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("ApiUserManagementService") ||
      SuppressionRule.LoggerNameContains("ApiTimeService") ||
      SuppressionRule.LoggerNameContains("CantonTimeServiceBackend") &&
      SuppressionRule.Level(Level.WARN)

}

class LedgerApiStaticTimeConformanceTestPostgres extends LedgerApiStaticTimeConformanceTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

trait LedgerApiTlsConformanceBase extends SingleVersionLedgerApiConformanceBase {

  override def connectedSynchronizersCount = 1

  private val certChainFile =
    PemFile(ExistingFile.tryCreate("./enterprise/app/src/test/resources/tls/ledger-api.crt"))
  private val privateKeyFile =
    PemFile(ExistingFile.tryCreate("./enterprise/app/src/test/resources/tls/ledger-api.pem"))
  private val trustCertCollectionFile =
    PemFile(ExistingFile.tryCreate("./enterprise/app/src/test/resources/tls/root-ca.crt"))

  private val tls = TlsServerConfig(
    certChainFile = certChainFile,
    privateKeyFile = privateKeyFile,
    trustCollectionFile = Some(trustCertCollectionFile),
  )

  protected def tlsCerts: Seq[(String, String)] =
    Seq(
      "--client-cert" -> (certChainFile.pemFile.unwrap.getPath + "," + privateKeyFile.pemFile.unwrap.getPath),
      "--cacrt" -> trustCertCollectionFile.pemFile.unwrap.getPath,
    )

  protected def minTlsVersion: TlsVersion.TlsVersion

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.tls)
            .replace(Some(tls.copy(minimumServerProtocolVersion = Some(minTlsVersion.version))))
        )
      )
      .withSetup(setupLedgerApiConformanceEnvironment)
}

// not testing in-memory/H2, as we have observed flaky h2 persistence problems in the indexer

// Only defined postgres to conserve resources
class LedgerApiTls12ConformanceTestPostgres extends LedgerApiTlsConformanceBase {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  val minTlsVersion = TlsVersion.V1_2

  "Ledger Api Test Tool" can {
    "pass tls 1.2 tests" in { implicit env =>
      ledgerApiTestToolPlugin.runSuites(
        suites = "TLSAtLeastOnePointTwoIT",
        exclude = Nil,
        concurrency = VariationsConformanceTestUtils.ConcurrentTestRuns,
        tlsCerts*
      )
    }
  }
}

class LedgerApiTls13ConformanceTestPostgres extends LedgerApiTlsConformanceBase {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  val minTlsVersion = TlsVersion.V1_3

  "Ledger Api Test Tool" can {
    "pass tls 1.3 tests" in { implicit env =>
      ledgerApiTestToolPlugin.runSuites(
        suites = "TLSOnePointThreeIT",
        exclude = Nil,
        concurrency = VariationsConformanceTestUtils.ConcurrentTestRuns,
        tlsCerts*
      )
    }
  }
}
