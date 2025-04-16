// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.metrics

import com.daml.metrics.MetricsFilterConfig
import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import monocle.macros.syntax.lens.*
import org.scalatest.BeforeAndAfterAll

import java.io.File

sealed trait MetricRegistryIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BeforeAndAfterAll {

  // Use the class name to ensure concurrent runs of different instances use different directories
  private val targetDir = new File("tmp-metrics-" + getClass.getSimpleName)

  override def beforeAll(): Unit =
    super.beforeAll()

  override def afterAll(): Unit = {
    super.afterAll()
    Option(targetDir.list()).foreach { files =>
      // targetDir.list() may return null if targetDir is not a directory or
      // some other I/O error occurred, making filter throw a NPE
      files.filter(_.endsWith(".csv")).foreach { f =>
        new File(targetDir, f).delete()
      }
      targetDir.delete()
    }
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(
        _.focus(_.monitoring.metrics)
          .replace(
            MetricsConfig(
              qualifiers = MetricQualification.All,
              reporters = Seq[MetricsReporterConfig](
                MetricsReporterConfig
                  .Csv(
                    directory = targetDir,
                    interval = config.NonNegativeFiniteDuration.ofSeconds(1),
                    filters = Seq(
                      "inflight-validations",
                      "confirmation-request-creation",
                      "indexer.updates",
                      "sequencer-client.submissions",
                      "indexer.events",
                    )
                      .map(str => MetricsFilterConfig(contains = str)),
                  ),
                MetricsReporterConfig.Logging(
                  interval = config.NonNegativeFiniteDuration.ofSeconds(1),
                  filters = Nil,
                ),
              ),
            )
          )
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
      }

  "Verifying that metrics from ledger api server exist" in { implicit env =>
    import env.*

    participant1.health.ping(participant1)

    val updatesBefore =
      participant1.metrics.get_long_point("daml.participant.api.indexer.updates").value

    val ledgerEndBefore =
      participant1.metrics
        .get_long_point("daml.participant.api.index.ledger_end_sequential_id")
        .value

    participant1.health.ping(participant1)

    val updatesAfter =
      participant1.metrics.get_long_point("daml.participant.api.indexer.updates").value
    updatesAfter should be > updatesBefore

    val ledgerEndAfter =
      participant1.metrics
        .get_long_point("daml.participant.api.index.ledger_end_sequential_id")
        .value
    ledgerEndAfter should be > ledgerEndBefore
  }

  "verify that metrics go up when we bong" in { implicit env =>
    import env.*

    participant1.testing.maybe_bong(Set(participant1.id), levels = 3) shouldBe defined

    participant1.metrics.list("daml.sequencer-client.submissions") should not be empty

    participant1.metrics
      .get_histogram(
        "daml.sequencer-client.submissions.sends.duration.seconds"
      )
      .count should be > 0L

    // to ensure the expected metrics really exist and the periodic background writer had time to actually write the metrics
    // PLEASE NOTE: the expected metrics are used in performance tests, maintaining consistency here is key to catch
    // mismatch in CI.
    eventually() {
      List(
        "participant1.synchronizer1.daml.participant.sync.inflight-validations.csv",
        "participant1.synchronizer1.daml.participant.sync.protocol-messages.confirmation-request-creation.duration.seconds.csv",
        "participant1.daml.participant.api.indexer.updates.csv",
        "participant1.synchronizer1.daml.sequencer-client.submissions.sends.duration.seconds.csv",
        "participant1.daml.participant.api.indexer.events.csv",
      ).foreach(
        new File(targetDir, _) should exist
      )
    }
    nodes.local.stop()
  }

  "verify that metrics don't complain on restart" when {

    "starting sequencer" in { implicit env =>
      import env.*
      loggerFactory.assertLogs(
        sequencer1.start()
      )
    }
    "starting mediator" in { implicit env =>
      import env.*
      loggerFactory.assertLogs(
        mediator1.start()
      )
    }

    "starting participant" in { implicit env =>
      import env.*
      loggerFactory.assertLogs(
        participant1.start()
      )
    }
    "reconnecting participant" in { implicit env =>
      import env.*
      loggerFactory.assertLogs(
        participant1.synchronizers.connect_local(sequencer1, daName)
      )
    }
    "pinging participant" in { implicit env =>
      import env.*
      loggerFactory.assertLogs(
        participant1.health.ping(participant1)
      )
    }
  }

}

class MetricRegistryIntegrationTestDefault extends MetricRegistryIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
