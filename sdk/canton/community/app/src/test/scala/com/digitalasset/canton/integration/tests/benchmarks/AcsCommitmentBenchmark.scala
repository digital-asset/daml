// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.benchmarks

import cats.syntax.functorFilter.*
import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.event.Event.Event.{Archived, Created, Exercised}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.value.Value
import com.daml.ledger.javaapi as javab
import com.daml.ledger.javaapi.data.Transaction.fromProto
import com.daml.metrics.api.testing.MetricValues
import com.daml.metrics.api.{MetricHandle, MetricInfo, MetricName, MetricQualification}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BigDecimalImplicits.DoubleToBigDecimal
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.MetricValue.Histogram
import com.digitalasset.canton.metrics.{MetricValue, MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{DefaultTestIdentities, Party, PartyId}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Random

trait AcsCommitmentBenchmark
    extends CommunityIntegrationTest
    with SharedEnvironment
    with MetricValues {
  private lazy val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)

  private val disableCommitmentsFor: Set[String] = Set(
    DefaultTestIdentities.participant1.uid.identifier.str
  )

  private val computeMetricName = "daml.participant.sync.commitments.compute.duration.seconds"

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(
        _.focus(_.monitoring.metrics)
          .replace(
            MetricsConfig(
              qualifiers = Seq[MetricQualification](
                MetricQualification.Errors,
                MetricQualification.Latency,
                MetricQualification.Saturation,
                MetricQualification.Traffic,
                MetricQualification.Debug,
              ),
              reporters = Seq(MetricsReporterConfig.Prometheus(port = UniquePortGenerator.next)),
            )
          )
      )
      .withSetup { implicit env =>
        import env.*

        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(reconciliationInterval = reconciliationInterval.toConfig),
            )
        )

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer1, alias = daName)

      }
      .addConfigTransforms(ConfigTransforms.disableAdditionalConsistencyChecks)
      .updateTestingConfig(
        _.copy(doNotUseCommitmentCachingFor = disableCommitmentsFor, supportAdhocMetrics = true)
      )

  private val module = "ManySimpleContracts"

  // creates partiesPerParticipant parties for each participant in parts and returns the created party ids per participant
  private def createParties(
      parts: Seq[LocalParticipantReference],
      partiesPerParticipant: Int,
  ): Map[LocalParticipantReference, Seq[Party]] =
    parts
      .map(p =>
        (
          p, {
            val parties =
              (0 until partiesPerParticipant)
                .map { _ =>
                  p.parties.enable(
                    Random.alphanumeric.take(10).mkString, // random party name

                    synchronizeParticipants = parts.diff(Seq(p)),
                  )
                }
            parties
          },
        )
      )
      .toMap

  private def createContracts(
      partiesAndParticipants: Map[LocalParticipantReference, Seq[Party]],
      nrContracts: Int,
      timers: Map[LocalParticipantReference, MetricHandle.Timer],
  ): Map[LocalParticipantReference, Seq[(Party, Seq[Party])]] = {

    // sorting only for reproducibility of the test setup
    val orderedParticipants = partiesAndParticipants.keySet.toSeq.sortBy(_.name)

    // each participant creates a stakeholder group made of one party on itself and combinations of a party on each of the following participants
    orderedParticipants.tails.toList.mapFilter { ps =>
      NonEmpty.from(ps).map { psNE =>
        val participant = psNE.head1
        val remainingParticipants = psNE.tail1

        val stakeholders = partiesAndParticipants
          .get(participant)
          .map(parties =>
            parties.flatMap { sig =>
              remainingParticipants
                .map(p => partiesAndParticipants.getOrElse(p, Seq[Party]()))
                .transpose
                .map(s => sig -> s)
            }
          )

        logger.debug(
          s"Participant $participant #stakeholder groups ${stakeholders.fold(0)(_.size)} stakeholders $stakeholders "
        )

        val creationTimer = timers(participant)
        creationTimer.time {
          stakeholders.foreach(_.groupBy { case (sig, _observers) => sig }.foreach {
            case (sig, observerGroups) =>
              createIOUs(
                nrContracts,
                participant,
                observerGroups.map { case (_s, obs) => obs },
                sig,
              )
          })
        }
        (
          participant,
          stakeholders.getOrElse {
            logger.error(s"Could not build stakeholder set for participant $participant")
            Seq.empty[(PartyId, Seq[PartyId])]
          },
        )
      }

    }.toMap
  }

  /** Create the given number of IOUs with the same signatory, but different observers */
  private def createIOUs(
      nrContracts: Int,
      participant: LocalParticipantReference,
      obs: Seq[Seq[Party]],
      sig: Party,
  ): Unit = {
    val cmd = Range(1, nrContracts)
      .map(_ =>
        obs.flatMap(observers =>
          new Iou(
            sig.toProtoPrimitive,
            sig.toProtoPrimitive,
            new Amount(1.0.toBigDecimal, "CHF"),
            observers.map(_.toProtoPrimitive).toList.asJava,
          ).create.commands.asScala
        )
      )
      .fold(Seq.empty[javab.data.Command])(_ ++ _)

    JavaDecodeUtil
      .decodeAllCreated(Iou.COMPANION)(
        participant.ledger_api.javaapi.commands
          .submit(Seq(sig), cmd)
      )
  }

  def calcAndPrint(
      histogram: MetricValue.Histogram,
      action: String,
  ): (Double, Double, Double, Long) = {
    val max = histogram.maxBoundary
    val p95 = histogram.percentileBoundary(0.95)
    val avg = histogram.average
    val count = histogram.count
    println(
      s"Time taken for $action: max=${dfmt(max)}, avg=${dfmt(avg)}, p95=${dfmt(p95)}, counts=$count"
    )
    (max, p95, avg, count)
  }

  private def dfmt(value: Double, divider: Double = 1e9): String = {
    val d = if (value > 1e5) 4 else 9
    s"%1.${d}f" format (value / divider)
  }

  "Benchmarking ACS commitments should be not too terrible" in { implicit env =>
    import env.*

    participants.all.foreach(_.dars.upload(CantonTestsPath))

    val pkg = participant1.packages.find_by_module(module).headOption.map(_.packageId).value

    val alice = participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
    )
    val bob =
      participant2.parties.enable(
        "Bob",
        synchronizeParticipants = Seq(participant1),
      )

    val metricName = "commitments.time-to-create-contracts"

    val creationTimerGenerator =
      env.environment.metricsRegistry
        .forParticipant(participant1.name)
        .openTelemetryMetricsFactory
        .timer(MetricInfo(MetricName(metricName), "", MetricQualification.Debug))

    /** Create the given number of contracts in a single view, by exercising a choice on a helper
      * contract
      */
    def singleViewCreate(nrContracts: Int) = {

      val template = "Creator"
      val createCmd =
        ledger_api_utils.create(pkg, module, template, Map("obs" -> bob, "sig" -> alice))

      val Value.Sum.ContractId(creatorCid) =
        extractSubmissionResult(
          participant1.ledger_api.commands
            .submit(Seq(alice), Seq(createCmd), transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS)
        ): @unchecked

      val exerciseCmd = ledger_api_utils.exercise(
        pkg,
        module,
        template,
        "Spawn",
        Map("count" -> nrContracts),
        creatorCid,
      )

      creationTimerGenerator.time {
        participant1.ledger_api.commands.submit(Seq(alice), Seq(exerciseCmd))
      }

    }

    // Alternatives: create in batches, or create in a single view
    // createInBatches(100, 100)
    singleViewCreate(1000)

    val after = environment.clock.now

    // wait up to 10 seconds for participant1 to compute the commitments and receive commitments from participant2
    eventually(10.seconds) {
      participant1.health.ping(participant2)
      participant1.testing
        .find_clean_commitments_timestamp(daName)
        .fold(fail("Not outstanding is None")) { ts =>
          ts should be > after
        }
    }

    val (cMax, _, _, _) = calcAndPrint(
      participant1.metrics.get_histogram(computeMetricName),
      "computing ACS",
    )

    val (crtMax, _, _, _) = calcAndPrint(
      participant1.metrics.get_histogram(
        "commitments.time-to-create-contracts.duration.seconds"
      ),
      "create contracts",
    )

    cMax should be <= 2.seconds.toNanos.toDouble
    crtMax should be <= 30.seconds.toNanos.toDouble
  }

  // Commitment caching is disabled for participantWithoutCommitmentCaching and enabled for participantWithCommitmentCaching
  // in the env setup in the beginning of the test.
  // The test creates nrParties on each participantWithoutCommitmentCaching and participantWithCommitmentCaching
  // Then it creates stakeholder groups with all combinations of a party on participantWithoutCommitmentCaching and a
  // party participantWithCommitmentCaching.
  // Therefore in total we have nrParties * nrParties stakeholder groups.
  // We create nrContractsPerStakeholderGroup contracts in each stakeholder group, and we trigger a round of commitments
  // Creation takes longer than the reconciliationInterval (set here at two minutes), so we will have commitments
  // Then we create nrContractsForUpdatedStakeholderGroup more contracts in one stakeholder group, which also takes
  // longer than the reconciliation interval.
  // participantWithoutCommitmentCaching does not cache the commitment for participantWithCommitmentCaching, so it will
  // have to recompute the commitment by adding all stakeholder commitments, whereas participantWithCommitmentCaching
  // caches the commitment for participantWithoutCommitmentCaching.
  // We then trigger another round of commitments and check that the time to compute the updated commitments is smaller
  // on participantWithCommitmentCaching than on participantWithoutCommitmentCaching.
  "ACS commitment caching should decrease computation time with many stakeholder groups where few see contract updates" in {
    implicit env =>
      import env.*

      logger.debug(s"Starting caching test; testing config ${env.environment.testingConfig}")
      val participantWithoutCommitmentCaching = participant1
      val participantWithCommitmentCaching = participant2

      participants.all.foreach(_.dars.upload(CantonTestsPath))
      val nrParties = 25
      val nrContractsPerStakeholderGroup = 2
      val nrContractsForUpdatedStakeholderGroup = 100

      logger.debug(s"Creating $nrParties parties on each participant")

      val partiesForParticipants =
        createParties(
          Seq(participantWithoutCommitmentCaching, participantWithCommitmentCaching),
          nrParties,
        )

      logger.debug(
        s"Finished creating parties ${participantWithoutCommitmentCaching.topology} ${participantWithCommitmentCaching.topology}"
      )

      val metricNameWithout = "commitments.time-to-create-contracts-caching-p1"

      val creationTimerGeneratorwo =
        env.environment.metricsRegistry
          .forParticipant(participantWithoutCommitmentCaching.name)
          .openTelemetryMetricsFactory
          .timer(MetricInfo(MetricName(metricNameWithout), "", MetricQualification.Debug))

      val metricNameWith = "commitments.time-to-create-contracts-caching-p2"
      val creationTimerGeneratorw =
        env.environment.metricsRegistry
          .forParticipant(participantWithCommitmentCaching.name)
          .openTelemetryMetricsFactory
          .timer(MetricInfo(MetricName(metricNameWith), "", MetricQualification.Debug))

      val stakeholderGroups =
        createContracts(
          partiesForParticipants,
          nrContractsPerStakeholderGroup,
          Map(
            participantWithoutCommitmentCaching -> creationTimerGeneratorwo,
            participantWithCommitmentCaching -> creationTimerGeneratorw,
          ),
        )

      logger.debug(
        s"Stakeholder groups $stakeholderGroups " +
          s"of which w/o caching $participantWithoutCommitmentCaching " +
          s"${stakeholderGroups.values.flatten.count { case (sig, obs) =>
              partiesForParticipants(participantWithoutCommitmentCaching).contains(sig) ||
              partiesForParticipants(participantWithoutCommitmentCaching).exists(p => obs.contains(p))
            }} " +
          s"of which w caching $participantWithCommitmentCaching " +
          s"${stakeholderGroups.values.flatten.count { case (sig, obs) =>
              partiesForParticipants(participantWithCommitmentCaching).contains(sig) ||
              partiesForParticipants(participantWithCommitmentCaching).exists(p => obs.contains(p))
            }} "
      )

      val time1 = environment.clock.now

      eventually(10.seconds) {
        participantWithoutCommitmentCaching.health.ping(participantWithCommitmentCaching)
        Seq(participantWithCommitmentCaching, participantWithoutCommitmentCaching).foreach(p =>
          p.testing
            .find_clean_commitments_timestamp(daName)
            .fold(fail("Not outstanding is None")) { ts =>
              ts should be > time1
            }
        )
      }

      val prepTimeWithoutCachingHistogram =
        participantWithoutCommitmentCaching.metrics.get_histogram(computeMetricName)

      val prepTimeWithCachingHistogram = participantWithCommitmentCaching.metrics.get_histogram(
        computeMetricName
      )

      // create 1 more contracts in one stakeholder group
      logger.debug(
        s"Participant without caching $participantWithoutCommitmentCaching has ${stakeholderGroups
            .get(participantWithoutCommitmentCaching)
            .fold(0)(v => v.size)} stakeholder groups." +
          s"Creating $nrContractsForUpdatedStakeholderGroup more contracts in one stakeholder group" +
          s"${stakeholderGroups.get(participantWithoutCommitmentCaching).map(_.headOption)}"
      )

      stakeholderGroups.get(participantWithoutCommitmentCaching).foreach {
        _.headOption.foreach { case (sig, observers) =>
          createIOUs(
            nrContractsForUpdatedStakeholderGroup,
            participantWithoutCommitmentCaching,
            Seq(observers),
            sig,
          )
        }
      }

      val time2 = environment.clock.now
      eventually(10.seconds) {
        participantWithoutCommitmentCaching.health.ping(participantWithCommitmentCaching)
        Seq(participantWithCommitmentCaching, participantWithoutCommitmentCaching).foreach(p =>
          p.testing
            .find_clean_commitments_timestamp(daName)
            .fold(fail("Not outstanding is None")) { ts =>
              ts should be > time2
            }
        )
      }

      val timeWithoutCachingHistogram =
        participantWithoutCommitmentCaching.metrics.get_histogram(
          computeMetricName
        )

      val timeWithCachingHistogram = participantWithCommitmentCaching.metrics.get_histogram(
        computeMetricName
      )

      def subtractHistograms(h1: Histogram, h2: Histogram) = {
        if (!h1.boundaries.equals(h2.boundaries)) {
          logger.error("Cannot subtract histograms with different boundaries")
        }
        val counts = h1.counts.zipAll(h2.counts, 0L, 0L).map { case (c1, c2) => c1 - c2 }
        val boundaries = h1.boundaries
        val attributes = h2.attributes.filter { case (k, v) =>
          !h1.attributes.get(k).contains(v)
        }
        Histogram(h1.sum - h2.sum, h1.count - h2.count, counts, boundaries, attributes)
      }

      val (_maxWithout, _p95Without, avgWithout, _sizeWO) =
        calcAndPrint(timeWithoutCachingHistogram, "commitment no caching")
      val (_maxWith, _p95With, avgWith, _sizeW) =
        calcAndPrint(timeWithCachingHistogram, "commitment with caching")
      val (_maxUpdateWithout, _p95UpdateWithout, avgUpdateWithout, _sizeUpdateWO) =
        calcAndPrint(
          subtractHistograms(timeWithoutCachingHistogram, prepTimeWithoutCachingHistogram),
          "commitment update no caching",
        )
      val (_maxUpdateWith, _p95UpdateWith, avgUpdateWith, _sizeUpdateW) =
        calcAndPrint(
          subtractHistograms(timeWithCachingHistogram, prepTimeWithCachingHistogram),
          "commitment update with caching",
        )
      // Actual run for reference: improvement ~2x in avg: Time without caching: max boundary 0.075 avg 0.01488  ||| time with caching: max boundary 0.05 avg 0.0075
      logger.debug(
        s"Time without caching: avg $avgWithout  ||| " +
          s"time with caching: avg $avgWith"
      )
      // Actual run for reference: improvement ~2x in avg: Update time without caching: max boundary 0.075 30 0.04365  ||| update time with caching: max boundary 0.05 avg 0.023
      logger.debug(
        s"Update time without caching: avg $avgUpdateWithout  ||| " +
          s"update time with caching: avg $avgUpdateWith"
      )

      // We're comparing only the average values here, because we have exact data only for those. For all other parameters
      // we do not have actual values but only have histogram boundaries, which are sensitive to the number of
      // measurements and the boundary values, and are not representative for comparison across histograms
      def roundTwoDec(nr: Double): Double =
        BigDecimal(nr).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      roundTwoDec(avgWithout) should be >= roundTwoDec(avgWith)
      roundTwoDec(avgUpdateWithout) should be >= roundTwoDec(avgUpdateWith)

      val (creationTimeMaxWithout, _, _, _) = calcAndPrint(
        participant1.metrics.get_histogram(metricNameWithout + ".duration.seconds"),
        // .get_double_point(
        "create contracts",
      )

      val (creationTimeMaxWith, _, _, _) = calcAndPrint(
        participant2.metrics.get_histogram(metricNameWith + ".duration.seconds"),
        "create contracts",
      )

      val creationTimeMaxTotal = (creationTimeMaxWithout + creationTimeMaxWith) / 2
      logger.debug(s"Contract creation time avg $creationTimeMaxTotal")
      creationTimeMaxTotal should be > reconciliationInterval.duration.toNanos.toDouble
  }

  def extractSubmissionResult(tx: Transaction): Value.Sum = {
    require(
      fromProto(Transaction.toJavaProto(tx)).getRootNodeIds.size == 1,
      s"Received transaction with not exactly one root node: $tx",
    )
    tx.events.head.event match {
      case Created(created) => Value.Sum.ContractId(created.contractId)
      case Exercised(exercised) =>
        val Value(result) = exercised.exerciseResult.value
        result
      case Archived(_) =>
        throw new IllegalArgumentException(
          s"Received transaction with unexpected archived event: $tx"
        )
      case Event.Empty =>
        throw new IllegalArgumentException(s"Received transaction with empty event: $tx")
    }
  }
}

class AcsCommitmentBenchmarkPostgres extends AcsCommitmentBenchmark {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

//class AcsCommitmentBenchmarkH2 extends AcsCommitmentBenchmark {
//  registerPlugin(new UseH2(loggerFactory))
//}
