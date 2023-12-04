// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.DomainParameters
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds, SimClock}
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=deprecated")
class SortedReconciliationIntervalsProviderTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with SortedReconciliationIntervalsHelpers {

  "SortedReconciliationIntervalsProvider" must {
    "allow to query reconciliation intervals (PV >= 4)" in {
      val protocolVersion = ProtocolVersion.v4

      val clock = new SimClock(fromEpoch(0), loggerFactory)

      val domainParameters = Vector(
        mkDynamicDomainParameters(0, 10, 1, protocolVersion),
        mkDynamicDomainParameters(10, 2, protocolVersion),
      )

      val reconciliationIntervals = domainParameters.map(_.map(_.reconciliationInterval))

      val topologySnapshot = mock[TopologySnapshot]
      when(topologySnapshot.listDynamicDomainParametersChanges())
        .thenAnswer(Future.successful(domainParameters.filter(_.validFrom <= clock.now)))

      val topologyClient = mock[DomainTopologyClient]

      when(topologyClient.approximateTimestamp).thenAnswer(clock.now)
      when(topologyClient.awaitSnapshot(any[CantonTimestamp])(any[TraceContext])).thenReturn {
        Future.successful(topologySnapshot)
      }

      val provider = SortedReconciliationIntervalsProvider(
        staticDomainParameters =
          BaseTest.defaultStaticDomainParametersWith(protocolVersion = protocolVersion),
        topologyClient = topologyClient,
        futureSupervisor = FutureSupervisor.Noop,
        loggerFactory = loggerFactory,
      )

      provider.getApproximateLatestReconciliationInterval shouldBe None

      def query(secondsFromEpoch: Long) =
        provider.reconciliationIntervals(fromEpoch(secondsFromEpoch)).futureValue

      clock.advanceTo(fromEpoch(1))
      query(1) shouldBe SortedReconciliationIntervals
        .create(reconciliationIntervals.take(1), clock.now)
        .value

      clock.advanceTo(fromEpoch(11))
      query(11) shouldBe SortedReconciliationIntervals
        .create(reconciliationIntervals, clock.now)
        .value
      provider.getApproximateLatestReconciliationInterval.value.intervalLength shouldBe PositiveSeconds
        .tryOfSeconds(2)
    }

    "allow to query reconciliation intervals (PV < 4)" in {
      val protocolVersion = ProtocolVersion.v3

      val clock = new SimClock(fromEpoch(0), loggerFactory)
      val defaultReconciliationInterval = defaultStaticDomainParameters.reconciliationInterval

      val updatedStaticDomainParameters =
        BaseTest.defaultStaticDomainParametersWith(
          reconciliationInterval =
            PositiveSeconds.tryOfSeconds(defaultReconciliationInterval.unwrap.getSeconds + 1),
          protocolVersion = protocolVersion,
        )

      // When PV is old enough, we have constant sorted reconciliation interval
      def expectedSortedReconciliationIntervals(validAt: CantonTimestamp) =
        SortedReconciliationIntervals
          .create(
            Seq(
              DomainParameters.WithValidity(
                CantonTimestamp.MinValue,
                None,
                updatedStaticDomainParameters.reconciliationInterval,
              )
            ),
            validUntil = validAt,
          )
          .value

      val provider = SortedReconciliationIntervalsProvider(
        staticDomainParameters = updatedStaticDomainParameters,
        topologyClient = mock[DomainTopologyClient],
        futureSupervisor = FutureSupervisor.Noop,
        loggerFactory = loggerFactory,
      )

      provider.getApproximateLatestReconciliationInterval shouldBe None

      def query(secondsFromEpoch: Long) =
        provider.reconciliationIntervals(fromEpoch(secondsFromEpoch)).futureValue

      val ts1 = 1L
      clock.advanceTo(fromEpoch(ts1))
      query(ts1) shouldBe expectedSortedReconciliationIntervals(CantonTimestamp.ofEpochSecond(ts1))

      val ts2 = 10000L
      clock.advanceTo(fromEpoch(ts2))
      query(ts2) shouldBe expectedSortedReconciliationIntervals(CantonTimestamp.ofEpochSecond(ts2))
      provider.getApproximateLatestReconciliationInterval.value.intervalLength shouldBe updatedStaticDomainParameters.reconciliationInterval
    }

    "return an error if topology is not known" in {
      val protocolVersion = ProtocolVersion.v4

      val topologyKnownAt = fromEpoch(10)

      val topologySnapshot = mock[TopologySnapshot]
      when(topologySnapshot.listDynamicDomainParametersChanges())
        .thenAnswer(Future.successful(Nil))

      val topologyClient = mock[DomainTopologyClient]
      when(topologyClient.approximateTimestamp).thenReturn(topologyKnownAt)
      when(topologyClient.awaitSnapshot(any[CantonTimestamp])(any[TraceContext])).thenReturn {
        Future.successful(topologySnapshot)
      }

      val provider = SortedReconciliationIntervalsProvider(
        staticDomainParameters =
          BaseTest.defaultStaticDomainParametersWith(protocolVersion = protocolVersion),
        topologyClient = topologyClient,
        futureSupervisor = FutureSupervisor.Noop,
        loggerFactory = loggerFactory,
      )

      val invalidQueryTime = topologyKnownAt + NonNegativeFiniteDuration.tryOfMillis(1)
      val validQueryTime = topologyKnownAt

      provider
        .reconciliationIntervals(validQueryTime)
        .futureValue shouldBe SortedReconciliationIntervals.create(Nil, validQueryTime).value

      val error =
        s"Unable to query domain parameters at $invalidQueryTime ; latest possible is $topologyKnownAt"

      loggerFactory.assertThrowsAndLogsAsync[RuntimeException](
        provider.reconciliationIntervals(invalidQueryTime),
        _ shouldBe (new RuntimeException(error)),
        _.warningMessage shouldBe error,
      )
    }

    "compute the correct reconciliation intervals covering a period" in {

      val protocolVersion = ProtocolVersion.v4

      val clock = new SimClock(fromEpoch(0), loggerFactory)

      val domainParameters = Vector(
        mkDynamicDomainParameters(0, 13, 2, testedProtocolVersion),
        mkDynamicDomainParameters(13, 9, testedProtocolVersion),
      )

      val topologySnapshot = mock[TopologySnapshot]
      when(topologySnapshot.listDynamicDomainParametersChanges())
        .thenAnswer(Future.successful(domainParameters.filter(_.validFrom <= clock.now)))

      val topologyClient = mock[DomainTopologyClient]

      when(topologyClient.approximateTimestamp).thenAnswer(clock.now)
      when(topologyClient.awaitSnapshot(any[CantonTimestamp])(any[TraceContext])).thenReturn {
        Future.successful(topologySnapshot)
      }

      val provider = SortedReconciliationIntervalsProvider(
        staticDomainParameters =
          BaseTest.defaultStaticDomainParametersWith(protocolVersion = protocolVersion),
        topologyClient = topologyClient,
        futureSupervisor = FutureSupervisor.Noop,
        loggerFactory = loggerFactory,
      )

      clock.advanceTo(fromEpoch(18))

      for {
        x <- provider.computeReconciliationIntervalsCovering(fromEpoch(10), fromEpoch(18))
      } yield {
        x shouldBe
          List(
            CommitmentPeriod.create(fromEpochSecond(10), fromEpochSecond(12)),
            CommitmentPeriod.create(fromEpochSecond(12), fromEpochSecond(18)),
          )
      }
    }

  }
}
