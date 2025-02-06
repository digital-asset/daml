// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.NonEmptySeq
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParametersWithValidity,
  TestSynchronizerParameters,
}
import com.digitalasset.canton.synchronizer.mediator.Mediator.{Safe, SafeUntil}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

class MediatorTest extends AnyWordSpec with BaseTest {
  private def parametersWith(confirmationResponseTimeout: NonNegativeFiniteDuration) =
    TestSynchronizerParameters.defaultDynamic.tryUpdate(confirmationResponseTimeout =
      confirmationResponseTimeout
    )

  private val defaultTimeout = NonNegativeFiniteDuration.tryOfSeconds(10)
  private val defaultParameters = parametersWith(defaultTimeout)

  private val origin = CantonTimestamp.now()
  private def relTime(offset: Long): CantonTimestamp = origin.plusSeconds(offset)

  private lazy val synchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::default")
  )

  "Mediator.checkPruningStatus" should {
    "deal with current synchronizer parameters" in {
      val parameters =
        DynamicSynchronizerParametersWithValidity(
          defaultParameters,
          CantonTimestamp.Epoch,
          None,
          synchronizerId,
        )

      val cleanTimestamp = CantonTimestamp.now()
      val earliestPruningTimestamp = cleanTimestamp - defaultTimeout

      Mediator.checkPruningStatus(parameters, cleanTimestamp) shouldBe SafeUntil(
        earliestPruningTimestamp
      )
    }

    "cap the time using SynchronizerParameters.WithValidity[DynamicSynchronizerParameters].validFrom" in {
      val validFrom = origin

      def test(validUntil: Option[CantonTimestamp]): Assertion = {
        val parameters =
          DynamicSynchronizerParametersWithValidity(
            defaultParameters,
            validFrom,
            validUntil,
            synchronizerId,
          )

        // Capping happen
        Mediator.checkPruningStatus(parameters, validFrom.plusSeconds(1)) shouldBe SafeUntil(
          validFrom
        )

        Mediator.checkPruningStatus(
          parameters,
          validFrom + defaultTimeout + NonNegativeFiniteDuration.tryOfSeconds(1),
        ) shouldBe SafeUntil(validFrom.plusSeconds(1))
      }

      test(validUntil = None)
      test(validUntil = Some(validFrom.plus(defaultTimeout.unwrap.multipliedBy(2))))
    }

    "deal with future synchronizer parameters" in {
      val parameters =
        DynamicSynchronizerParametersWithValidity(
          defaultParameters,
          origin,
          None,
          synchronizerId,
        )

      Mediator.checkPruningStatus(
        parameters,
        origin - NonNegativeFiniteDuration.tryOfSeconds(10),
      ) shouldBe Safe
    }

    "deal with past synchronizer parameters" in {
      val dpChangeTs = relTime(60)

      val parameters =
        DynamicSynchronizerParametersWithValidity(
          defaultParameters,
          origin,
          Some(dpChangeTs),
          synchronizerId,
        )

      {
        val cleanTimestamp = dpChangeTs + NonNegativeFiniteDuration.tryOfSeconds(1)
        Mediator.checkPruningStatus(
          parameters,
          cleanTimestamp,
        ) shouldBe SafeUntil(cleanTimestamp - defaultTimeout)
      }

      {
        val cleanTimestamp = dpChangeTs + defaultTimeout
        Mediator.checkPruningStatus(
          parameters,
          cleanTimestamp,
        ) shouldBe Safe
      }
    }
  }

  "Mediator.latestSafePruningTsBefore" should {
    /*
      We consider the following setup:

                  O          20=dpChangeTs1    40=dpChangeTs2
      time        |-----------------|-----------------|---------------->
      timeout              10s            10 days            10s
     */

    val hugeTimeout = NonNegativeFiniteDuration.tryOfDays(10)

    val dpChangeTs1 = relTime(20)
    val dpChangeTs2 = relTime(40)

    val parameters = NonEmptySeq.of(
      DynamicSynchronizerParametersWithValidity(
        defaultParameters,
        origin,
        Some(dpChangeTs1),
        synchronizerId,
      ),
      // This one prevents pruning for some time
      DynamicSynchronizerParametersWithValidity(
        parametersWith(hugeTimeout),
        dpChangeTs1,
        Some(dpChangeTs2),
        synchronizerId,
      ),
      DynamicSynchronizerParametersWithValidity(
        defaultParameters,
        dpChangeTs2,
        None,
        synchronizerId,
      ),
    )

    "query in the first slice" in {
      // Tests in the first slice (timeout = defaultTimeout)
      Mediator.latestSafePruningTsBefore(
        parameters,
        origin + defaultTimeout - NonNegativeFiniteDuration.tryOfSeconds(1),
      ) shouldBe Some(origin) // capping happens

      Mediator.latestSafePruningTsBefore(
        parameters,
        dpChangeTs1,
      ) shouldBe Some(dpChangeTs1 - defaultTimeout)
    }

    "query in the second slice" in {
      {
        val cleanTs = dpChangeTs1 + NonNegativeFiniteDuration.tryOfSeconds(5)
        Mediator.latestSafePruningTsBefore(
          parameters,
          cleanTs,
        ) shouldBe Some(cleanTs - defaultTimeout) // effect of the first synchronizer parameters
      }

      {
        val cleanTs = dpChangeTs1 + defaultTimeout + NonNegativeFiniteDuration.tryOfSeconds(10)
        Mediator.latestSafePruningTsBefore(
          parameters,
          cleanTs,
        ) shouldBe Some(dpChangeTs1)
      }
    }

    "query in the third slice" in {
      // We cannot allow any request in second slice to be issued -> dpChangeTs1
      Mediator.latestSafePruningTsBefore(
        parameters,
        relTime(40),
      ) shouldBe Some(dpChangeTs1)

      // We cannot allow any request in second slice to be issued -> dpChangeTs1
      Mediator.latestSafePruningTsBefore(
        parameters,
        relTime(60),
      ) shouldBe Some(dpChangeTs1)

      // If enough time elapsed since huge timeout was revoked, we are fine again
      val endOfHugeTimeoutEffect = dpChangeTs2 + hugeTimeout
      Mediator.latestSafePruningTsBefore(
        parameters,
        endOfHugeTimeoutEffect,
      ) shouldBe Some(endOfHugeTimeoutEffect - defaultTimeout)
    }
  }
}
