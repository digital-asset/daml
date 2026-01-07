// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParametersWithValidity,
  TestSynchronizerParameters,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
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

  "Mediator.latestSafePruningTsForSynchronizerParameters" should {
    "deal with current synchronizer parameters" in {
      val parameters =
        DynamicSynchronizerParametersWithValidity(
          defaultParameters,
          CantonTimestamp.Epoch,
          None,
        )

      val cleanTimestamp = CantonTimestamp.now()
      val earliestPruningTimestamp = cleanTimestamp - defaultTimeout

      Mediator.latestSafePruningTsForSynchronizerParameters(
        parameters,
        cleanTimestamp,
      ) shouldBe earliestPruningTimestamp
    }

    "cap the time using SynchronizerParameters.WithValidity[DynamicSynchronizerParameters].validFrom" in {
      val validFrom = origin

      def test(validUntil: Option[CantonTimestamp]): Assertion = {
        val parameters =
          DynamicSynchronizerParametersWithValidity(
            defaultParameters,
            validFrom,
            validUntil,
          )

        // Capping happen
        Mediator.latestSafePruningTsForSynchronizerParameters(
          parameters,
          validFrom.plusSeconds(1),
        ) shouldBe validFrom

        Mediator.latestSafePruningTsForSynchronizerParameters(
          parameters,
          validFrom + defaultTimeout + NonNegativeFiniteDuration.tryOfSeconds(1),
        ) shouldBe validFrom.plusSeconds(1)
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
        )

      val cleanTimestamp = origin - NonNegativeFiniteDuration.tryOfSeconds(10)
      Mediator.latestSafePruningTsForSynchronizerParameters(
        parameters,
        cleanTimestamp,
      ) shouldBe cleanTimestamp
    }

    "deal with past synchronizer parameters" in {
      val dpChangeTs = relTime(60)

      val parameters =
        DynamicSynchronizerParametersWithValidity(
          defaultParameters,
          origin,
          Some(dpChangeTs),
        )

      {
        val cleanTimestamp = dpChangeTs + NonNegativeFiniteDuration.tryOfSeconds(1)
        Mediator.latestSafePruningTsForSynchronizerParameters(
          parameters,
          cleanTimestamp,
        ) shouldBe (cleanTimestamp - defaultTimeout)
      }

      {
        val cleanTimestamp = dpChangeTs + defaultTimeout
        Mediator.latestSafePruningTsForSynchronizerParameters(
          parameters,
          cleanTimestamp,
        ) shouldBe cleanTimestamp
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

    val parameters = NonEmpty.mk(
      Seq,
      DynamicSynchronizerParametersWithValidity(
        defaultParameters,
        origin,
        Some(dpChangeTs1),
      ),
      // This one prevents pruning for some time
      DynamicSynchronizerParametersWithValidity(
        parametersWith(hugeTimeout),
        dpChangeTs1,
        Some(dpChangeTs2),
      ),
      DynamicSynchronizerParametersWithValidity(
        defaultParameters,
        dpChangeTs2,
        None,
      ),
    )

    "query in the first slice" in {
      // Tests in the first slice (timeout = defaultTimeout)
      Mediator.latestSafePruningTsBefore(
        parameters,
        origin + defaultTimeout - NonNegativeFiniteDuration.tryOfSeconds(1),
      ) shouldBe origin // capping happens

      Mediator.latestSafePruningTsBefore(
        parameters,
        dpChangeTs1,
      ) shouldBe (dpChangeTs1 - defaultTimeout)
    }

    "query in the second slice" in {
      {
        val cleanTs = dpChangeTs1 + NonNegativeFiniteDuration.tryOfSeconds(5)
        Mediator.latestSafePruningTsBefore(
          parameters,
          cleanTs,
        ) shouldBe (cleanTs - defaultTimeout) // effect of the first synchronizer parameters
      }

      {
        val cleanTs = dpChangeTs1 + defaultTimeout + NonNegativeFiniteDuration.tryOfSeconds(10)
        Mediator.latestSafePruningTsBefore(
          parameters,
          cleanTs,
        ) shouldBe dpChangeTs1
      }
    }

    "query in the third slice" in {
      // We cannot allow any request in second slice to be issued -> dpChangeTs1
      Mediator.latestSafePruningTsBefore(
        parameters,
        relTime(40),
      ) shouldBe dpChangeTs1

      // We cannot allow any request in second slice to be issued -> dpChangeTs1
      Mediator.latestSafePruningTsBefore(
        parameters,
        relTime(60),
      ) shouldBe dpChangeTs1

      // If enough time elapsed since huge timeout was revoked, we are fine again
      val endOfHugeTimeoutEffect = dpChangeTs2 + hugeTimeout
      Mediator.latestSafePruningTsBefore(
        parameters,
        endOfHugeTimeoutEffect,
      ) shouldBe (endOfHugeTimeoutEffect - defaultTimeout)
    }

    "query non-overlapping future synchronizer parameters" in {
      val cleanTimestamp = CantonTimestamp.ofEpochSecond(10)
      Mediator.latestSafePruningTsBefore(
        NonEmpty.mk(
          Seq,
          DynamicSynchronizerParametersWithValidity(
            defaultParameters,
            CantonTimestamp.ofEpochSecond(20),
            None,
          ),
        ),
        cleanTimestamp,
      ) shouldBe cleanTimestamp
    }

    "query non-overlapping synchronizer parameters expired before max response timeout" in {
      val cleanTimestamp = CantonTimestamp.ofEpochSecond(40)
      Mediator.latestSafePruningTsBefore(
        NonEmpty.mk(
          Seq,
          DynamicSynchronizerParametersWithValidity(
            parametersWith(NonNegativeFiniteDuration.tryOfSeconds(10)),
            CantonTimestamp.ofEpochSecond(10),
            Some(CantonTimestamp.ofEpochSecond(20)),
          ),
        ),
        cleanTimestamp,
      ) shouldBe cleanTimestamp
    }
  }
}
