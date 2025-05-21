// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.sequencer.grpc

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.sequencer.SequencerConnectClient.SynchronizerClientBootstrapInfo
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader.{
  LoadSequencerEndpointInformationResult,
  SequencerInfoLoaderError,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnectionValidation,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.topology.{
  PhysicalSynchronizerId,
  SequencerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.{ProtocolVersionCompatibility, ReleaseVersion}
import com.digitalasset.canton.{
  BaseTest,
  BaseTestWordSpec,
  HasExecutionContext,
  SequencerAlias,
  SynchronizerAlias,
}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalatest.Assertion

import scala.concurrent.Promise

class SequencerInfoLoaderTest extends BaseTestWordSpec with HasExecutionContext {

  private lazy val sequencer1 = SequencerId(
    UniqueIdentifier.tryFromProtoPrimitive("sequencer1::namespace")
  )
  private lazy val sequencer2 = SequencerId(
    UniqueIdentifier.tryFromProtoPrimitive("sequencer2::namespace")
  )
  private lazy val sequencerAlias1 = SequencerAlias.tryCreate("sequencer1")
  private lazy val sequencerAlias2 = SequencerAlias.tryCreate("sequencer2")
  private lazy val sequencerAlias3 = SequencerAlias.tryCreate("sequencer3")
  private lazy val synchronizerId1 = SynchronizerId.tryFromString("first::namespace").toPhysical
  private lazy val synchronizerId2 = SynchronizerId.tryFromString("second::namespace").toPhysical
  private lazy val endpoint1 = Endpoint("localhost", Port.tryCreate(1001))
  private lazy val endpoint2 = Endpoint("localhost", Port.tryCreate(1002))
  private lazy val endpoint3 = Endpoint("localhost", Port.tryCreate(1003))
  private lazy val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()
  private lazy val synchronizerAlias = SynchronizerAlias.tryCreate("synchronizer1")

  private def mapArgs(
      args: List[
        (
            SequencerAlias,
            Endpoint,
            Either[SequencerInfoLoaderError, SynchronizerClientBootstrapInfo],
        )
      ]
  ): List[LoadSequencerEndpointInformationResult] =
    args
      .map { case (alias, endpoint, result) =>
        (
          GrpcSequencerConnection(
            NonEmpty.mk(Seq, endpoint),
            transportSecurity = false,
            None,
            alias,
            None,
          ),
          result,
        )
      }
      .map {
        case (conn, Right(result)) =>
          LoadSequencerEndpointInformationResult.Valid(
            conn,
            result,
            staticSynchronizerParameters,
          )
        case (conn, Left(result)) =>
          LoadSequencerEndpointInformationResult.NotValid(conn, result)
      }

  private def run(
      expectSynchronizerId: Option[PhysicalSynchronizerId],
      args: List[
        (
            SequencerAlias,
            Endpoint,
            Either[SequencerInfoLoaderError, SynchronizerClientBootstrapInfo],
        )
      ],
      validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
      threshold: PositiveInt = PositiveInt.one,
  ): Either[Seq[LoadSequencerEndpointInformationResult.NotValid], Unit] = SequencerInfoLoader
    .validateNewSequencerConnectionResults(
      expectSynchronizerId,
      validation,
      threshold,
      logger,
    )(mapArgs(args))

  private def hasError(
      expectSynchronizerId: Option[PhysicalSynchronizerId],
      args: List[
        (
            SequencerAlias,
            Endpoint,
            Either[SequencerInfoLoaderError, SynchronizerClientBootstrapInfo],
        )
      ],
      validation: SequencerConnectionValidation = SequencerConnectionValidation.All,
      threshold: PositiveInt = PositiveInt.one,
  )(check: String => Assertion): Assertion = {
    val result = run(expectSynchronizerId, args, validation, threshold)
    result.left.value should have length (1)
    result.left.value.foreach(x => check(x.error.cause))
    succeed
  }

  "endpoint result validation" should {
    "left is returned as left" in {
      hasError(
        None,
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
          (
            sequencerAlias2,
            endpoint2,
            Left(SequencerInfoLoaderError.InvalidState("booh")),
          ),
        ),
      )(_ should include("booh"))
    }
    "detect mismatches in synchronizer id" in {
      hasError(
        None,
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
          (
            sequencerAlias2,
            endpoint2,
            Right(SynchronizerClientBootstrapInfo(synchronizerId2, sequencer2)),
          ),
        ),
      )(_ should include("Synchronizer id mismatch"))
    }
    "detect if synchronizer id does not match expected one" in {
      hasError(
        Some(synchronizerId2),
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          )
        ),
      )(_ should include("does not match expected"))
    }
    "detect mismatches in sequencer-id between an alias" in {
      hasError(
        None,
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
          (
            sequencerAlias1,
            endpoint2,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer2)),
          ),
        ),
      )(_ should include("sequencer-id mismatch"))
    }
    "detect the same sequencer-id among different sequencer aliases" in {
      hasError(
        None,
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
          (
            sequencerAlias2,
            endpoint2,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
        ),
      )(_ should include("same sequencer-id reported by different alias"))
    }
    "accept if everything is fine" in {
      run(
        None,
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
          (
            sequencerAlias2,
            endpoint2,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer2)),
          ),
        ),
      ).value shouldBe (())
    }
    "tolerate errors if threshold can be reached" in {
      forAll(
        Seq(SequencerConnectionValidation.Active, SequencerConnectionValidation.ThresholdActive)
      ) { validation =>
        run(
          None,
          List(
            (
              sequencerAlias1,
              endpoint1,
              Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
            ),
            (sequencerAlias2, endpoint2, Left(SequencerInfoLoaderError.InvalidState("booh"))),
            (
              sequencerAlias3,
              endpoint3,
              Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer2)),
            ),
          ),
          validation,
          threshold = PositiveInt.tryCreate(2),
        ).value shouldBe (())
      }
    }
    "tolerate errors for Active if threshold can not be reached" in {
      run(
        None,
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
          (sequencerAlias2, endpoint2, Left(SequencerInfoLoaderError.InvalidState("booh2"))),
          (sequencerAlias3, endpoint3, Left(SequencerInfoLoaderError.InvalidState("booh3"))),
        ),
        SequencerConnectionValidation.Active,
        threshold = PositiveInt.tryCreate(2),
      ).value shouldBe (())
    }
    "complain about errors for StrictActive if threshold can not be reached" in {
      val result = run(
        None,
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
          (sequencerAlias2, endpoint2, Left(SequencerInfoLoaderError.InvalidState("booh2"))),
          (sequencerAlias3, endpoint3, Left(SequencerInfoLoaderError.InvalidState("booh3"))),
        ),
        SequencerConnectionValidation.ThresholdActive,
        threshold = PositiveInt.tryCreate(2),
      )
      result.left.value should have length 2
      forAll(result.left.value)(_.error.cause should (include("booh2") or include("booh3")))
    }
  }

  "aggregation" should {

    def aggregate(
        args: List[
          (
              SequencerAlias,
              Endpoint,
              Either[SequencerInfoLoaderError, SynchronizerClientBootstrapInfo],
          )
        ]
    ): Either[SequencerInfoLoaderError, SequencerInfoLoader.SequencerAggregatedInfo] =
      SequencerInfoLoader.aggregateBootstrapInfo(
        logger,
        sequencerTrustThreshold = PositiveInt.tryCreate(2),
        SubmissionRequestAmplification.NoAmplification,
        SequencerConnectionValidation.All,
        None,
      )(mapArgs(args))

    "accept if everything is fine" in {
      aggregate(
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
          (
            sequencerAlias2,
            endpoint2,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer2)),
          ),
        )
      ) match {
        case Right(_) => succeed
        case Left(value) => fail(value.toString)
      }
    }

    "reject if we don't have enough sequencers" in {
      aggregate(
        List(
          (
            sequencerAlias1,
            endpoint1,
            Right(SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1)),
          ),
          (sequencerAlias2, endpoint2, Left(SequencerInfoLoaderError.InvalidState("booh"))),
        )
      ) match {
        case Right(_) => fail("should not succeed")
        case Left(_) => succeed
      }
    }

  }

  "sequencer info loading" should {
    val actorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(actorSystem)
    val sequencerInfoLoader = new SequencerInfoLoader(
      ProcessingTimeout(),
      TracingConfig.Propagation.Disabled,
      clientProtocolVersions = ProtocolVersionCompatibility.supportedProtocols(
        includeAlphaVersions = true,
        includeBetaVersions = true,
        release = ReleaseVersion.current,
      ),
      minimumProtocolVersion = Some(testedProtocolVersion),
      dontWarnOnDeprecatedPV = false,
      loggerFactory = loggerFactory,
    )
    // Futures in loadSequencerInfoAsync can race such that more than the expected tolerance can be returned.
    val toleranceForRaciness = 3

    "return complete results when requested" in {
      val scs = sequencerConnections(PositiveInt.tryCreate(10))
      val res = sequencerInfoLoader
        .loadSequencerEndpointsParallel(
          synchronizerAlias,
          scs,
          parallelism = NonNegativeInt.tryCreate(3),
          maybeThreshold = None,
        )(loadSequencerInfoFactory(Map(2 -> nonValidResultF(scs(1)))))
        .futureValueUS
      res.size shouldBe scs.size
      val (valid, invalid) = splitValidAndInvalid(res)
      valid shouldBe (1 to 10).filterNot(_ == 2)
      invalid shouldBe Seq(2)
    }

    "return partial results when threshold specified" in {
      val threshold = PositiveInt.tryCreate(3)
      val scs = sequencerConnections(PositiveInt.tryCreate(10))
      val invalidSequencerConnections = Map(
        2 -> nonValidResultF(scs(1)),
        3 -> nonValidResultF(scs(2)),
      )
      val res = sequencerInfoLoader
        .loadSequencerEndpointsParallel(
          synchronizerAlias,
          scs,
          parallelism = NonNegativeInt.tryCreate(3),
          maybeThreshold = Some(threshold),
        )(loadSequencerInfoFactory(invalidSequencerConnections))
        .futureValueUS
      val (valid, _) = splitValidAndInvalid(res)
      valid.intersect(Seq(2, 3)) shouldBe Seq.empty
      // note that we can't say anything about invalid due to raciness, e.g.
      // if 1, 4, and 5 are loaded first as actually happened on CI.
      // So we can't even assert: invalid shouldBe Seq(2, 3)
      assertBetween(
        "partial result size",
        valid.size,
        threshold.unwrap,
        toleranceForRaciness,
      )
    }

    "not get stuck on hung sequencers" in {
      val threshold = PositiveInt.tryCreate(5)
      val delayedPromises =
        Seq(1, 3, 4)
          .map(_ -> Promise[UnlessShutdown[LoadSequencerEndpointInformationResult]]())
          .toMap
      val scs = sequencerConnections(PositiveInt.tryCreate(10))
      val res = sequencerInfoLoader
        .loadSequencerEndpointsParallel(
          synchronizerAlias,
          scs,
          parallelism = NonNegativeInt.tryCreate(4),
          maybeThreshold = Some(threshold),
        )(loadSequencerInfoFactory(delayedPromises.map { case (k, v) =>
          k -> FutureUnlessShutdown(v.future)
        }))
        .futureValueUS
      val (valid, invalid) = splitValidAndInvalid(res)
      valid.intersect(Seq(1, 3, 4)) shouldBe Seq.empty
      invalid shouldBe Seq.empty
      res.size shouldBe threshold.unwrap
      assertBetween("hung sequencers result size", res.size, threshold.unwrap, toleranceForRaciness)
      logger.info("Before exiting test, complete futures")
      delayedPromises.foreach { case (i, p) => p.success(Outcome(validResult(scs(i - 1)))) }
    }

    "return early in case of a failed Future" in {
      val scs = sequencerConnections(PositiveInt.tryCreate(10))
      val invalidSequencerConnections = Map(
        5 -> FutureUnlessShutdown.failed(new RuntimeException("booh"))
      )
      loggerFactory
        .assertThrowsAndLogsAsync[RuntimeException](
          sequencerInfoLoader
            .loadSequencerEndpointsParallel(
              synchronizerAlias,
              scs,
              parallelism = NonNegativeInt.tryCreate(3),
              maybeThreshold = None,
            )(loadSequencerInfoFactory(invalidSequencerConnections))
            .failOnShutdown,
          assertion = _.getMessage should include("booh"),
          _.errorMessage should include(
            "Exception loading sequencer Sequencer 'sequencer5' info in synchronizer Synchronizer 'synchronizer1'"
          ),
        )
        .futureValue
    }
  }

  private def sequencerConnections(n: PositiveInt): NonEmpty[Seq[SequencerConnection]] =
    NonEmpty
      .from(
        (1 to n.value)
          .map(i =>
            GrpcSequencerConnection(
              NonEmpty.mk(Seq, endpoint1),
              transportSecurity = false,
              None,
              SequencerAlias.tryCreate(s"sequencer$i"),
              None,
            )
          )
      )
      .value

  private def loadSequencerInfoFactory(
      m: Map[Int, FutureUnlessShutdown[LoadSequencerEndpointInformationResult]]
  ): SequencerConnection => FutureUnlessShutdown[LoadSequencerEndpointInformationResult] = sc =>
    m.getOrElse(
      sc.sequencerAlias.unwrap.drop("sequencer".length).toInt,
      FutureUnlessShutdown.pure(validResult(sc)),
    )

  private def validResult(sc: SequencerConnection): LoadSequencerEndpointInformationResult =
    LoadSequencerEndpointInformationResult.Valid(
      sc,
      SynchronizerClientBootstrapInfo(synchronizerId1, sequencer1),
      staticSynchronizerParameters,
    )

  private def nonValidResultF(
      sc: SequencerConnection
  ): FutureUnlessShutdown[LoadSequencerEndpointInformationResult] =
    FutureUnlessShutdown.pure(
      LoadSequencerEndpointInformationResult
        .NotValid(sc, SequencerInfoLoaderError.InvalidState("booh"))
    )

  private def assertBetween(check: String, value: Int, expected: Int, tolerance: Int): Assertion =
    clue(check) {
      value should (be >= expected and be <= (expected + tolerance))
    }

  private def splitValidAndInvalid(
      results: Seq[LoadSequencerEndpointInformationResult]
  ): (Seq[Int], Seq[Int]) = {
    val valid = results.collect { case LoadSequencerEndpointInformationResult.Valid(c, _, _) =>
      c.sequencerAlias.unwrap.drop("sequencer".length).toInt
    }.sorted
    val notValid = results.collect { case LoadSequencerEndpointInformationResult.NotValid(c, _) =>
      c.sequencerAlias.unwrap.drop("sequencer".length).toInt
    }.sorted
    (valid, notValid)
  }
}
