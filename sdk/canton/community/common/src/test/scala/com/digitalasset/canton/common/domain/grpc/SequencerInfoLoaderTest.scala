// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain.grpc

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.domain.SequencerConnectClient.DomainClientBootstrapInfo
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader.{
  LoadSequencerEndpointInformationResult,
  SequencerInfoLoaderError,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnectionValidation,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.topology.{DomainId, SequencerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.{ProtocolVersionCompatibility, ReleaseVersion}
import com.digitalasset.canton.{
  BaseTest,
  BaseTestWordSpec,
  DomainAlias,
  HasExecutionContext,
  SequencerAlias,
}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalatest.Assertion

import scala.concurrent.{Future, Promise}

class SequencerInfoLoaderTest extends BaseTestWordSpec with HasExecutionContext {

  private lazy val sequencer1 = SequencerId(
    UniqueIdentifier.tryFromProtoPrimitive("sequencer1::namespace")
  )
  private lazy val sequencer2 = SequencerId(
    UniqueIdentifier.tryFromProtoPrimitive("sequencer2::namespace")
  )
  private lazy val sequencerAlias1 = SequencerAlias.tryCreate("sequencer1")
  private lazy val sequencerAlias2 = SequencerAlias.tryCreate("sequencer2")
  private lazy val domainId1 = DomainId.tryFromString("first::namespace")
  private lazy val domainId2 = DomainId.tryFromString("second::namespace")
  private lazy val endpoint1 = Endpoint("localhost", Port.tryCreate(1001))
  private lazy val endpoint2 = Endpoint("localhost", Port.tryCreate(1002))
  private lazy val staticDomainParameters = BaseTest.defaultStaticDomainParametersWith()
  private lazy val domainAlias = DomainAlias.tryCreate("domain1")

  private def mapArgs(
      args: List[
        (SequencerAlias, Endpoint, Either[SequencerInfoLoaderError, DomainClientBootstrapInfo])
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
          ),
          result,
        )
      }
      .map {
        case (conn, Right(result)) =>
          LoadSequencerEndpointInformationResult.Valid(
            conn,
            result,
            staticDomainParameters,
          )
        case (conn, Left(result)) =>
          LoadSequencerEndpointInformationResult.NotValid(conn, result)
      }

  private def run(
      expectDomainId: Option[DomainId],
      args: List[
        (SequencerAlias, Endpoint, Either[SequencerInfoLoaderError, DomainClientBootstrapInfo])
      ],
      activeOnly: Boolean = false,
  ) = SequencerInfoLoader
    .validateNewSequencerConnectionResults(
      expectDomainId,
      if (activeOnly) SequencerConnectionValidation.Active else SequencerConnectionValidation.All,
      logger,
    )(mapArgs(args))

  private def hasError(
      expectDomainId: Option[DomainId],
      args: List[
        (SequencerAlias, Endpoint, Either[SequencerInfoLoaderError, DomainClientBootstrapInfo])
      ],
      activeOnly: Boolean = false,
  )(check: String => Assertion): Assertion = {
    val result = run(expectDomainId, args, activeOnly)
    result.left.value should have length (1)
    result.left.value.foreach(x => check(x.error.cause))
    succeed
  }

  "endpoint result validation" should {
    "left is returned as left" in {
      hasError(
        None,
        List(
          (sequencerAlias1, endpoint1, Right(DomainClientBootstrapInfo(domainId1, sequencer1))),
          (
            sequencerAlias2,
            endpoint2,
            Left(SequencerInfoLoaderError.InvalidState("booh")),
          ),
        ),
      )(_ should include("booh"))
    }
    "detect mismatches in domain-id" in {
      hasError(
        None,
        List(
          (sequencerAlias1, endpoint1, Right(DomainClientBootstrapInfo(domainId1, sequencer1))),
          (sequencerAlias2, endpoint2, Right(DomainClientBootstrapInfo(domainId2, sequencer2))),
        ),
      )(_ should include("Domain-id mismatch"))
    }
    "detect if domain-id does not match expected one" in {
      hasError(
        Some(domainId2),
        List(
          (sequencerAlias1, endpoint1, Right(DomainClientBootstrapInfo(domainId1, sequencer1)))
        ),
      )(_ should include("does not match expected"))
    }
    "detect mismatches in sequencer-id between an alias" in {
      hasError(
        None,
        List(
          (sequencerAlias1, endpoint1, Right(DomainClientBootstrapInfo(domainId1, sequencer1))),
          (sequencerAlias1, endpoint2, Right(DomainClientBootstrapInfo(domainId1, sequencer2))),
        ),
      )(_ should include("sequencer-id mismatch"))
    }
    "detect the same sequencer-id among different sequencer aliases" in {
      hasError(
        None,
        List(
          (sequencerAlias1, endpoint1, Right(DomainClientBootstrapInfo(domainId1, sequencer1))),
          (sequencerAlias2, endpoint2, Right(DomainClientBootstrapInfo(domainId1, sequencer1))),
        ),
      )(_ should include("same sequencer-id reported by different alias"))
    }
    "accept if everything is fine" in {
      run(
        None,
        List(
          (sequencerAlias1, endpoint1, Right(DomainClientBootstrapInfo(domainId1, sequencer1))),
          (sequencerAlias2, endpoint2, Right(DomainClientBootstrapInfo(domainId1, sequencer2))),
        ),
        activeOnly = false,
      ).value shouldBe (())
    }
  }

  "aggregation" should {

    def aggregate(
        args: List[
          (SequencerAlias, Endpoint, Either[SequencerInfoLoaderError, DomainClientBootstrapInfo])
        ]
    ): Either[SequencerInfoLoaderError, SequencerInfoLoader.SequencerAggregatedInfo] = {
      SequencerInfoLoader.aggregateBootstrapInfo(
        logger,
        sequencerTrustThreshold = PositiveInt.tryCreate(2),
        SubmissionRequestAmplification.NoAmplification,
        SequencerConnectionValidation.All,
        None,
      )(mapArgs(args))
    }

    "accept if everything is fine" in {
      aggregate(
        List(
          (sequencerAlias1, endpoint1, Right(DomainClientBootstrapInfo(domainId1, sequencer1))),
          (sequencerAlias2, endpoint2, Right(DomainClientBootstrapInfo(domainId1, sequencer2))),
        )
      ) match {
        case Right(_) => succeed
        case Left(value) => fail(value.toString)
      }
    }

    "reject if we don't have enough sequencers" in {
      aggregate(
        List(
          (sequencerAlias1, endpoint1, Right(DomainClientBootstrapInfo(domainId1, sequencer1))),
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
      clientProtocolVersions = ProtocolVersionCompatibility.supportedProtocolsParticipant(
        includeUnstableVersions = true,
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
      val scs = sequencerConnections(10)
      val res = sequencerInfoLoader
        .loadSequencerEndpointsParallel(
          domainAlias,
          scs,
          parallelism = NonNegativeInt.tryCreate(3),
          maybeThreshold = None,
        )(
          loadSequencerInfoFactory(Map(2 -> nonValidResultF(scs(1))))
        )
        .futureValue
      res.size shouldBe scs.size
      val (valid, invalid) = splitValidAndInvalid(res)
      valid shouldBe (1 to 10).filterNot(_ == 2)
      invalid shouldBe Seq(2)
    }

    "return partial results when threshold specified" in {
      val threshold = PositiveInt.tryCreate(3)
      val scs = sequencerConnections(10)
      val invalidSequencerConnections = Map(
        2 -> nonValidResultF(scs(1)),
        3 -> nonValidResultF(scs(2)),
      )
      val res = sequencerInfoLoader
        .loadSequencerEndpointsParallel(
          domainAlias,
          scs,
          parallelism = NonNegativeInt.tryCreate(3),
          maybeThreshold = Some(threshold),
        )(
          loadSequencerInfoFactory(invalidSequencerConnections)
        )
        .futureValue
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
        Seq(1, 3, 4).map(_ -> Promise[LoadSequencerEndpointInformationResult]()).toMap
      val scs = sequencerConnections(10)
      val res = sequencerInfoLoader
        .loadSequencerEndpointsParallel(
          domainAlias,
          scs,
          parallelism = NonNegativeInt.tryCreate(4),
          maybeThreshold = Some(threshold),
        )(loadSequencerInfoFactory(delayedPromises.map { case (k, v) => k -> v.future }))
        .futureValue
      val (valid, invalid) = splitValidAndInvalid(res)
      valid.intersect(Seq(1, 3, 4)) shouldBe Seq.empty
      invalid shouldBe Seq.empty
      res.size shouldBe threshold.unwrap
      assertBetween("hung sequencers result size", res.size, threshold.unwrap, toleranceForRaciness)
      logger.info("Before exiting test, complete futures")
      delayedPromises.foreach { case (i, p) => p.success(validResult(scs(i - 1))) }
    }

    "return early in case of a failed Future" in {
      val scs = sequencerConnections(10)
      val invalidSequencerConnections = Map(
        5 -> Future.failed(new RuntimeException("booh"))
      )
      loggerFactory
        .assertThrowsAndLogsAsync[RuntimeException](
          sequencerInfoLoader
            .loadSequencerEndpointsParallel(
              domainAlias,
              scs,
              parallelism = NonNegativeInt.tryCreate(3),
              maybeThreshold = None,
            )(
              loadSequencerInfoFactory(invalidSequencerConnections)
            ),
          assertion = _.getMessage should include("booh"),
          _.errorMessage should include(
            "Exception loading sequencer Sequencer 'sequencer5' info in domain Domain 'domain1'"
          ),
        )
        .futureValue
    }
  }

  private def sequencerConnections(n: Int): NonEmpty[Seq[SequencerConnection]] =
    NonEmpty
      .from(
        (1 to n)
          .map(i =>
            GrpcSequencerConnection(
              NonEmpty.mk(Seq, endpoint1),
              transportSecurity = false,
              None,
              SequencerAlias.tryCreate(s"sequencer${i}"),
            )
          )
      )
      .getOrElse(throw new IllegalArgumentException("n must be positive"))

  private def loadSequencerInfoFactory(
      m: Map[Int, Future[LoadSequencerEndpointInformationResult]] = Map.empty
  ): SequencerConnection => Future[LoadSequencerEndpointInformationResult] = sc =>
    m.getOrElse(
      sc.sequencerAlias.unwrap.drop("sequencer".length).toInt,
      Future.successful(validResult(sc)),
    )

  private def validResult(sc: SequencerConnection): LoadSequencerEndpointInformationResult =
    LoadSequencerEndpointInformationResult.Valid(
      sc,
      DomainClientBootstrapInfo(domainId1, sequencer1),
      staticDomainParameters,
    )

  private def nonValidResultF(
      sc: SequencerConnection
  ): Future[LoadSequencerEndpointInformationResult] =
    Future.successful(
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
