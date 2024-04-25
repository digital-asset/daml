// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain.grpc

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.domain.SequencerConnectClient.DomainClientBootstrapInfo
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader.{
  LoadSequencerEndpointInformationResult,
  SequencerInfoLoaderError,
}
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnectionValidation,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.topology.{DomainId, SequencerId, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, BaseTestWordSpec, SequencerAlias}
import org.scalatest.Assertion

class SequencerInfoLoaderTest extends BaseTestWordSpec {

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

}
