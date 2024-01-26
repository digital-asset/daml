// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.domain.admin.{v0, v2}
import com.digitalasset.canton.domain.sequencing.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerResponse,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.tracing.Traced
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

class GrpcSequencerInitializationServiceTest extends AsyncWordSpec with BaseTest {
  private val domainId = DefaultTestIdentities.domainId
  private val sequencerKey = SymbolicCrypto.signingPublicKey("seq-key")

  def createSut(
      initialize: Traced[InitializeSequencerRequest] => EitherT[
        FutureUnlessShutdown,
        String,
        InitializeSequencerResponse,
      ]
  ) =
    new GrpcSequencerInitializationService(initialize, loggerFactory)

  "GrpcSequencerInitializationService" should {
    "call given initialize function (v2) " in {
      val initRequest =
        v2.InitRequest(
          domainId = domainId.toProtoPrimitive,
          topologySnapshot = Some(StoredTopologyTransactions.empty.toProtoV30),
          domainParameters = Some(defaultStaticDomainParameters.toProtoV30),
          snapshot = ByteString.EMPTY,
        )

      val sut =
        createSut(_ =>
          EitherT.rightT[FutureUnlessShutdown, String](
            InitializeSequencerResponse("test", sequencerKey, false)
          )
        )
      for {
        response <- sut.initV2(initRequest)
      } yield {
        response shouldBe v0.InitResponse("test", Some(sequencerKey.toProtoV30), false)
      }
    }
  }
}
