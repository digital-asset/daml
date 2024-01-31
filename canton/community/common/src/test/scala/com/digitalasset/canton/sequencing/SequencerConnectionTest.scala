// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.{BaseTest, SequencerAlias}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class SequencerConnectionTest extends AnyWordSpec with BaseTest {
  import SequencerConnectionTest.*

  "SequencerConnection.merge" should {
    "merge grpc connection endpoints" in {
      SequencerConnection.merge(Seq(grpc1, grpc2)) shouldBe Right(grpcMerged)
    }
    "fail with empty connections" in {
      SequencerConnection.merge(Seq.empty) shouldBe Left(
        "There must be at least one sequencer connection defined"
      )
    }
    "grpc connection alone remains unchanged" in {
      SequencerConnection.merge(Seq(grpc1)) shouldBe Right(grpc1)
    }
  }
}

object SequencerConnectionTest {
  def endpoint(n: Int) = Endpoint(s"host$n", Port.tryCreate(100 * n))

  val grpc1 = GrpcSequencerConnection(
    NonEmpty(Seq, endpoint(1), endpoint(2)),
    false,
    Some(ByteString.copyFromUtf8("certificates")),
    SequencerAlias.Default,
  )
  val grpc2 = GrpcSequencerConnection(
    NonEmpty(Seq, endpoint(3), endpoint(4)),
    false,
    None,
    SequencerAlias.Default,
  )
  val grpcMerged = GrpcSequencerConnection(
    NonEmpty(Seq, endpoint(1), endpoint(2), endpoint(3), endpoint(4)),
    false,
    Some(ByteString.copyFromUtf8("certificates")),
    SequencerAlias.Default,
  )
}
