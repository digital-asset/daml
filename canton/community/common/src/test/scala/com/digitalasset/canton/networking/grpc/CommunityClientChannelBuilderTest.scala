// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.networking.Endpoint

import java.util.concurrent.Executor

class CommunityClientChannelBuilderTest extends BaseTestWordSpec {
  "constructing with more than one endpoint" should {
    "complain that we don't support many endpoints" in {
      val clientChannelBuilder = CommunityClientChannelBuilder(loggerFactory)

      loggerFactory.assertLogs(
        clientChannelBuilder.create(
          NonEmpty(
            Seq,
            Endpoint("localhost", Port.tryCreate(3000)),
            Endpoint("localhost", Port.tryCreate(3001)),
          ),
          false,
          mock[Executor],
        ),
        _.warningMessage should (
          include("does not support using many connections")
            and include("Defaulting to first: localhost:3000")
        ),
      )
    }
  }
}
