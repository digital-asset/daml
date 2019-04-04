// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.client.rs

import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike

class ClientPublisherTest
    extends PublisherVerification[Long](new TestEnvironment(500L, 100L, false))
    with PublisherCreation
    with TestNGSuiteLike
