// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client.rs

import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatestplus.testng.TestNGSuiteLike

class ClientPublisherTest
    extends PublisherVerification[Long](new TestEnvironment(500L, 100L, false))
    with PublisherCreation
    with TestNGSuiteLike
