// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import java.net.InetSocketAddress

class AkkaServiceHttpSpec extends AkkaServiceSpecBase(Some(new InetSocketAddress("127.0.0.1", 0)))
