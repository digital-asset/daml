// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import java.net.InetSocketAddress

class PekkoClientWithReferenceServiceHttpSpec
    extends PekkoClientWithReferenceServiceSpecBase(Some(new InetSocketAddress("127.0.0.1", 0)))
