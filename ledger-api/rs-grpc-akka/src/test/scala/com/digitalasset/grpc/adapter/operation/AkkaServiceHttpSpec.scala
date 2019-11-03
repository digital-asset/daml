// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.operation

import java.net.InetSocketAddress

class AkkaServiceHttpSpec extends AkkaServiceSpecBase(Some(new InetSocketAddress("127.0.0.1", 0)))
