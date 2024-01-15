// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.interceptors

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ServerCall

final class ForwardingServerCall[ReqT, RespT](call: ServerCall[ReqT, RespT])
    extends SimpleForwardingServerCall[ReqT, RespT](call) {}
