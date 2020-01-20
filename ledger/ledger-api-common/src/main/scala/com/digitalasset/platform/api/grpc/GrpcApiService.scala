// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.api.grpc

import io.grpc.BindableService

/**
  * Defines a interface that identifies a api service which will be registered with the
  * ledger api grpc server.
  */
trait GrpcApiService extends BindableService with AutoCloseable
