// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import io.grpc.BindableService

/** Defines a interface that identifies a api service which will be registered with the
  * ledger api grpc server.
  */
trait GrpcApiService extends BindableService with AutoCloseable
