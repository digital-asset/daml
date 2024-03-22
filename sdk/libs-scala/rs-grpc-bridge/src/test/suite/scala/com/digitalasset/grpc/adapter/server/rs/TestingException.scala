// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.rs

import scala.util.control.NoStackTrace

case class TestingException(msg: String) extends RuntimeException(msg) with NoStackTrace
