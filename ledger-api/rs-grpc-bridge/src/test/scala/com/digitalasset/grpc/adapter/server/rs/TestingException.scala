// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.server.rs

import scala.util.control.NoStackTrace

case class TestingException(msg: String) extends RuntimeException(msg) with NoStackTrace
