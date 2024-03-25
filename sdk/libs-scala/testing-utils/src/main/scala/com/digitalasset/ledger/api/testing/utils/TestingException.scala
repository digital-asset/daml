// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import scala.util.control.NoStackTrace

final case class TestingException(msg: String)
    extends RuntimeException("This exception was thrown for testing purposes. Message: " + msg)
    with NoStackTrace
