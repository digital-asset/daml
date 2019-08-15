// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import scala.util.control.NoStackTrace

final case class TestingException(msg: String)
    extends RuntimeException("This exception was thrown for testing purposes. Message: " + msg)
    with NoStackTrace
