// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.testing

import scala.util.control.NoStackTrace

case object TimeoutException extends RuntimeException with NoStackTrace {
  override val getMessage: String = s"Future could not be completed before timeout"
}
