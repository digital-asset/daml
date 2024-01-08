// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import io.grpc.StatusRuntimeException

import scala.util.control.NoStackTrace

/** The sole purpose of this class is to give StatusRuntimeException with NoStacktrace a nice name in logs. */
class ApiException(exception: StatusRuntimeException)
    extends StatusRuntimeException(exception.getStatus, exception.getTrailers)
    with NoStackTrace
