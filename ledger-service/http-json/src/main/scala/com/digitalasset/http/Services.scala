// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.{v1 => lav1}

import scala.concurrent.Future

object Services {
  type SubmitAndWaitForTransaction =
    lav1.command_service.SubmitAndWaitRequest => Future[
      lav1.command_service.SubmitAndWaitForTransactionResponse]
}
