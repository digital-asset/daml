// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.testing

import com.digitalasset.ports.FreePort
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec

trait RandomPorts extends LazyLogging { self: AkkaTest =>

  def randomPort(lowerBound: Option[Int] = None, upperBound: Option[Int] = None): Int = {

    @tailrec
    def tryNext(candidatePort: Int = 0): Int = {
      if (candidatePort != 0) {
        logger.info(s"Using port $candidatePort")
        candidatePort
      } else {
        val port = FreePort.find()
        logger.info(s"Checking port $port")
        val isHighEnough = lowerBound.fold(true)(port > _)
        val isLowEnough = upperBound.fold(true)(port < _)
        if (isHighEnough && isLowEnough) {
          tryNext(port)
        } else {
          tryNext()
        }
      }
    }

    tryNext()
  }
}
