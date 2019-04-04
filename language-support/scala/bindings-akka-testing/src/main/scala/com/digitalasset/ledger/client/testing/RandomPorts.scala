// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.testing

import java.net.ServerSocket

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
        var candidate: ServerSocket = null
        try {
          candidate = new ServerSocket(0)
        } finally {
          candidate.close()
        }
        logger.info(s"Checking port ${candidate.getLocalPort}")
        val isHighEnough = lowerBound.fold(true)(candidate.getLocalPort > _)
        val isLowEnough = upperBound.fold(true)(candidate.getLocalPort < _)
        if (isHighEnough && isLowEnough) {
          tryNext(candidate.getLocalPort)
        } else {
          tryNext()
        }
      }
    }

    tryNext()
  }
}
