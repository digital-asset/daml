// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors

import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object TokenHolder {

  private val logger = LoggerFactory.getLogger(classOf[TokenHolder])

  private def slurp(path: Path): Option[String] =
    try {
      logger.info(s"Reading token from $path...")
      Option(Files.readAllLines(path).stream.collect(Collectors.joining("\n")))
    } catch {
      case NonFatal(e) =>
        logger.error(s"Unable to read token from $path", e)
        None
    }

}

final class TokenHolder(path: Path) {

  private val ref = new AtomicReference(TokenHolder.slurp(path))

  def token: Option[String] = ref.get()

  def refresh(): Unit = ref.set(TokenHolder.slurp(path))

}
