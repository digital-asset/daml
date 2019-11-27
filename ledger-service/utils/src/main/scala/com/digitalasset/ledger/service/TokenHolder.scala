// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.service

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors

import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object TokenHolder {

  private val logger = LoggerFactory.getLogger(classOf[TokenHolder])

  private def slurpToken(path: Path): Option[String] =
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

  import TokenHolder.slurpToken

  private[this] val ref = new AtomicReference(slurpToken(path))

  def token: Option[String] = ref.get()

  def refresh(): Unit = ref.set(slurpToken(path))

}
