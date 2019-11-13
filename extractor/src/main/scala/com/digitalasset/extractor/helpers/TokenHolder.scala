// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.helpers

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors

import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}
import scala.util.control.NonFatal

object TokenHolder {

  private val logger = LoggerFactory.getLogger(classOf[TokenHolder])

  private def slurpToken(path: Path): Try[String] =
    Try {
      logger.info(s"Reading token from $path...")
      Files.readAllLines(path).stream.collect(Collectors.joining("\n"))
    } recoverWith {
      case NonFatal(e) =>
        logger.error(s"Unable to read token from $path", e)
        Failure(e)
    }

}

final class TokenHolder(location: Option[Path]) {

  import TokenHolder.slurpToken

  private[this] val ref = location.map(path => new AtomicReference(slurpToken(path)))

  def token: Option[String] = ref.flatMap(_.get().toOption)

  def refresh(): Unit = location.foreach(path => ref.foreach(_.set(slurpToken(path))))

}
