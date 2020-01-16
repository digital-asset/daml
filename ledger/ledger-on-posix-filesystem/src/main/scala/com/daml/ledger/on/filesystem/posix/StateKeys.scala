// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.Path

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey

object StateKeys {
  val NameChunkSize: Int = 32

  def resolveStateKey(root: Path, key: DamlStateKey): Path = {
    val name = Bytes.toString(key.toByteArray)
    Iterator
      .iterate(name.splitAt(NameChunkSize)) {
        case (_, rest) => rest.splitAt(NameChunkSize)
      }
      .map(_._1)
      .takeWhile(_.nonEmpty)
      .foldLeft(root)(_.resolve(_))
  }
}
