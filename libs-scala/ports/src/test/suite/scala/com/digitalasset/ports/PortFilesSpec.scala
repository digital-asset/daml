// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.nio.file.{Path, Paths}
import java.util.UUID

import com.daml.ports.PortFiles.FileAlreadyExists
import org.scalatest.{FreeSpec, Inside, Matchers}
import scalaz.{-\/, \/-}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PortFilesSpec extends FreeSpec with Matchers with Inside {

  "Can create a port file with a unique file name" in {
    val path = uniquePath()
    inside(PortFiles.write(path, Port(1024))) {
      case \/-(()) =>
    }
    path.toFile.exists() shouldBe true
  }

  "Cannot create a port file with a nonunique file name" in {
    val path = uniquePath()
    inside(PortFiles.write(path, Port(1024))) {
      case \/-(()) =>
    }
    inside(PortFiles.write(path, Port(1024))) {
      case -\/(FileAlreadyExists(p)) =>
        p shouldBe path
    }
  }

  private def uniquePath(): Path = {
    val fileName = s"${this.getClass.getSimpleName}-${UUID.randomUUID().toString}.dummy"
    Paths.get(fileName)
  }
}
