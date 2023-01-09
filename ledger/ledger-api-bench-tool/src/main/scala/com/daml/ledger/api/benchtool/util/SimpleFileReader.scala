// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.util

import com.google.protobuf.ByteString

import java.io.{BufferedReader, File, FileReader, Reader}
import scala.util.{Try, Using}

object SimpleFileReader {

  def readResource[Result](name: String): Try[ByteString] =
    Using(getClass.getClassLoader.getResourceAsStream(name))(ByteString.readFrom)

  def readFile[Result](file: File)(f: Reader => Result): Try[Result] =
    Using(new BufferedReader(new FileReader(file)))(f)

}
