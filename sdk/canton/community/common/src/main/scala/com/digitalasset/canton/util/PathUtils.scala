// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import java.nio.file.Path

object PathUtils {

  /** Get the file name without extension for the provided file path
    * @param path the file path
    * @return the filename without extension
    */
  def getFilenameWithoutExtension(path: Path): String = {
    val fileName = path.getFileName.toString
    if (fileName.indexOf(".") >= 0) fileName.substring(0, fileName.lastIndexOf(".")) else fileName
  }

}
