// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.archives

import com.daml.lf.archive.ArchiveParser

object ArchiveConversions {

  @throws[com.daml.lf.archive.Error]
  def extractHash(rawArchive: RawArchive): String =
    ArchiveParser.assertFromByteString(rawArchive.byteString).getHash

  def toHashesAndRawArchives(
      archives: List[com.daml.daml_lf_dev.DamlLf.Archive]
  ): Map[String, RawArchive] =
    archives.view
      .map(archive => archive.getHash -> RawArchive(archive.toByteString))
      .toMap
}
