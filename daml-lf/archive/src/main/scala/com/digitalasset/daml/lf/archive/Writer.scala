// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.security.MessageDigest

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml_lf.{DamlLf => PLF}

abstract class Writer[Pkg] {

  protected[this] def encodePayloadOfVersion(Pkg: Pkg, version: LanguageVersion): PLF.ArchivePayload

  final def encodeArchive(pkg: Pkg, version: LanguageVersion): PLF.Archive = {

    val payload = encodePayloadOfVersion(pkg, version).toByteString
    val hash = PackageId.assertFromString(
      MessageDigest.getInstance("SHA-256").digest(payload.toByteArray).map("%02x" format _).mkString
    )

    PLF.Archive
      .newBuilder()
      .setHashFunction(PLF.HashFunction.SHA256)
      .setPayload(payload)
      .setHash(hash)
      .build()

  }

}
