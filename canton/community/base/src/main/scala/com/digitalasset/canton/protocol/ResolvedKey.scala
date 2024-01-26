// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfVersioned
import com.digitalasset.canton.data.SerializableKeyResolution
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class ResolvedKey(key: LfGlobalKey, resolution: SerializableKeyResolution) {
  def toProtoV30: v30.ViewParticipantData.ResolvedKey =
    v30.ViewParticipantData.ResolvedKey(
      // oddity: pass the version from resolution to the proto-key
      key = Some(GlobalKeySerialization.assertToProto(LfVersioned(resolution.version, key))),
      resolution = resolution.toProtoOneOfV30,
    )
}

object ResolvedKey {
  def fromProtoV30(
      resolvedKeyP: v30.ViewParticipantData.ResolvedKey
  ): ParsingResult[ResolvedKey] = {
    val v30.ViewParticipantData.ResolvedKey(keyP, resolutionP) = resolvedKeyP
    for {
      keyWithVersion <- ProtoConverter
        .required("ResolvedKey.key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV30)
      LfVersioned(version, key) = keyWithVersion
      // oddity: pass the version from the proto-key to resolution
      resolution <- SerializableKeyResolution.fromProtoOneOfV30(resolutionP, version)
    } yield ResolvedKey(key, resolution)
  }
}
