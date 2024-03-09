// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfVersioned
import com.digitalasset.canton.data.SerializableKeyResolution
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class ResolvedKey(key: LfGlobalKey, resolution: SerializableKeyResolution) {
  def toProtoV0: v0.ViewParticipantData.ResolvedKey =
    v0.ViewParticipantData.ResolvedKey(
      // oddity: pass the version from resolution to the proto-key
      key = Some(GlobalKeySerialization.assertToProtoV0(LfVersioned(resolution.version, key))),
      resolution = resolution.toProtoOneOfV0,
    )
  def toProtoV1: v1.ResolvedKey =
    v1.ResolvedKey(
      // oddity: pass the version from resolution to the proto-key
      key = Some(GlobalKeySerialization.assertToProtoV1(LfVersioned(resolution.version, key))),
      resolution = resolution.toProtoOneOfV1,
    )
}

object ResolvedKey {
  def fromProtoV0(
      resolvedKeyP: v0.ViewParticipantData.ResolvedKey
  ): ParsingResult[ResolvedKey] = {
    val v0.ViewParticipantData.ResolvedKey(keyP, resolutionP) = resolvedKeyP
    for {
      keyWithVersion <- ProtoConverter
        .required("ResolvedKey.key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV0)
      LfVersioned(version, key) = keyWithVersion
      // oddity: pass the version from the proto-key to resolution
      resolution <- SerializableKeyResolution.fromProtoOneOfV0(resolutionP, version)
    } yield ResolvedKey(key, resolution)
  }
  def fromProtoV1(
      resolvedKeyP: v1.ResolvedKey
  ): ParsingResult[ResolvedKey] = {
    val v1.ResolvedKey(keyP, resolutionP) = resolvedKeyP
    for {
      keyWithVersion <- ProtoConverter
        .required("ResolvedKey.key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV1)
      LfVersioned(version, key) = keyWithVersion
      // oddity: pass the version from the proto-key to resolution
      resolution <- SerializableKeyResolution.fromProtoOneOfV1(resolutionP, version)
    } yield ResolvedKey(key, resolution)
  }
}
