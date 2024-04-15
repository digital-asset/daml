// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfVersioned
import com.digitalasset.canton.data.SerializableKeyResolution
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class ResolvedKey(key: LfVersioned[LfGlobalKey], resolution: SerializableKeyResolution) {
  def toProtoV30: v30.ViewParticipantData.ResolvedKey =
    v30.ViewParticipantData.ResolvedKey(
      key = Some(GlobalKeySerialization.assertToProto(key)),
      resolution = resolution.toProtoOneOfV30,
    )

  def toPair: (LfGlobalKey, LfVersioned[SerializableKeyResolution]) =
    key.unversioned -> LfVersioned(key.version, resolution)
}

object ResolvedKey {
  def fromProtoV30(
      resolvedKeyP: v30.ViewParticipantData.ResolvedKey
  ): ParsingResult[ResolvedKey] = {
    val v30.ViewParticipantData.ResolvedKey(keyP, resolutionP) = resolvedKeyP
    for {
      key <- ProtoConverter
        .required("ResolvedKey.key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV30)
      resolution <- SerializableKeyResolution.fromProtoOneOfV30(resolutionP)
    } yield ResolvedKey(key, resolution)
  }

  def fromPair(pair: (LfGlobalKey, LfVersioned[SerializableKeyResolution])): ResolvedKey = {
    val (key, LfVersioned(version, resolution)) = pair
    ResolvedKey(LfVersioned(version, key), resolution)
  }
}
