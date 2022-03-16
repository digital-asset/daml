// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.security.evidence.scalatest

import com.daml.security.evidence.tag.EvidenceTag
import org.scalatest.Tag
import scala.language.implicitConversions

object ScalaTestSupport {
  class TagContainer(testTag: EvidenceTag) extends Tag("SystematicTesting") {
    override val name: String = JsonCodec.encodeEvidenceTag(testTag).noSpaces
  }

  object Implicits {
    implicit def tagToContainer(tag: EvidenceTag): Tag = new TagContainer(tag)
  }
}
