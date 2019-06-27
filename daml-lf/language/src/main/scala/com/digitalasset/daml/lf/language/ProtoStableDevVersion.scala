// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

abstract class ProtoStableDevVersion extends Product with Serializable {
  def toProtoIdentifier: String
  final def protoValue: String = toProtoIdentifier
}

abstract class ProtoStableDevVersionCompanion[Root] extends (String => Root) {
  type Stable <: Root
  val Stable: String => Stable
  val Dev: Root

  final def apply(identifier: String): Root = fromProtoIdentifier(identifier)

  final def fromProtoIdentifier(identifier: String): Root = identifier match {
    case "dev" => Dev
    case _ => Stable(identifier)
  }
}
