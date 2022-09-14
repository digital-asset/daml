// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

sealed trait UpdatePathError {
  def getReason: String = getClass.getSimpleName
}

object UpdatePathError {

  private def shorten(s: String): String =
    if (s.length > 53) { s.take(50) + "..." }
    else s

  final case class MergeUpdateModifierOnEmptyMapField(rawUpdatePath: String)
      extends UpdatePathError {
    override def getReason: String =
      s"The update path: '${shorten(rawUpdatePath)}' for a map field contains an explicit '!merge' modifier but the new value is the empty map. " +
        "This would result in a no-up update for this field and is probably a mistake."
  }

  final case class MergeUpdateModifierOnPrimitiveFieldWithDefaultValue(rawUpdatePath: String)
      extends UpdatePathError {
    override def getReason: String =
      s"The update path: '${shorten(rawUpdatePath)}' for a primitive field contains an explicit '!merge' modifier but the new value is the default value. " +
        "This would result in a no-up update for this field and is probably a mistake."
  }

  final case class UnknownOrUnmodifiableFieldPath(rawUpdatePath: String) extends UpdatePathError {
    override def getReason: String =
      s"The update path: '${shorten(rawUpdatePath)}' points to an unknown or an unmodifiable field."
  }

  final case class UnknownUpdateModifier(rawUpdatePath: String) extends UpdatePathError {
    override def getReason: String =
      s"The update path: '${shorten(rawUpdatePath)}' contains an unknown update modifier."
  }

  final case class InvalidUpdatePathSyntax(rawUpdatePath: String) extends UpdatePathError {
    override def getReason: String =
      s"The update path: '${shorten(rawUpdatePath)}' has invalid syntax."
  }

  final case class DuplicatedFieldPath(rawUpdatePath: String) extends UpdatePathError {
    override def getReason: String =
      s"The update path: '${shorten(rawUpdatePath)}' points to a field a different update path already points to."
  }

  final case object EmptyUpdateMask extends UpdatePathError {
    override def getReason: String = "The update mask contains no entries"
  }
}
