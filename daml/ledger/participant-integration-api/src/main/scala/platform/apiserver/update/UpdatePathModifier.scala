// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

object UpdatePathModifier {
  object Merge extends UpdatePathModifier {
    override def toRawString: String = "!merge"
  }
  object Replace extends UpdatePathModifier {
    override def toRawString: String = "!replace"
  }
  object NoModifier extends UpdatePathModifier {
    override def toRawString: String = ""
  }
}

sealed trait UpdatePathModifier {
  def toRawString: String
}
