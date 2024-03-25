// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import ammonite.interp.api.APIHolder
import ammonite.util.Bind

/** ammonite requires a ApiHolder in this pattern to make items through bindings available within the dynamic Console environment.
  */
final case class BindingsHolder(bindings: IndexedSeq[Bind[_]])
object BindingsBridge extends APIHolder[BindingsHolder]
