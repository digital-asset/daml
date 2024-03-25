// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import org.scalatest.Tag

/** Tag used to mark individual test cases as unstable.
  *
  * See [[annotations.UnstableTest]] for more information.
  */
object UnstableTest extends Tag("UnstableTest")
