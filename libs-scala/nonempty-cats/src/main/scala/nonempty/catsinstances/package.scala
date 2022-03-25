// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty

import cats.Traverse

package object catsinstances extends catsinstances.impl.CatsInstancesLow {
  implicit def `cats nonempty traverse`[F[_]](implicit F: Traverse[F]): Traverse[NonEmptyF[F, *]] =
    NonEmptyColl.Instance substF F
}
