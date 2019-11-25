// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import scala.collection.generic.CanBuildFrom

abstract class PrimitiveInstances

object PrimitiveInstances {

  implicit def textMapCanBuildFrom[V]
    : CanBuildFrom[Primitive.TextMap.Coll, (String, V), Primitive.TextMap[V]] =
    Primitive.TextMap.canBuildFrom

  implicit def genMapCanBuildFrom[K, V]
    : CanBuildFrom[Primitive.GenMap.Coll, (K, V), Primitive.GenMap[K, V]] =
    Primitive.GenMap.canBuildFrom

}
