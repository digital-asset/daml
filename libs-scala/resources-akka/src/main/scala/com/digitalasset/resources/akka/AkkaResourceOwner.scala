// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources.akka

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.digitalasset.resources.ResourceOwner

object AkkaResourceOwner {
  def forActorSystem(acquire: () => ActorSystem): ResourceOwner[ActorSystem] =
    new ActorSystemResourceOwner(acquire)

  def forMaterializer(acquire: () => Materializer): ResourceOwner[Materializer] =
    new ActorMaterializerResourceOwner(acquire)
}
