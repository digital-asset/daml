// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.lifecycle.FlagCloseable

/** A [[HealthElement]] that is its own [[com.digitalasset.canton.lifecycle.FlagCloseable]].
  * Use this when the health reporting shall use inheritance over composition.
  *
  * When combining different [[HealthElement]] traits, mix in this one first
  * so that the [[com.digitalasset.canton.lifecycle.FlagCloseable]] gets initialized first.
  */
trait CloseableHealthElement extends FlagCloseable with HealthElement {
  final override protected def associatedOnShutdownRunner: FlagCloseable = this
}

/** Refines the state of a [[CloseableHealthElement]] to something convertible to a [[ComponentHealthState]] */
trait CloseableHealthQuasiComponent extends CloseableHealthElement with HealthQuasiComponent

/** Fixes the state of a [[CloseableHealthQuasiComponent]] to [[ComponentHealthState]] */
trait CloseableHealthComponent extends CloseableHealthQuasiComponent with HealthComponent
