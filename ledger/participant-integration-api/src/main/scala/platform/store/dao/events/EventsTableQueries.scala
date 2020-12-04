// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

private[events] object EventsTableQueries {
  def format(ps: Set[Party]): String =
    ps.view.map(p => s"'$p'").mkString(",")
  def format(ps: List[Party]): String =
    ps.map(p => s"'$p'").mkString(",")
}
