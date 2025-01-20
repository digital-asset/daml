// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pruning

import com.digitalasset.canton.config.CantonRequireTypes.String100
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.store.db.DbSerializationException
import slick.jdbc.{GetResult, SetParameter}

sealed trait PruningPhase extends Product with Serializable with PrettyPrinting {
  def kind: String
  // lazy val so that `kind` is initialized first in the subclasses
  final lazy val toDbPrimitive: String100 =
    // The Oracle DB schemas set a limit of 100 characters, Postgres and H2 map it to an enum
    String100.tryCreate(kind)
  def index: Int

  override def pretty: Pretty[PruningPhase] = prettyOfParam(_.kind.unquoted)
}

object PruningPhase {
  case object Started extends PruningPhase {
    override val kind: String = "started"
    override def index: Int = 0
  }
  case object Completed extends PruningPhase {
    override val kind: String = "completed"
    override def index: Int = 1
  }

  def tryFromDbPrimitive: String => PruningPhase = {
    case Started.kind => Started
    case Completed.kind => Completed
    case other => throw new DbSerializationException(s"Unknown pruning phase $other")
  }

  implicit val orderingPruningPhase: Ordering[PruningPhase] =
    Ordering.by[PruningPhase, Int](_.index)

  implicit val getResultPruningPhase: GetResult[PruningPhase] =
    GetResult(r => PruningPhase.tryFromDbPrimitive(r.nextString()))
  implicit val setParameterPruningPhase: SetParameter[PruningPhase] = (d, pp) =>
    pp >> d.toDbPrimitive
}

final case class PruningStatus(
    phase: PruningPhase,
    timestamp: CantonTimestamp,
    lastSuccess: Option[CantonTimestamp],
) extends PrettyPrinting {
  override def pretty: Pretty[PruningStatus] = prettyOfClass(
    param("phase", _.phase),
    param("timestamp", _.timestamp),
    param("lastSuccess", _.lastSuccess),
  )
}

object PruningStatus {
  implicit val orderingPruningStatus: Ordering[PruningStatus] =
    Ordering.by[PruningStatus, (CantonTimestamp, PruningPhase)](status =>
      (status.timestamp, status.phase)
    )

  implicit val getResultPruningStatus: GetResult[PruningStatus] = GetResult(r =>
    PruningStatus(
      PruningPhase.getResultPruningPhase(r),
      GetResult[CantonTimestamp].apply(r),
      GetResult[Option[CantonTimestamp]].apply(r),
    )
  )
}
