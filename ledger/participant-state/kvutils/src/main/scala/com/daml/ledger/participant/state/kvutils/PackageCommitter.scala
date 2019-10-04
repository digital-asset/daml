// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.util.concurrent.Executors
import com.codahale.metrics.{Counter, Timer}
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.{Committer, Context}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml_lf.DamlLf.Archive
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

private[kvutils] case class PackageCommitter(engine: Engine)
    extends Committer[DamlPackageUploadEntry, DamlPackageUploadEntry.Builder] {

  private val serialContext =
    ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  private object Metrics {
    val preloadTimer: Timer = metricsRegistry.timer(metricsName("preload-timer"))
    val decodeTimer: Timer = metricsRegistry.timer(metricsName("decode-timer"))
    val accepts: Counter = metricsRegistry.counter(metricsName("accepts"))
  }

  override def init(subm: DamlPackageUploadEntry): DamlPackageUploadEntry.Builder =
    subm.toBuilder

  override val steps: Iterable[Step] = Iterable(
    filterDuplicates,
    (_, subm) => {
      serialContext.execute(preload(subm.getArchivesList.asScala))
      subm
    },
    finish
  )

  private def filterDuplicates(ctx: Context, subm: DamlPackageUploadEntry.Builder) = {
    val archives = subm.getArchivesList.asScala.filter { archive =>
      val stateKey = DamlStateKey.newBuilder
        .setPackageId(archive.getHash)
        .build
      ctx.get(stateKey).isEmpty
    }
    subm.clearArchives().addAllArchives(archives.asJava)
  }

  private def finish(ctx: Context, subm: DamlPackageUploadEntry.Builder) = {
    logger.trace(
      s"Packages committed: ${subm.getArchivesList.asScala.map(_.getHash).mkString(", ")}")

    subm.getArchivesList.forEach { archive =>
      ctx.set(
        DamlStateKey.newBuilder.setPackageId(archive.getHash).build,
        DamlStateValue.newBuilder.setArchive(archive).build
      )
    }

    ctx.done(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(ctx.getRecordTime))
        .setPackageUploadEntry(subm)
        .build,
    )
  }

  private def preload(archives: Iterable[Archive]): Runnable = { () =>
    val ctx = Metrics.preloadTimer.time()
    try {
      logger.trace("Preloading engine...")
      val loadedPackages = engine.compiledPackages().packageIds
      val packages: Map[Ref.PackageId, Ast.Package] = Metrics.decodeTimer.time { () =>
        archives
          .filterNot(
            a =>
              Ref.PackageId
                .fromString(a.getHash)
                .fold(_ => false, loadedPackages.contains))
          .map { archive =>
            Decode.readArchiveAndVersion(archive)._1
          }
          .toMap
      }
      packages.headOption.foreach {
        case (pkgId, pkg) =>
          engine
            .preloadPackage(pkgId, pkg)
            .consume(
              _ => sys.error("Unexpected request to PCS in preloadPackage"),
              pkgId => packages.get(pkgId),
              _ => sys.error("Unexpected request to keys in preloadPackage")
            )
      }
      logger.trace(s"Preload complete.")
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.error("preload exception: $err")
    } finally {
      val _ = ctx.stop()
    }
  }

}
