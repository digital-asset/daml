// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.packages

import java.util.concurrent.ConcurrentHashMap

import com.daml.metrics.api.MetricHandle.Timer
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.daml.metrics.Timed

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/** Deduplicates package load requests, such that concurrent requests
  * only access the data store and decode the package once.
  */
private[platform] class DeduplicatingPackageLoader() {
  // Concurrent map of promises to request each package only once.
  private[this] val packagePromises: ConcurrentHashMap[Ref.PackageId, Promise[Option[Package]]] =
    new ConcurrentHashMap()

  def loadPackage(
      packageId: PackageId,
      delegate: PackageId => Future[Option[DamlLf.Archive]],
      metric: Timer,
  )(implicit ec: ExecutionContext): Future[Option[Package]] = {
    var gettingPackage = false

    val promise = packagePromises.computeIfAbsent(
      packageId,
      _ => {
        gettingPackage = true
        Promise[Option[Package]]()
      },
    )

    if (gettingPackage) {
      val future =
        Timed.future(
          metric,
          delegate(packageId)
            .flatMap(archiveO =>
              Future.fromTry(Try(archiveO.map(archive => Decode.assertDecodeArchive(archive)._2)))
            ),
        )
      future.onComplete {
        case Success(None) | Failure(_) =>
          // Did not find the package or got an error when looking for it. Remove the promise to allow later retries.
          packagePromises.remove(packageId)

        case Success(Some(_)) =>
        // we don't need to treat a successful package fetch here
      }
      promise.completeWith(future)
    }

    promise.future
  }
}
