// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.util.concurrent.CompletionStage

import com.digitalasset.daml_lf.DamlLf.Archive

/** An interface for uploading packages via a participant. */
trait WritePackagesService {

  /** Upload a collection of DAML-LF packages to the ledger.
    *
    * This method must be thread-safe, not throw, and not block on IO. It is
    * though allowed to perform significant computation.
    *
    * The result of the archives upload is communicated synchronously.
    * TODO: consider also providing an asynchronous response in a similar
    * manner as it is done for transaction submission. It is possible that
    * in some implementations, upload will fail due to authorization etc.
    *
    * Successful archives upload will result in a [[Update.PublicPackageUploaded]]
    * message. See the comments on [[ReadService.stateUpdates]] and [[Update]] for
    * further details.
    *
    * Note: we accept [[Archive]]s rather than parsed packages, because we want
    * to be able to get the byte size of each individual ArchivePayload, which
    * is information that the read / index service need to provide. Moreover
    * this information should be consistent with the payload that the
    * [[com.digitalasset.ledger.api.v1.package_service.GetPackageResponse]]
    * contains. If we were to consume packages we'd have to re-encode them to
    * provide the size, and the size might potentially be different from the
    * original size, which would be quite confusing.
    *
    * @param sourceDescription : Description provided by the backing participant
    *   describing where it got the package from, e.g., when, where, or by whom
    *   the packages were uploaded.
    *
    * @param payload           : DAML-LF archives to be uploaded to the ledger.
    *
    * @return an async result of a [[UploadPackagesResult]]
    */
  def uploadPackages(
      payload: List[Archive],
      sourceDescription: Option[String]
  ): CompletionStage[UploadPackagesResult]
}
