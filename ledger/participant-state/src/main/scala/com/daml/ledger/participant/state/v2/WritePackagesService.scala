// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.util.concurrent.CompletionStage

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
    * Successful archives upload will result in a [[Update.PublicPackagesUploaded]]
    * message. See the comments on [[ReadService.stateUpdates]] and [[Update]] for
    * further details.
    *
    * @param payload           : DAML-LF packages to be uploaded to the ledger.
    * @param sourceDescription : the description of the packages provided by the
    *                            participant implementation.
    *
    * @return an async result of a SubmissionResult
    */
  // NOTE(FM): we accept dars rather than a list of archives because we want
  // to be able to get the byte size of each individual ArchivePayload, which is
  // information that the read / index service need to provide. Moreover this
  // information should be consistent with the payload that the
  // `GetPackageResponse` contains. If we were to consume archives we'd have
  // to re-encode them to provide the size, and the size might potentially be
  // different from the original size, which would be quite confusing.
  def uploadDar(sourceDescription: String, payload: Array[Byte]): CompletionStage[UploadDarResult]
}
