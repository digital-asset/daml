// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultServiceError
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

trait RotateWrapperKeyFailureIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EncryptedCryptoPrivateStoreTestHelpers {

  protected val protectedNodes: Set[String] = Set("participant1")

  protected val disabledKeyId: KmsKeyId

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "fails if we select the same existing wrapper key for a rotation" in { implicit env =>
    import env.*
    val currentWrapperKey = getEncryptedCryptoStore(participant1.name).wrapperKeyId
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.keys.secret.rotate_wrapper_key(currentWrapperKey.unwrap),
      entry => {
        entry.shouldBeCantonErrorCode(
          GrpcVaultServiceError.WrapperKeyAlreadyInUseError.code
        )
        entry.errorMessage should include(
          s"Wrapper key id [$currentWrapperKey] selected for rotation is already being used."
        )
      },
    )
  }

  "fails if we select a non existing wrapper key for a rotation" in { implicit env =>
    import env.*
    val wrongKeyId = KmsKeyId(String300.tryCreate("a_key_that_does_not_exist"))
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.keys.secret.rotate_wrapper_key(wrongKeyId.unwrap),
      _.warningMessage should include(
        s"KMS operation `verify key $wrongKeyId exists and is active` failed: KmsCannotFindKeyError"
      ),
      entry => {
        entry.shouldBeCantonErrorCode(GrpcVaultServiceError.WrapperKeyNotExistError.code)
        entry.errorMessage should include(
          s"Wrapper key id [$wrongKeyId] selected for rotation does not match an existing KMS key id."
        )
      },
    )
  }

  "fails if we select a disabled wrapper key for a rotation" in { implicit env =>
    import env.*
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.keys.secret.rotate_wrapper_key(disabledKeyId.unwrap),
      _.warningMessage should include(
        s"KMS operation `verify key $disabledKeyId exists and is active` failed: KmsKeyDisabledError"
      ),
      entry => {
        entry.shouldBeCantonErrorCode(
          GrpcVaultServiceError.WrapperKeyDisabledOrDeletedError.code
        )
        entry.errorMessage should include(
          s"Wrapper key id [$disabledKeyId] selected for rotation cannot be used " +
            s"because key is disabled or set to be deleted."
        )
      },
    )
  }

  "fails if we rotate a wrapper key without an encrypted private store" in { implicit env =>
    import env.*
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant2.keys.secret.rotate_wrapper_key(),
      entry => {
        entry.shouldBeCantonErrorCode(
          GrpcVaultServiceError.NoEncryptedPrivateKeyStoreError.code
        )
        entry.errorMessage should include(
          "Node is not running an encrypted private store"
        )
      },
      entry => {
        entry.errorMessage should include(
          "GrpcServiceUnavailable: UNIMPLEMENTED/An error occurred. " +
            "Please contact the operator and inquire about the request"
        )
      },
    )
  }

}
