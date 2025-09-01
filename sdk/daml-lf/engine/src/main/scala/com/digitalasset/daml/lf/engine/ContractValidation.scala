// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.Error.Interpretation
import com.digitalasset.daml.lf.transaction.FatContractInstance
import com.digitalasset.daml.lf.value.Value.ContractId

/** Methods to build and validate contract instances */
trait ContractValidation {

  /** Validates the contract by performing the following checks
    * - Verifies that the argument type checks against the target package
    * - Verifies that the ensures clause does not have assertion failures
    * - Check that the metadata in the contract is consistent with that produced by the target package
    * - Hashes the contract instance, after rewriting contract ids, using the specified hashing method
    * - Passes this hash to the idValidator and verifies it returns true
    *
    * @param instance
    *   the contract instance to validate
    * @param targetPackageId
    *   the target package id against which the instance is validated
    * @param hashingMethod
    *   the hash type to use for validation
    * @param contractIdRewriter
    *   this method is called to rewrite contract ids present in the contract argument before hashing
    * @param idValidator
    *   a function that checks if a given hash is valid
    * @return
    *   a Result containing an unit either on success and a string error message on failure
    */
  def validate(
      instance: FatContractInstance,
      targetPackageId: Ref.PackageId,
      hashingMethod: Hash.HashingMethod,
      contractIdRewriter: ContractId => ContractId = (cid: ContractId) => cid,
      idValidator: Hash => Boolean,
  ): Result[Either[String, Unit]]

  /** This method is used to compute the hash of contract instances during transaction
    * interpretation. This is for cases where a hash is needed for adhoc reasons.
    *
    * @param instance
    *   the contract instance for which the hash is computed
    * @param targetPackageId
    *   the target package id against which the instance is hashed
    * @param hashingMethod
    *   the hashing method to use
    * @param contractIdRewriter
    *   this method is called to rewrite contract ids present in the contract argument before hashing
    * @return
    *   a Result containing the computed hash
    */
  def hash(
      instance: FatContractInstance,
      targetPackageId: Ref.PackageId,
      hashingMethod: Hash.HashingMethod,
      contractIdRewriter: ContractId => ContractId = (cid: ContractId) => cid,
  ): Result[Hash]

}

object ContractValidation {

  def apply(engine: Engine): ContractValidation = {
    discard(engine)
    LegacyContractValidation
  }

  private object LegacyContractValidation extends ContractValidation {

    override def validate(
        instance: FatContractInstance,
        targetPackageId: PackageId,
        hashingMethod: Hash.HashingMethod,
        contractIdRewriter: ContractId => ContractId = (cid: ContractId) => cid,
        idValidator: Hash => Boolean,
    ): Result[Either[String, Unit]] = {

      hashInternal(instance, hashingMethod) match {
        case Right(hash) if idValidator(hash) =>
          // Missing checks:
          // - Verification that the ensures clause does not have assertion failures
          // - Verification that the metadata in the contract is consistent with that produced by the target package
          ResultDone(Right(()))

        case Right(_) =>
          ResultDone(Left(s"Contract did not validate"))

        case Left(err) =>
          ResultDone(Left(err))
      }

    }

    override def hash(
        instance: FatContractInstance,
        targetPackageId: PackageId,
        hashingMethod: Hash.HashingMethod,
        contractIdRewriter: ContractId => ContractId = (cid: ContractId) => cid,
    ): Result[Hash] = {
      hashInternal(instance, hashingMethod) match {
        case Right(hash) => ResultDone(hash)
        case Left(err) => ResultError(Interpretation.Internal("LegacyLfApi.hash", err, None))
      }
    }

    private def hashInternal(
        instance: FatContractInstance,
        hashingMethod: Hash.HashingMethod,
    ): Either[String, Hash] = {

      val upgradeFriendlyO = hashingMethod match {
        case Hash.HashingMethod.Legacy => Some(false)
        case Hash.HashingMethod.UpgradeFriendly => Some(true)
        case _ => None
      }

      upgradeFriendlyO match {
        case Some(upgradeFriendly) =>
          Hash.hashContractInstance(
            instance.templateId,
            instance.createArg,
            instance.packageName,
            upgradeFriendly = upgradeFriendly,
          )

        case None =>
          Left(s"Hashing method $hashingMethod not supported")
      }
    }
  }
}
