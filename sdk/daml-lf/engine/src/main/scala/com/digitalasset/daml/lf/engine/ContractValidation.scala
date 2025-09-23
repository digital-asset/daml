// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.interpretation.{Error => IError}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node}
import com.digitalasset.daml.lf.value.Value.ContractId

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
    *   a Result containing a unit on success
    */
  def validate(
      instance: FatContractInstance,
      targetPackageId: Ref.PackageId,
      hashingMethod: Hash.HashingMethod,
      contractIdRewriter: ContractId => ContractId = (cid: ContractId) => cid,
      idValidator: Hash => Boolean,
  ): Result[Either[IError, Unit]]

  /** This method is used to compute the hash of Create nodes during transaction
    * interpretation. This is for cases where a hash is needed for adhoc reasons.
    *
    * @param create
    *   the Create node for which the hash is computed
    * @param hashingMethod
    *   the hashing method to use
    * @param contractIdRewriter
    *   this method is called to rewrite contract ids present in the contract argument before hashing
    * @return
    *   a Result containing the computed hash
    */
  def hash(
      create: Node.Create,
      hashingMethod: Hash.HashingMethod,
      contractIdRewriter: ContractId => ContractId = (cid: ContractId) => cid,
  ): Result[Hash]

}

object ContractValidation {

  def apply(engine: Engine): ContractValidation = {
    new ContractValidationImpl(engine)
  }

  private class ContractValidationImpl(engine: Engine) extends ContractValidation {

    override def validate(
        instance: FatContractInstance,
        targetPackageId: PackageId,
        hashingMethod: Hash.HashingMethod,
        contractIdRewriter: ContractId => ContractId = (cid: ContractId) => cid,
        idValidator: Hash => Boolean,
    ): Result[Either[IError, Unit]] = {
      engine.validateContractInstance(
        instance,
        targetPackageId,
        hashingMethod,
        idValidator,
      )(LoggingContext.empty)
    }

    def hash(
        create: Node.Create,
        hashingMethod: Hash.HashingMethod,
        contractIdRewriter: ContractId => ContractId = (cid: ContractId) => cid,
    ): Result[Hash] = engine.hashCreateNode(create, hashingMethod)
  }
}
