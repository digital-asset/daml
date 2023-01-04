// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.javaapi.data.codegen.Contract;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.ContractId;
import com.daml.ledger.javaapi.data.codegen.CreateAnd;
import com.daml.ledger.javaapi.data.codegen.Created;
import com.daml.ledger.javaapi.data.codegen.Update;

public abstract class Template extends com.daml.ledger.javaapi.data.codegen.DamlRecord<Template> {

  public abstract Update<? extends Created<? extends ContractId<? extends Template>>> create();

  /**
   * Set up a {@link CreateAndExerciseCommand}; invoke an {@code exercise} method on the result of
   * this to finish creating the command, or convert to an interface first with {@code toInterface}
   * to invoke an interface {@code exercise} method.
   */
  public abstract CreateAnd createAnd();

  // with a self-type tparam to Template, that could replace every occurrence of
  // ? extends Template below
  /**
   * <strong>INTERNAL API</strong>: this is meant for use by {@link ContractCompanion} and {@link
   * com.daml.ledger.javaapi.data.codegen.InterfaceCompanion}, and <em>should not be referenced
   * directly</em>. Applications should refer to other methods like {@link #getContractTypeId}
   * instead.
   *
   * @hidden
   */
  protected abstract ContractCompanion<
          ? extends
              Contract<
                  ? extends com.daml.ledger.javaapi.data.codegen.ContractId<? extends Template>,
                  ? extends Template>,
          ? extends com.daml.ledger.javaapi.data.codegen.ContractId<? extends Template>,
          ? extends Template>
      getCompanion();

  /** The template ID for this template. */
  public final Identifier getContractTypeId() {
    return getCompanion().TEMPLATE_ID;
  }
}
