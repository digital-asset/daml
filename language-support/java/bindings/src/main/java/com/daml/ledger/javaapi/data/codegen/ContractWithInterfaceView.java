// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import java.util.Optional;
import java.util.Set;

final class ContractWithInterfaceView<Id, View> extends Contract<Id, View> {

  private final ContractTypeCompanion<?, View> contractTypeCompanion;

  ContractWithInterfaceView(
      ContractTypeCompanion<?, View> contractTypeCompanion,
      Id id,
      View interfaceView,
      Optional<String> agreementText,
      Set<String> signatories,
      Set<String> observers) {
    super(id, interfaceView, agreementText, signatories, observers);
    this.contractTypeCompanion = contractTypeCompanion;
  }

  @Override
  protected ContractTypeCompanion<?, View> getCompanion() {
    return contractTypeCompanion;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ContractWithInterfaceView && super.equals(object);
  }
}
