// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import java.util.Optional;
import java.util.Set;

final class ContractWithInterfaceView<Id, View> extends Contract<Id, View> {

  private final InterfaceCompanion<?, Id, View> contractTypeCompanion;

  ContractWithInterfaceView(
      InterfaceCompanion<?, Id, View> contractTypeCompanion,
      Id id,
      View interfaceView,
      Set<String> signatories,
      Set<String> observers) {
    super(id, interfaceView, signatories, observers);
    this.contractTypeCompanion = contractTypeCompanion;
  }

  @Override
  protected InterfaceCompanion<?, Id, View> getCompanion() {
    return contractTypeCompanion;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ContractWithInterfaceView && super.equals(object);
  }
}
