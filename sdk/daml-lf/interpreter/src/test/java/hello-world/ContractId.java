// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

interface ContractId[T <: Template] {
   public id: Ref.Identifier

   public default T fetch() {
       arg = ledger.api.fetchContractArg(T.templateId, id);
       return new T(arg);
   }

   public default R exercise<R>(choiceName: String, arg: LfValue) {
        // TODO: convert LfValue to R
        return ledger.api.exerciseChoice(T.templateId, id, choiceName, arg);
   }
}

