// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

public abstract class Template(templateId: Ref.TemplateId) {

    // FIXME: use reflection to collect all @arg attributes and build an LfValue
    public arg: LfValue = ???

    // TODO: want following to be static
    public static ContractId<T> create[T <: Template]() {
        return ledger.api.createContract(templateId, arg);
    }

    public Boolean precond() {
        return true;
    }

    public List<Ref.Party> signatories() {
        return List.empty;
    }

    public List<Ref.Party> observers {
        return List.empty;
    }
}

