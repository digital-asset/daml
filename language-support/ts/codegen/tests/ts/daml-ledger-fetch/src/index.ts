// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Choice, Contract, ContractId, Party, Template, Query, TemplateId, Serializable } from '@digitalasset/daml-json-types';
import * as jtv from '@mojotech/json-type-validation';
import fetch from 'cross-fetch';

type LedgerResponse = {
  status: number;
  result: unknown;
}

type LedgerError = {
  status: number;
  errors: string[];
}

export type CreatedEvent = {
  created: Contract<object>;
}

export type ArchivedEvent = {
  archived: {
    templateId: TemplateId;
    contractId: ContractId<object>;
    witnessParties: Party[];
  };
}

export type Event = CreatedEvent | ArchivedEvent;

// FIXME(MH): The cast below is a gross hack relying on the fact that
// `Contract` will only use the `decoder` field of its argument. We should
// instead provide something like a proper `AnyContract` in `daml-json-types`.
const AnyTemplate: Template<object> = {
  decoder: () => jtv.object({}),
} as Template<object>;

const CreatedEvent: Serializable<CreatedEvent> = {
  decoder: () => jtv.object({
    created: Contract(AnyTemplate).decoder(),
  }),
};

const ArchivedEvent: Serializable<ArchivedEvent> = {
  decoder: () => jtv.object({
    archived: jtv.object({
      templateId: TemplateId.decoder(),
      contractId: ContractId(AnyTemplate).decoder(),
      witnessParties: jtv.array(Party.decoder()),
    }),
  }),
};

const Event: Serializable<Event> = {
  decoder: () => jtv.oneOf<Event>(CreatedEvent.decoder(), ArchivedEvent.decoder()),
}

/**
 * An object of type `Ledger` represents a handle to a DAML ledger.
 */
class Ledger {
  private readonly token: string;
  private readonly baseUrl: string;

  constructor(token: string, baseUrl?: string) {
    this.token = token;
    if (!baseUrl) {
      this.baseUrl = '';
    } else if (baseUrl.endsWith('/')) {
      this.baseUrl = baseUrl;
    } else {
      throw Error(`The ledger base URL must end in a '/'. (${baseUrl})`);
    }
  }

  /**
   * Internal function to submit a command to the JSON API.
   */
  private async submit(endpoint: string, payload: unknown): Promise<unknown> {
    const httpResponse = await fetch(this.baseUrl + endpoint, {
      body: JSON.stringify(payload),
      headers: {
        'Authorization': 'Bearer ' + this.token,
        'Content-type': 'application/json'
      },
      method: 'post',
    });
    const json = await httpResponse.json();
    if (!httpResponse.ok) {
      console.log(json);
      // TODO(MH): Validate.
      const ledgerError = json as LedgerError;
      throw ledgerError;
    }
    // TODO(MH): Validate.
    const ledgerResponse = json as LedgerResponse;
    return ledgerResponse.result;
  }

  /**
   * Retrieve all contracts for a given template which match a query. See
   * https://github.com/digital-asset/daml/blob/master/docs/source/json-api/search-query-language.rst
   * for a description of the query language.
   */
  async query<T>(template: Template<T>, query: Query<T>): Promise<Contract<T>[]> {
    const payload = {"%templates": [template.templateId]};
    Object.assign(payload, query);
    const json = await this.submit('contracts/search', payload);
    return jtv.Result.withException(jtv.array(Contract(template).decoder()).run(json));
  }

  /**
   * Retrieve all contracts for a given template.
   */
  async fetchAll<T>(template: Template<T>): Promise<Contract<T>[]> {
    return this.query(template, {} as Query<T>);
  }

  /**
   * Mimic DAML's `lookupByKey`. The `key` must be a formulation of the
   * contract key as a query.
   */
  async pseudoLookupByKey<T>(template: Template<T>, key: Query<T>): Promise<Contract<T> | undefined> {
    const contracts = await this.query(template, key);
    if (contracts.length > 1) {
      throw Error("pseudoLookupByKey: query returned multiple contracts");
    }
    return contracts[0];
  }

  /**
   * Mimic DAML's `fetchByKey`. The `key` must be a formulation of the
   * contract key as a query.
   */
  async pseudoFetchByKey<T>(template: Template<T>, key: Query<T>): Promise<Contract<T>> {
    const contract = await this.pseudoLookupByKey(template, key);
    if (contract === undefined) {
      throw Error("pseudoFetchByKey: query returned no contract");
    }
    return contract;
  }

  /**
   * Create a contract for a given template.
   */
  async create<T>(template: Template<T>, argument: T): Promise<Contract<T>> {
    const payload = {
      templateId: template.templateId,
      argument,
    }
    const json = await this.submit('command/create', payload);
    return jtv.Result.withException(Contract(template).decoder().run(json));
  }

  /**
   * Exercise a choice on a contract.
   */
  async exercise<T, C>(choice: Choice<T, C>, contractId: ContractId<T>, argument: C): Promise<Event[]> {
    const payload = {
      templateId: choice.template().templateId,
      contractId,
      choice: choice.choiceName,
      argument,
    };
    const json = await this.submit('command/exercise', payload);
    return jtv.Result.withException(jtv.array(Event.decoder()).run(json));
  }

  /**
   * Mimic DAML's `exerciseByKey`. The `key` must be a formulation of the
   * contract key as a query.
   */
  async pseudoExerciseByKey<T, C>(choice: Choice<T, C>, key: Query<T>, argument: C): Promise<Event[]> {
    const contract = await this.pseudoFetchByKey(choice.template(), key);
    return this.exercise(choice, contract.contractId, argument);
  }

  /**
   * Archive a contract given by its contract id.
   */
  async archive<T>(template: Template<T>, contractId: ContractId<T>): Promise<unknown> {
    return this.exercise(template.Archive, contractId, {});
  }

  /**
   * Archive a contract given by its contract id.
   */
  async pseudoArchiveByKey<T>(template: Template<T>, key: Query<T>): Promise<unknown> {
    return this.pseudoExerciseByKey(template.Archive, key, {});
  }
}

export default Ledger;

