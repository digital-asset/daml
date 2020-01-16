// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { Choice, ContractId, List, Party, Template, Text, lookupTemplate } from '@digitalasset/daml-json-types';
import * as jtv from '@mojotech/json-type-validation';
import fetch from 'cross-fetch';

export type CreateEvent<T extends object, K = unknown> = {
  templateId: string;
  contractId: ContractId<T>;
  signatories: List<Party>;
  observers: List<Party>;
  agreementText: Text;
  key: K;
  payload: T;
}

export type ArchiveEvent<T extends object> = {
  templateId: string;
  contractId: ContractId<T>;
}

export type Event<T extends object, K = unknown> =
  | { created: CreateEvent<T, K> }
  | { archived: ArchiveEvent<T> }

const decodeCreateEvent = <T extends object, K>(template: Template<T, K>): jtv.Decoder<CreateEvent<T, K>> => jtv.object({
  templateId: jtv.string(),
  contractId: ContractId(template).decoder(),
  signatories: List(Party).decoder(),
  observers: List(Party).decoder(),
  agreementText: Text.decoder(),
  key: template.keyDecoder(),
  payload: template.decoder(),
});

const decodeCreateEventUnknown: jtv.Decoder<CreateEvent<object>> =
  jtv.valueAt(['templateId'], jtv.string()).andThen((templateId) =>
    decodeCreateEvent(lookupTemplate(templateId))
  );

const decodeArchiveEventUnknown: jtv.Decoder<ArchiveEvent<object>> = jtv.object({
  templateId: jtv.string(),
  contractId: ContractId({decoder: jtv.unknownJson}).decoder(),
});

const decodeEventUnknown: jtv.Decoder<Event<object>> = jtv.oneOf<Event<object>>(
  jtv.object({created: decodeCreateEventUnknown}),
  jtv.object({archived: decodeArchiveEventUnknown}),
);

/**
 * Type for queries against the `/contract/search` endpoint of the JSON API.
 * `Query<T>` is the type of queries that are valid when searching for
 * contracts of template type `T`.
 *
 * Comparison queries are not yet supported.
 *
 * NB: This type is heavily related to the `DeepPartial` type that can be found
 * in the TypeScript community.
 */
export type Query<T> = T extends object ? {[K in keyof T]?: Query<T[K]>} : T;
// TODO(MH): Support comparison queries.


type LedgerResponse = {
  status: number;
  result: unknown;
}

type LedgerError = {
  status: number;
  errors: string[];
}

const decodeLedgerResponse: jtv.Decoder<LedgerResponse> = jtv.object({
  status: jtv.number(),
  result: jtv.unknownJson(),
});

const decodeLedgerError: jtv.Decoder<LedgerError> = jtv.object({
  status: jtv.number(),
  errors: jtv.array(jtv.string()),
});

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
      throw jtv.Result.withException(decodeLedgerError.run(json));
    }
    const ledgerResponse = jtv.Result.withException(decodeLedgerResponse.run(json));
    return ledgerResponse.result;
  }

  /**
   * Retrieve all contracts for a given template which match a query. See
   * https://github.com/digital-asset/daml/blob/master/docs/source/json-api/search-query-language.rst
   * for a description of the query language.
   */
  async query<T extends object, K>(template: Template<T, K>, query: Query<T>): Promise<CreateEvent<T, K>[]> {
    const payload = {"%templates": [template.templateId], ...query};
    const json = await this.submit('contracts/search', payload);
    return jtv.Result.withException(jtv.array(decodeCreateEvent(template)).run(json));
  }

  /**
   * Retrieve all contracts for a given template.
   */
  async fetchAll<T extends object, K>(template: Template<T, K>): Promise<CreateEvent<T, K>[]> {
    return this.query(template, {} as Query<T>);
  }

  async lookupByKey<T extends object, K>(template: Template<T, K>, key: K extends undefined ? never : K): Promise<CreateEvent<T, K> | null> {
    const payload = {
      templateId: template.templateId,
      key,
    };
    const json = await this.submit('contracts/lookup', payload);
    return jtv.Result.withException(jtv.oneOf(jtv.constant(null), decodeCreateEvent(template)).run(json));
  }

  /**
   * Mimic DAML's `lookupByKey`. The `key` must be a formulation of the
   * contract key as a query.
   */
  async pseudoLookupByKey<T extends object, K>(template: Template<T, K>, key: Query<T>): Promise<CreateEvent<T, K> | undefined> {
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
  async pseudoFetchByKey<T extends object, K>(template: Template<T, K>, key: Query<T>): Promise<CreateEvent<T, K>> {
    const contract = await this.pseudoLookupByKey(template, key);
    if (contract === undefined) {
      throw Error("pseudoFetchByKey: query returned no contract");
    }
    return contract;
  }

  /**
   * Create a contract for a given template.
   */
  async create<T extends object, K>(template: Template<T, K>, argument: T): Promise<CreateEvent<T, K>> {
    const payload = {
      templateId: template.templateId,
      argument,
    };
    const json = await this.submit('command/create', payload);
    return jtv.Result.withException(decodeCreateEvent(template).run(json));
  }

  /**
   * Exercise a choice on a contract.
   */
  async exercise<T extends object, C, R>(choice: Choice<T, C, R>, contractId: ContractId<T>, argument: C): Promise<[R , Event<object>[]]> {
    const payload = {
      templateId: choice.template().templateId,
      contractId,
      choice: choice.choiceName,
      argument,
    };
    const json = await this.submit('command/exercise', payload);
    // Decode the server response into a tuple.
    const responseDecoder: jtv.Decoder<{exerciseResult: R; contracts: Event<object>[]}> = jtv.object({
      exerciseResult: choice.resultDecoder(),
      contracts: jtv.array(decodeEventUnknown),
    });
    const {exerciseResult, contracts} = jtv.Result.withException(responseDecoder.run(json));
    return [exerciseResult, contracts];
  }

  /**
   * Exercise a choice on a contract identified by its contract key.
   */
  async exerciseByKey<T extends object, C, R, K>(choice: Choice<T, C, R, K>, key: K extends undefined ? never : K, argument: C): Promise<[R, Event<object>[]]> {
    const contract = await this.lookupByKey(choice.template(), key);
    if (contract === null) {
      throw Error(`exerciseByKey: no contract with key ${JSON.stringify(key)} for template ${choice.template().templateId}`);
    }
    return this.exercise(choice, contract.contractId, argument);
  }

  /**
   * Mimic DAML's `exerciseByKey`. The `key` must be a formulation of the
   * contract key as a query.
   */
  async pseudoExerciseByKey<T extends object, C, R>(choice: Choice<T, C, R>, key: Query<T>, argument: C): Promise<[R, Event<object>[]]> {
    const contract = await this.pseudoFetchByKey(choice.template(), key);
    return this.exercise(choice, contract.contractId, argument);
  }

  /**
   * Archive a contract given by its contract id.
   */
  async archive<T extends object>(template: Template<T>, contractId: ContractId<T>): Promise<unknown> {
    return this.exercise(template.Archive, contractId, {});
  }

  /**
   * Archive a contract given by its contract id.
   */
  async pseudoArchiveByKey<T extends object>(template: Template<T>, key: Query<T>): Promise<unknown> {
    return this.pseudoExerciseByKey(template.Archive, key, {});
  }
}

export default Ledger;
