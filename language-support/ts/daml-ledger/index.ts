// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { Choice, ContractId, List, Party, Template, Text, lookupTemplate } from '@daml/types';
import * as jtv from '@mojotech/json-type-validation';
import fetch from 'cross-fetch';

export type CreateEvent<T extends object, K = unknown, I extends string = string> = {
  templateId: I;
  contractId: ContractId<T>;
  signatories: List<Party>;
  observers: List<Party>;
  agreementText: Text;
  key: K;
  payload: T;
}

export type ArchiveEvent<T extends object, I extends string = string> = {
  templateId: I;
  contractId: ContractId<T>;
}

export type Event<T extends object, K = unknown, I extends string = string> =
  | { created: CreateEvent<T, K, I> }
  | { archived: ArchiveEvent<T, I> }

const decodeCreateEvent = <T extends object, K, I extends string>(template: Template<T, K, I>): jtv.Decoder<CreateEvent<T, K, I>> => jtv.object({
  templateId: jtv.constant(template.templateId),
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
  async query<T extends object, K, I extends string>(template: Template<T, K, I>, query: Query<T>): Promise<CreateEvent<T, K, I>[]> {
    const payload = {templateIds: [template.templateId], query};
    const json = await this.submit('contracts/search', payload);
    return jtv.Result.withException(jtv.array(decodeCreateEvent(template)).run(json));
  }

  /**
   * Retrieve all contracts for a given template.
   */
  async fetchAll<T extends object, K, I extends string>(template: Template<T, K, I>): Promise<CreateEvent<T, K, I>[]> {
    return this.query(template, {} as Query<T>);
  }

  /**
   * Fetch a contract by its key.
   */
  async lookupByKey<T extends object, K, I extends string>(template: Template<T, K, I>, key: K): Promise<CreateEvent<T, K, I> | null> {
    if (key === undefined) {
      throw Error(`Cannot lookup by key on template ${template.templateId} because it does not define a key.`);
    }
    const payload = {
      templateId: template.templateId,
      key,
    };
    const json = await this.submit('contracts/lookup', payload);
    return jtv.Result.withException(jtv.oneOf(jtv.constant(null), decodeCreateEvent(template)).run(json));
  }

  /**
   * Create a contract for a given template.
   */
  async create<T extends object, K, I extends string>(template: Template<T, K, I>, payload: T): Promise<CreateEvent<T, K, I>> {
    const command = {
      templateId: template.templateId,
      payload,
    };
    const json = await this.submit('command/create', command);
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
  async exerciseByKey<T extends object, C, R, K>(choice: Choice<T, C, R, K>, key: K, argument: C): Promise<[R, Event<object>[]]> {
    if (key === undefined) {
      throw Error(`Cannot exercise by key on template ${choice.template().templateId} because it does not define a key.`);
    }
    const payload = {
      templateId: choice.template().templateId,
      key,
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
   * Archive a contract.
   */
  async archive<T extends object>(template: Template<T>, contractId: ContractId<T>): Promise<unknown> {
    return this.exercise(template.Archive, contractId, {});
  }

  /**
   * Archive a contract identified by its key.
   */
  async archiveByKey<T extends object>(template: Template<T>, key: Query<T>): Promise<unknown> {
    return this.exerciseByKey(template.Archive, key, {});
  }
}

export default Ledger;
