// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { Choice, ContractId, List, Party, Template, TemplateId, Text, lookupTemplate } from '@digitalasset/daml-json-types';
import * as jtv from '@mojotech/json-type-validation';
import fetch from 'cross-fetch';

export type CreateEvent<T> = {
  templateId: TemplateId;
  contractId: ContractId<T>;
  signatories: List<Party>;
  observers: List<Party>;
  agreementText: Text;
  key: unknown;
  argument: T;
  witnessParties: List<Party>;
  workflowId?: string;
}

export type ArchiveEvent<T> = {
  templateId: TemplateId;
  contractId: ContractId<T>;
  witnessParties: List<Party>;
}

export type Event<T> =
  | { created: CreateEvent<T> }
  | { archived: ArchiveEvent<T> }

const decodeTemplateId = (): jtv.Decoder<TemplateId> => jtv.object({
  packageId: jtv.string(),
  moduleName: jtv.string(),
  entityName: jtv.string(),
});

const decodeCreateEvent = <T>(t: Template<T>): jtv.Decoder<CreateEvent<T>> => jtv.object({
  templateId: decodeTemplateId(),
  contractId: ContractId(t).decoder(),
  signatories: List(Party).decoder(),
  observers: List(Party).decoder(),
  agreementText: Text.decoder(),
  key: jtv.unknownJson(),
  argument: t.decoder(),
  witnessParties: List(Party).decoder(),
  workflowId: jtv.optional(jtv.string()),
});

const decodeCreateEventUnknown = (): jtv.Decoder<CreateEvent<unknown>> =>
  jtv.valueAt(['templateId'], decodeTemplateId()).andThen((templateId) =>
    decodeCreateEvent(lookupTemplate(templateId))
  );

const decodeArchiveEventUnknown = (): jtv.Decoder<ArchiveEvent<unknown>> => jtv.object({
  templateId: decodeTemplateId(),
  contractId: ContractId({decoder: jtv.unknownJson}).decoder(),
  witnessParties: List(Party).decoder(),
});

const decodeEventUnknown = (): jtv.Decoder<Event<unknown>> => jtv.oneOf<Event<unknown>>(
  jtv.object({created: decodeCreateEventUnknown()}),
  jtv.object({archived: decodeArchiveEventUnknown()}),
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
  async query<T>(template: Template<T>, query: Query<T>): Promise<CreateEvent<T>[]> {
    const payload = {"%templates": [template.templateId]};
    Object.assign(payload, query);
    const json = await this.submit('contracts/search', payload);
    return jtv.Result.withException(jtv.array(decodeCreateEvent(template)).run(json));
  }

  /**
   * Retrieve all contracts for a given template.
   */
  async fetchAll<T>(template: Template<T>): Promise<CreateEvent<T>[]> {
    return this.query(template, {} as Query<T>);
  }

  /**
   * Mimic DAML's `lookupByKey`. The `key` must be a formulation of the
   * contract key as a query.
   */
  async pseudoLookupByKey<T>(template: Template<T>, key: Query<T>): Promise<CreateEvent<T> | undefined> {
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
  async pseudoFetchByKey<T>(template: Template<T>, key: Query<T>): Promise<CreateEvent<T>> {
    const contract = await this.pseudoLookupByKey(template, key);
    if (contract === undefined) {
      throw Error("pseudoFetchByKey: query returned no contract");
    }
    return contract;
  }

  /**
   * Create a contract for a given template.
   */
  async create<T>(template: Template<T>, argument: T): Promise<CreateEvent<T>> {
    const payload = {
      templateId: template.templateId,
      argument,
    }
    const json = await this.submit('command/create', payload);
    return jtv.Result.withException(decodeCreateEvent(template).run(json));
  }

  /**
   * Exercise a choice on a contract.
   */
  async exercise<T, C, R>(choice: Choice<T, C, R>, contractId: ContractId<T>, argument: C): Promise<[R , Event<unknown>[]]> {
    const payload = {
      templateId: choice.template().templateId,
      contractId,
      choice: choice.choiceName,
      argument,
    };
    const json = await this.submit('command/exercise', payload);
    return jtv.Result.withException(jtv.tuple([choice.resultDecoder(), jtv.array(decodeEventUnknown())]).run(json));
  }

  /**
   * Mimic DAML's `exerciseByKey`. The `key` must be a formulation of the
   * contract key as a query.
   */
  async pseudoExerciseByKey<T, C, R>(choice: Choice<T, C, R>, key: Query<T>, argument: C): Promise<[R, Event<unknown>[]]> {
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
