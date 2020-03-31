// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import * as jtv from '@mojotech/json-type-validation';

/**
 * Interface for companion objects of serializable types. Its main purpose is
 * to describe the JSON encoding of values of the serializable type.
 *
 * @typeparam T The template type.
 */
export interface Serializable<T> {
  /**
   * The decoder for a contract of template T.
   *
   * NB: This is a function to allow for mutually recursive decoders.
   */
  decoder: () => jtv.Decoder<T>;
}

/**
 * Interface for objects representing DAML templates. It is similar to the
 * `Template` type class in DAML.
 *
 * @typeparam T The template type.
 * @typeparam K The contract key type.
 * @typeparam I The contract id type.
 *
 */
export interface Template<T extends object, K = unknown, I extends string = string> extends Serializable<T> {
  templateId: I;
  keyDecoder: () => jtv.Decoder<K>;
  Archive: Choice<T, {}, {}, K>;
}

/**
 * Interface for objects representing DAML choices.
 *
 * @typeparam T The template type.
 * @typeparam K The contract key type.
 * @typeparam C The choice type.
 * @typeparam R The choice return type.
 *
 */
export interface Choice<T extends object, C, R, K = unknown> {
  /**
   * Returns the template to which this choice belongs.
   */
  template: () => Template<T, K>;
  /**
   * Returns a decoder to decode the choice arguments.
   */
  argumentDecoder: () => jtv.Decoder<C>;
  /**
   * Returns a deocoder to decode the return value.
   */
  resultDecoder: () => jtv.Decoder<R>;
  /**
   * The choice name.
   */
  choiceName: string;
}

/**
 * @internal
 */
const registeredTemplates: {[key: string]: Template<object>} = {};

/**
 * @internal
 */
export const registerTemplate = <T extends object>(template: Template<T>): void => {
  const templateId = template.templateId;
  const oldTemplate = registeredTemplates[templateId];
  if (oldTemplate === undefined) {
    registeredTemplates[templateId] = template;
    console.debug(`Registered template ${templateId}.`);
  } else {
    console.warn(`Trying to re-register template ${templateId}.`);
  }
}

/**
 * @internal
 */
export const lookupTemplate = (templateId: string): Template<object> => {
  const template = registeredTemplates[templateId];
  if (template === undefined) {
    throw Error(`Trying to look up template ${templateId}.`);
  }
  return template;
}

/**
 * The counterpart of DAML's `()` type.
 */
export type Unit = {};

/**
 * Companion obect of the [[Unit]] type.
 */
export const Unit: Serializable<Unit> = {
  decoder: () => jtv.object({}),
}

/**
 * The counterpart of DAML's `Bool` type.
 */
export type Bool = boolean;

/**
 * Companion object of the [[Bool]] type.
 */
export const Bool: Serializable<Bool> = {
  decoder: jtv.boolean,
}

/**
 * The counterpart of DAML's `Int` type.
 *
 * We represent `Int`s as string in order to avoid a loss of precision.
 */
export type Int = string;

/**
 * Companion object of the [[Int]] type.
 */
export const Int: Serializable<Int> = {
  decoder: jtv.string,
}

/**
 * The counterpart of DAML's `Numeric` type.
 *
 * We represent `Numeric`s as string in order to avoid a loss of precision. The string must match
 * the regular expression `-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?`.
 */
export type Numeric = string;

/**
 * The counterpart of DAML's Decimal type.
 *
 * In DAML, Decimal's are the same as Numeric with precision 10.
 *
 */
export type Decimal = Numeric;

/**
 * Companion function of the [[Numeric]] type.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const Numeric = (_: number): Serializable<Numeric> =>
  ({
    decoder: jtv.string,
  })

/**
 * Companion object of the [[Decimal]] type.
 */
export const Decimal: Serializable<Decimal> = Numeric(10)

/**
 * The counterpart of DAML's `Text` type.
 */
export type Text = string;

/**
 * Companion object of the [[Text]] type.
 */
export const Text: Serializable<Text> = {
  decoder: jtv.string,
}

/**
 * The counterpart of DAML's `Time` type.
 *
 * We represent `Times`s as strings with format `YYYY-MM-DDThh:mm:ss[.ssssss]Z`.
 */
export type Time = string;

/**
 * Companion object of the [[Time]] type.
 */
export const Time: Serializable<Time> = {
  decoder: jtv.string,
}

/**
 * The counterpart of DAML's `Party` type.
 *
 * We represent `Party`s as strings matching the regular expression `[A-Za-z0-9:_\- ]+`.
 */
export type Party = string;

/**
 * Companion object of the [[Party]] type.
 */
export const Party: Serializable<Party> = {
  decoder: jtv.string,
}

/**
 * The counterpart of DAML's `[T]` list type.
 *
 * We represent lists using arrays.
 *
 * @typeparam T The type of the list values.
 */
export type List<T> = T[];

/**
 * Companion object of the [[List]] type.
 */
export const List = <T>(t: Serializable<T>): Serializable<T[]> => ({
  decoder: (): jtv.Decoder<T[]> => jtv.array(t.decoder()),
});

/**
 * The counterpart of DAML's `Date` type.
 *
 * We represent `Date`s as strings with format `YYYY-MM-DD`.
 */
export type Date = string;

/**
 * Companion object of the [[Date]] type.
 */
export const Date: Serializable<Date> = {
  decoder: jtv.string,
}

/**
 * Used to `brand` [[ContractId]].
 */
const ContractIdBrand: unique symbol = Symbol();

/**
 * The counterpart of DAML's `ContractId T` type.
 *
 * We represent `ContractId`s as strings. Their exact format of these strings depends on the ledger
 * the DAML application is running on.
 *
 * The purpose of the intersection with `{ [ContractIdBrand]: T }` is to
 * prevent accidental use of a `ContractId<T>` when a `ContractId<U>` is
 * needed (unless `T` is a subtype of `U`). This technique is known as
 * "branding" in the TypeScript community.
 *
 * @typeparam T The contract template.
 */
export type ContractId<T> = string & { [ContractIdBrand]: T }

/**
 * Companion object of the [[ContractId]] type.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const ContractId = <T>(_t: Serializable<T>): Serializable<ContractId<T>> => ({
  decoder: jtv.string as () => jtv.Decoder<ContractId<T>>,
});

/**
 * The counterpart of DAML's `Optional T` type.
 *
 * @typeparam T The type of the optionally present value.
 */
export type Optional<T> = null | OptionalInner<T>

/**
 * Inner type of [[Optional]].
 */
type OptionalInner<T> = null extends T ? [] | [Exclude<T, null>] : T

/**
 * This class does the actual work behind the [[Optional]] companion function.  In addition to
 * implementing the [[Serializable]] interface it also stores the [[Serializable]] instance of the
 * payload of the [[Optional]] and uses it to provide a decoder for the [[OptionalInner]] type.
 *
 * @typeparam T The type of the optionally present value.
 */
class OptionalWorker<T> implements Serializable<Optional<T>> {
  constructor(private payload: Serializable<T>) { }

  decoder(): jtv.Decoder<Optional<T>> {
    return jtv.oneOf(jtv.constant(null), this.innerDecoder());
  }

  private innerDecoder(): jtv.Decoder<OptionalInner<T>> {
    if (this.payload instanceof OptionalWorker) {
      // NOTE(MH): `T` is of the form `Optional<U>` for some `U` here, that is
      // `T = Optional<U> = null | OptionalInner<U>`. Since `null` does not
      // extend `OptionalInner<V>` for any `V`, this implies
      // `OptionalInner<U> = Exclude<T, null>`. This also implies
      // `OptionalInner<T> = [] | [Exclude<T, null>]`.
      type OptionalInnerU = Exclude<T, null>
      const payloadInnerDecoder =
        this.payload.innerDecoder() as jtv.Decoder<unknown> as jtv.Decoder<OptionalInnerU>;
      return jtv.oneOf<[] | [Exclude<T, null>]>(
        jtv.constant<[]>([]),
        jtv.tuple([payloadInnerDecoder]),
      ) as jtv.Decoder<OptionalInner<T>>;
    } else {
      // NOTE(MH): `T` is not of the form `Optional<U>` here and hence `null`
      // does not extend `T`. Thus, `OptionalInner<T> = T`.
      return this.payload.decoder() as jtv.Decoder<OptionalInner<T>>;
    }
  }
}

/**
 * Companion function of the [[Optional]] type.
 */
export const Optional = <T>(t: Serializable<T>): Serializable<Optional<T>> =>
  new OptionalWorker(t);

/**
 * The counterpart of DAML's `TextMap T` type.
 *
 * We represent `TextMap`s as dictionaries.
 *
 * @typeparam T The type of the map values.
 */
export type TextMap<T> = { [key: string]: T };

/**
 * Companion object of the [[TextMap]] type.
 */
export const TextMap = <T>(t: Serializable<T>): Serializable<TextMap<T>> => ({
  decoder: (): jtv.Decoder<TextMap<T>> => jtv.dict(t.decoder()),
});

// TODO(MH): `Map` type.
