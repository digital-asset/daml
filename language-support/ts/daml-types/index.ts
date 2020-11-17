// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import * as jtv from '@mojotech/json-type-validation';

/**
 * Interface for companion objects of serializable types. Its main purpose is
 * to serialize and deserialize values between raw JSON and typed values.
 *
 * @typeparam T The template type.
 */
export interface Serializable<T> {
  /**
   * @internal
   */
  decoder: jtv.Decoder<T>;
  /**
   * @internal Encodes T in expected shape for JSON API.
   */
  encode: (t: T) => unknown;
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
  /**
   * @internal
   */
  keyDecoder: jtv.Decoder<K>;
  /**
   * @internal
   */
  keyEncode: (k: K) => unknown;
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
   * @internal Returns a decoder to decode the choice arguments.
   *
   * Note: we never need to decode the choice arguments, as they are sent over
   * the API but not received.
   */
  argumentDecoder: jtv.Decoder<C>;
  /**
   * @internal
   */
  argumentEncode: (c: C) => unknown;
  /**
   * @internal Returns a deocoder to decode the return value.
   */
  resultDecoder: jtv.Decoder<R>;
  // note: no encoder for result, as they cannot be sent, only received.
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
    registeredTemplates[templateId] = template as unknown as Template<object, unknown, string>;
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
 * @internal Turn a thunk into a memoized version of itself. The memoized thunk
 * invokes the original thunk only on its first invocation and caches the result
 * for later uses. We use this to implement a version of `jtv.lazy` with
 * memoization.
 */
export function memo<A>(thunk: () => A): () => A {
  let memoized: () => A = () => {
      const cache = thunk();
      memoized = (): A => cache;
      return cache;
  };
  // NOTE(MH): Since we change `memoized` when the resultung thunk is invoked
  // for the first time, we need to return it "by reference". Thus, we return
  // a closure which contains a reference to `memoized`.
  return (): A => memoized();
}

/**
 * @internal Variation of `jtv.lazy` which memoizes the computed decoder on its
 * first invocation.
 */
export function lazyMemo<A>(mkDecoder: () => jtv.Decoder<A>): jtv.Decoder<A> {
  return jtv.lazy(memo(mkDecoder));
}

/**
 * The counterpart of DAML's `()` type.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface Unit {
  // NOTE(MH): Although eslint claims that the empty interface and the type
  // `{}` are equivalent, that's not true. The former does not extend
  // `{[key: string]: string}` whereas the latter does. In other words,
  // `Unit extends {[key: string]: string} ? true : false` is `false`,
  // whereas `{} extends {[key: string]: string} ? true : false` is `true`.
  // This might become important for defining a better version of the
  // `Query<T>` type in @daml/ledger.
}

/**
 * Companion obect of the [[Unit]] type.
 */
export const Unit: Serializable<Unit> = {
  decoder: jtv.object({}),
  encode: (t: Unit) => t,
}

/**
 * The counterpart of DAML's `Bool` type.
 */
export type Bool = boolean;

/**
 * Companion object of the [[Bool]] type.
 */
export const Bool: Serializable<Bool> = {
  decoder: jtv.boolean(),
  encode: (b: Bool) => b,
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
  decoder: jtv.string(),
  encode: (i: Int) => i,
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
    decoder: jtv.string(),
    encode: (n: Numeric): unknown => n,
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
  decoder: jtv.string(),
  encode: (t: Text) => t,
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
  decoder: jtv.string(),
  encode: (t: Time) => t,
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
  decoder: jtv.string(),
  encode: (p: Party) => p,
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
  decoder: jtv.array(t.decoder),
  encode: (l: List<T>): unknown => l.map((element: T) => t.encode(element)),
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
  decoder: jtv.string(),
  encode: (d: Date) => d,
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
  decoder: jtv.string() as jtv.Decoder<ContractId<T>>,
  encode: (c: ContractId<T>): unknown => c,
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
  decoder: jtv.Decoder<Optional<T>>;
  private innerDecoder: jtv.Decoder<OptionalInner<T>>;
  encode: (o: Optional<T>) => unknown;

  constructor(payload: Serializable<T>) {
    if (payload instanceof OptionalWorker) {
      // NOTE(MH): `T` is of the form `Optional<U>` for some `U` here, that is
      // `T = Optional<U> = null | OptionalInner<U>`. Since `null` does not
      // extend `OptionalInner<V>` for any `V`, this implies
      // `OptionalInner<U> = Exclude<T, null>`. This also implies
      // `OptionalInner<T> = [] | [Exclude<T, null>]`.
      type OptionalInnerU = Exclude<T, null>
      const payloadInnerDecoder =
        payload.innerDecoder as jtv.Decoder<unknown> as jtv.Decoder<OptionalInnerU>;
      this.innerDecoder = jtv.oneOf<[] | [Exclude<T, null>]>(
        jtv.constant<[]>([]),
        jtv.tuple([payloadInnerDecoder]),
      ) as jtv.Decoder<OptionalInner<T>>;
      this.encode = (o: Optional<T>): unknown => {
        if (o === null) {
          // Top-level enclosing Optional where the type argument is also
          // Optional and we represent None.
          return null;
        } else {
          // The current type is Optional<Optional<...>> and the current value
          // is Some x. Therefore the nested value is represented as [] for
          // x = None or as [y] for x = Some y. In both cases mapping the
          // encoder of the type parameter does the right thing.
          return (o as unknown as T[]).map(nested => payload.encode(nested));
        }
      }
    } else {
      // NOTE(MH): `T` is not of the form `Optional<U>` here and hence `null`
      // does not extend `T`. Thus, `OptionalInner<T> = T`.
      this.innerDecoder = payload.decoder as jtv.Decoder<OptionalInner<T>>;
      this.encode = (o: Optional<T>): unknown => {
        if (o === null) {
          // This branch is only reached if we are at the top-level and the
          // entire type is a non-nested Optional, i.e. Optional<U> where U is
          // not Optional. Recursive calls from the other branch would stop
          // before reaching this case, as nested None are empty lists and
          // never null.
          return null;
        } else {
          return payload.encode(o as unknown as T);
        }
      }
    }
    this.decoder = jtv.oneOf(jtv.constant(null), this.innerDecoder);
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
  decoder: jtv.dict(t.decoder),
  encode: (tm: TextMap<T>): unknown => {
    const out: {[key: string]: unknown} = {};
    Object.keys(tm).forEach((k) => {
      out[k] = t.encode(tm[k]);
    });
    return out;
  }
});

// TODO(MH): `Map` type.
