// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import * as jtv from "@mojotech/json-type-validation";
import _ from "lodash";

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
 * Companion objects for templates and interfaces, containing their choices.
 *
 * @typeparam T The template payload format or interface view.
 * @typeparam I The template or interface id.
 */
export interface ContractTypeCompanion<T extends object, I extends string> {
  templateId: I;
  /**
   * @internal
   */
  sdkVersion: "0.0.0-SDKVERSION";
  /**
   * @internal
   */
  decoder: jtv.Decoder<T>;
}

/**
 * Interface for objects representing Daml templates. It is similar to the
 * `Template` type class in Daml.
 *
 * @typeparam T The template type.
 * @typeparam K The contract key type.
 * @typeparam I The template id type.
 *
 */
export interface Template<
  T extends object,
  K = unknown,
  I extends string = string,
> extends ContractTypeCompanion<T, I>,
    Serializable<T> {
  /**
   * @internal
   */
  keyDecoder: jtv.Decoder<K>;
  /**
   * @internal
   */
  keyEncode: (k: K) => unknown;
  // eslint-disable-next-line @typescript-eslint/ban-types
  Archive: Choice<T, {}, {}, K>;
}

/**
 * A mixin for [[Template]] that provides the `toInterface` and
 * `unsafeFromInterface` contract ID conversion functions.
 *
 * Even templates that directly implement no interfaces implement this, because
 * this also permits conversion with interfaces that supply retroactive
 * implementations to this template.
 *
 * @typeparam T The template type.
 * @typeparam IfU The union of implemented interfaces, or `never` for templates
 *            that directly implement no interface.
 */
export interface ToInterface<T extends object, IfU> {
  // overload for direct interface implementations
  toInterface<If extends IfU>(
    ic: FromTemplate<If, unknown>,
    cid: ContractId<T>,
  ): ContractId<If>;
  // overload for retroactive interface implementations
  toInterface<If>(ic: FromTemplate<If, T>, cid: ContractId<T>): ContractId<If>;

  // overload for direct interface implementations
  unsafeFromInterface(
    ic: FromTemplate<IfU, unknown>,
    cid: ContractId<IfU>,
  ): ContractId<T>;
  // overload for retroactive interface implementations
  unsafeFromInterface<If>(
    ic: FromTemplate<If, T>,
    cid: ContractId<If>,
  ): ContractId<T>;
}

const InterfaceBrand: unique symbol = Symbol();

/**
 * An interface type, for use with contract IDs.
 *
 * @typeparam IfId The interface ID as a constant string.
 */
export type Interface<IfId> = { readonly [InterfaceBrand]: IfId };

/**
 * Interface for objects representing Daml interfaces.
 */
export interface InterfaceCompanion<T extends object, I extends string = string>
  extends ContractTypeCompanion<T, I> {}

const FromTemplateBrand: unique symbol = Symbol();

/**
 * A mixin for [[InterfaceCompanion]].  This supplies the basis
 * for the methods of [[ToInterface]].
 *
 * Even interfaces that retroactively implement for no templates implement this,
 * because forward implementations still require this marker to work.
 *
 * @typeparam If The interface type.
 * @typeparam TX The intersection of template types this interface retroactively
 *               implements, or `unknown` if there are none.
 */
export interface FromTemplate<If, TX> {
  readonly [FromTemplateBrand]: [If, TX];
}

/**
 * Interface for objects representing Daml choices.
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
  template: () => T extends Interface<infer I>
    ? InterfaceCompanion<T, I & string>
    : Template<T, K>;
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

function toInterfaceMixin<T extends object, IfU>(): ToInterface<T, IfU> {
  return {
    toInterface<If>(_: FromTemplate<If, unknown>, cid: ContractId<T>) {
      return cid as ContractId<never> as ContractId<If>;
    },

    unsafeFromInterface<If>(_: FromTemplate<If, unknown>, cid: ContractId<If>) {
      return cid as ContractId<never> as ContractId<T>;
    },
  };
}

/**
 * @internal
 */
export function assembleTemplate<T extends object, IfU>(
  template: Template<T>,
  ...interfaces: FromTemplate<IfU, unknown>[]
): Template<T> & ToInterface<T, IfU> {
  const combined = {};
  const overloaded: string[] = [];
  for (const iface of interfaces) {
    _.mergeWith(combined, iface, (left, right, k) => {
      if (left !== undefined && right !== undefined) overloaded.push(k);
      return undefined;
    });
  }
  return Object.assign(
    _.omit(combined, overloaded),
    toInterfaceMixin<T, IfU>(),
    template,
  );
}

/**
 * @internal
 */
export function assembleInterface<
  T extends object,
  I extends string,
  C extends object,
>(
  templateId: I,
  decoderSource: Serializable<T>,
  choices: C,
): InterfaceCompanion<Interface<I> & T, I> & C {
  return {
    templateId: templateId,
    sdkVersion: "0.0.0-SDKVERSION",
    // `Interface<I> &` is a phantom intersection
    decoder: decoderSource.decoder as jtv.Decoder<Interface<I> & T>,
    ...choices,
  };
}

/**
 * @internal
 */
const registeredTemplates: { [key: string]: Template<object> } = {};

/**
 * @internal
 */
export const registerTemplate = <T extends object>(
  template: Template<T>,
): void => {
  const templateId = template.templateId;
  const oldTemplate = registeredTemplates[templateId];
  if (oldTemplate === undefined) {
    registeredTemplates[templateId] = template as unknown as Template<
      object,
      unknown,
      string
    >;
    console.debug(`Registered template ${templateId}.`);
  } else {
    console.warn(`Trying to re-register template ${templateId}.`);
  }
};

/**
 * @internal
 */
export const lookupTemplate = (templateId: string): Template<object> => {
  const template = registeredTemplates[templateId];
  if (template === undefined) {
    throw Error(
      `Failed to look up template ${templateId}. Make sure your @daml/types version agrees with the used Daml SDK version.`,
    );
  }
  return template;
};

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
 * The counterpart of Daml's `()` type.
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
 * Companion object of the [[Unit]] type.
 */
export const Unit: Serializable<Unit> = {
  decoder: jtv.object({}),
  encode: (t: Unit) => t,
};

/**
 * The counterpart of Daml's `Bool` type.
 */
export type Bool = boolean;

/**
 * Companion object of the [[Bool]] type.
 */
export const Bool: Serializable<Bool> = {
  decoder: jtv.boolean(),
  encode: (b: Bool) => b,
};

/**
 * The counterpart of Daml's `Int` type.
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
};

/**
 * The counterpart of Daml's `Numeric` type.
 *
 * We represent `Numeric`s as string in order to avoid a loss of precision. The string must match
 * the regular expression `-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?`.
 */
export type Numeric = string;

/**
 * The counterpart of Daml's `Decimal` type.
 *
 * In Daml, Decimal's are the same as Numeric with precision 10.
 *
 */
export type Decimal = Numeric;

/**
 * Companion function of the [[Numeric]] type.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const Numeric = (_: number): Serializable<Numeric> => ({
  decoder: jtv.string(),
  encode: (n: Numeric): unknown => n,
});

/**
 * Companion object of the [[Decimal]] type.
 */
export const Decimal: Serializable<Decimal> = Numeric(10);

/**
 * The counterpart of Daml's `Text` type.
 */
export type Text = string;

/**
 * Companion object of the [[Text]] type.
 */
export const Text: Serializable<Text> = {
  decoder: jtv.string(),
  encode: (t: Text) => t,
};

/**
 * The counterpart of Daml's `Time` type.
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
};

/**
 * The counterpart of Daml's `Party` type.
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
};

/**
 * The counterpart of Daml's `[T]` list type.
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
 * The counterpart of Daml's `Date` type.
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
};

/**
 * Used to `brand` [[ContractId]].
 */
const ContractIdBrand: unique symbol = Symbol();

/**
 * The counterpart of Daml's `ContractId T` type.
 *
 * We represent `ContractId`s as strings. Their exact format of these strings depends on the ledger
 * the Daml application is running on.
 *
 * The purpose of the intersection with `{ [ContractIdBrand]: T }` is to
 * prevent accidental use of a `ContractId<T>` when a `ContractId<U>` is
 * needed (unless `T` is a subtype of `U`). This technique is known as
 * "branding" in the TypeScript community.
 *
 * @typeparam T The contract template.
 */
export type ContractId<T> = string & { [ContractIdBrand]: T };

/**
 * Companion object of the [[ContractId]] type.
 */
export const ContractId = <T>(
  _t: Serializable<T>, // eslint-disable-line @typescript-eslint/no-unused-vars
): Serializable<ContractId<T>> => ({
  decoder: jtv.string() as jtv.Decoder<ContractId<T>>,
  encode: (c: ContractId<T>): unknown => c,
});

/**
 * The counterpart of Daml's `Optional T` type.
 *
 * @typeparam T The type of the optionally present value.
 */
export type Optional<T> = null | OptionalInner<T>;

/**
 * Inner type of [[Optional]].
 */
type OptionalInner<T> = null extends T ? [] | [Exclude<T, null>] : T;

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
      type OptionalInnerU = Exclude<T, null>;
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
      };
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
      };
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
 * The counterpart of Daml's `TextMap T` type.
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
    const out: { [key: string]: unknown } = {};
    Object.keys(tm).forEach(k => {
      out[k] = t.encode(tm[k]);
    });
    return out;
  },
});

/**
 * The counterpart of Daml's `DA.Map.Map K V` type.
 *
 * This is an immutable map which compares keys via deep equality. The order of
 * iteration is unspecified; the only guarantee is that the order in `keys` and
 * `values` match, i.e. `m.get(k)` is (deep-, value-based) equal to
 * `[...m.values()][[...m.keys()].findIndex((l) => _.isEqual(k, l))]`
 *
 * @typeparam K The type of the map keys.
 * @typeparam V The type of the map values.
 */
export interface Map<K, V> {
  get: (k: K) => V | undefined;
  has: (k: K) => boolean;
  set: (k: K, v: V) => Map<K, V>;
  delete: (k: K) => Map<K, V>;
  keys: () => Iterator<K, undefined, undefined>;
  values: () => Iterator<V, undefined, undefined>;
  entries: () => Iterator<[K, V], undefined, undefined>;
  entriesArray: () => [K, V][];
  forEach: <T, U>(f: (value: V, key: K, map: Map<K, V>) => T, u?: U) => void;
}

function* it<T>(arr: T[]): Iterator<T, undefined, undefined> {
  for (let i = 0; i < arr.length; i++) {
    yield _.cloneDeep(arr[i]);
  }
  return undefined;
}

// This code assumes that the decoder is only ever used in decoding values
// straight from the API responses, and said raw responses are never reused
// afterwards. This should be enforced by this class not being exported and the
// daml-ledger module not letting raw JSON responses escape without going
// through this.
//
// Without that assumption, the constructor would need to deep-copy its kvs
// argument.
class MapImpl<K, V> implements Map<K, V> {
  private _kvs: [K, V][];
  private _keys: K[];
  private _values: V[];
  constructor(kvs: [K, V][]) {
    // sorting done so that generic object deep comparison would find equal
    // maps equal (as defined by jest's expect().toEqual())
    this._kvs = _.sortBy(kvs, kv => JSON.stringify(kv[0]));
    this._keys = this._kvs.map(e => e[0]);
    this._values = this._kvs.map(e => e[1]);
  }
  private _idx(k: K): number {
    return this._keys.findIndex(l => _.isEqual(k, l));
  }
  has(k: K): boolean {
    return this._idx(k) !== -1;
  }
  get(k: K): V | undefined {
    return _.cloneDeep(this._values[this._idx(k)]);
  }
  set(k: K, v: V): Map<K, V> {
    if (this.has(k)) {
      const cpy = this._kvs.slice();
      cpy[this._idx(k)] = _.cloneDeep([k, v]);
      return new MapImpl(cpy);
    } else {
      const head: [K, V][] = _.cloneDeep([[k, v]]);
      return new MapImpl(head.concat(this._kvs));
    }
  }
  delete(k: K): Map<K, V> {
    const i = this._idx(k);
    if (i !== -1) {
      return new MapImpl(this._kvs.slice(0, i).concat(this._kvs.slice(i + 1)));
    } else {
      return this;
    }
  }
  keys(): Iterator<K, undefined, undefined> {
    return it(this._keys);
  }
  values(): Iterator<V, undefined, undefined> {
    return it(this._values);
  }
  entries(): Iterator<[K, V], undefined, undefined> {
    return it(this._kvs);
  }
  entriesArray(): [K, V][] {
    return _.cloneDeep(this._kvs);
  }
  forEach<T, U>(f: (v: V, k: K, m: Map<K, V>) => T, u?: U): void {
    const g = u ? f.bind(u) : f;
    for (const [k, v] of this._kvs) {
      g(v, k, this);
    }
  }
}

export const emptyMap = <K, V>(): Map<K, V> => new MapImpl<K, V>([]);

/**
 * Companion function of the [[GenMap]] type.
 */
export const Map = <K, V>(
  kd: Serializable<K>,
  vd: Serializable<V>,
): Serializable<Map<K, V>> => ({
  decoder: jtv
    .array(jtv.tuple([kd.decoder, vd.decoder]))
    .map(kvs => new MapImpl(kvs)),
  encode: (m: Map<K, V>): unknown =>
    m.entriesArray().map(e => [kd.encode(e[0]), vd.encode(e[1])]),
});
