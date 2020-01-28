// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import * as jtv from '@mojotech/json-type-validation';

/**
 * Interface for companion objects of serializable types. Its main purpose is
 * to describe the JSON encoding of values of the serializable type.
 */
export interface Serializable<T> {
  // NOTE(MH): This must be a function to allow for mutually recursive decoders.
  decoder: () => jtv.Decoder<T>;
}

/**
 * This is a check to ensure that enum's are serializable. If the enum is named 'Color', the check
 * is done by adding a line 'STATIC_IMPLEMENTS_SERIALIZABLE_CHECK<Color>(Color)' after the
 * definition of 'Color'.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-function, @typescript-eslint/no-unused-vars
export const STATIC_IMPLEMENTS_SERIALIZABLE_CHECK = <T>(_: Serializable<T>) => {}

/**
 * Interface for objects representing DAML templates. It is similar to the
 * `Template` type class in DAML.
 */
export interface Template<T extends object, K = unknown, I extends string = string> extends Serializable<T> {
  templateId: I;
  keyDecoder: () => jtv.Decoder<K>;
  Archive: Choice<T, {}, {}>;
}

/**
 * Interface for objects representing DAML choices. It is similar to the
 * `Choice` type class in DAML.
 */
export interface Choice<T extends object, C, R, K = unknown> {
  template: () => Template<T, K>;
  argumentDecoder: () => jtv.Decoder<C>;
  resultDecoder: () => jtv.Decoder<R>;
  choiceName: string;
}

const registeredTemplates: {[key: string]: Template<object>} = {};

export const registerTemplate = <T extends object>(template: Template<T>) => {
  const templateId = template.templateId;
  const oldTemplate = registeredTemplates[templateId];
  if (oldTemplate === undefined) {
    registeredTemplates[templateId] = template;
    console.debug(`Registered template ${templateId}.`);
  } else {
    console.warn(`Trying to re-register template ${templateId}.`);
  }
}

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
 * Companion obect of the `Unit` type.
 */
export const Unit: Serializable<Unit> = {
  decoder: () => jtv.object({}),
}

/**
 * The counterpart of DAML's `Bool` type.
 */
export type Bool = boolean;

/**
 * Companion object of the `Bool` type.
 */
export const Bool: Serializable<Bool> = {
  decoder: jtv.boolean,
}

/**
 * The counterpart of DAML's `Int` type. We represent `Int`s as string in order
 * to avoid a loss of precision.
 */
export type Int = string;

/**
 * Companion object of the `Int` type.
 */
export const Int: Serializable<Int> = {
  decoder: jtv.string,
}

/**
 * The counterpart of DAML's `Numeric` type. We represent `Numeric`s as string
 * in order to avoid a loss of precision. The string must match the regular
 * expression `-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?`.
 */
export type Numeric = string;
export type Decimal = Numeric;

/**
 * Companion function of the `Numeric` type.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const Numeric = (_: number): Serializable<Numeric> =>
  ({
    decoder: jtv.string,
  })

export const Decimal: Serializable<Decimal> = Numeric(10)

/**
 * The counterpart of DAML's `Text` type.
 */
export type Text = string;

/**
 * Companion object of the `Text` type.
 */
export const Text: Serializable<Text> = {
  decoder: jtv.string,
}

/**
 * The counterpart of DAML's `Time` type. We represent `Times`s as strings with
 * format `YYYY-MM-DDThh:mm:ss[.ssssss]Z`.
 */
export type Time = string;

/**
 * Companion object of the `Time` type.
 */
export const Time: Serializable<Time> = {
  decoder: jtv.string,
}

/**
 * The counterpart of DAML's `Party` type. We represent `Party`s as strings
 * matching the regular expression `[A-Za-z0-9:_\- ]+`.
 */
export type Party = string;

/**
 * Companion object of the `Party` type.
 */
export const Party: Serializable<Party> = {
  decoder: jtv.string,
}

/**
 * The counterpart of DAML's `[T]` list type. We represent lists using arrays.
 */
export type List<T> = T[];

/**
 * Companion object of the `List` type.
 */
export const List = <T>(t: Serializable<T>): Serializable<T[]> => ({
  decoder: () => jtv.array(t.decoder()),
});

/**
 * The counterpart of DAML's `Date` type. We represent `Date`s as strings with
 * format `YYYY-MM-DD`.
 */
export type Date = string;

/**
 * Companion object of the `Date` type.
 */
export const Date: Serializable<Date> = {
  decoder: jtv.string,
}

/**
 * The counterpart of DAML's `ContractId T` type. We represent `ContractId`s
 * as strings. Their exact format of these strings depends on the ledger the
 * DAML application is running on.
 */
export type ContractId<T> = string;

/**
 * Companion object of the `ContractId` type.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const ContractId = <T>(_t: Serializable<T>): Serializable<ContractId<T>> => ({
  decoder: jtv.string,
});

/**
 * The counterpart of DAML's `Optional T` type.
 */
export type Optional<T> = null | OptionalInner<T>

type OptionalInner<T> = null extends T ? [] | [Exclude<T, null>] : T

/**
 * This class does the actual work behind the `Optional` companion function.
 * In addition to implementing the `Serializable` interface it also stores
 * the `Serializable` instance of the payload of the `Optional` and uses it to
 * provide a decoder for the `OptionalInner` type.
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
 * Companion function of the `Optional` type.
 */
export const Optional = <T>(t: Serializable<T>): Serializable<Optional<T>> =>
  new OptionalWorker(t);

/**
 * The counterpart of DAML's `TextMap T` type. We represent `TextMap`s as
 * dictionaries.
 */
export type TextMap<T> = { [key: string]: T };

/**
 * Companion object of the `TextMap` type.
 */
export const TextMap = <T>(t: Serializable<T>): Serializable<TextMap<T>> => ({
  decoder: () => jtv.dict(t.decoder()),
});

// TODO(MH): `Map` type.
