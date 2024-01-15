// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as configAPISource from "!raw-loader!../config/api/v1";
import * as UICore from "@da/ui-core";
import * as Session from "@da/ui-core/lib/session";
import * as Babel from "babel-standalone";
import * as React from "react";
import * as ReactDOM from "react-dom";
import * as Link from "../components/Link";
import * as Routes from "../routes";
import { load as load_v1 } from "./api/v1-load";
import * as V2 from "./api/v2";
import { load as load_v2 } from "./api/v2-load";
import { Either, left, right } from "./either";
import { catchToError } from "./utils";

/** List of approved and bundled imports that are available to the config file */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const imports: { [name: string]: any } = {
  react: React,
  "react-DOM": ReactDOM,
  "@da/ui-core": UICore,
  "@navigator/routes": Routes,
  "@navigator/link": Link,
};

// Latest schema and version
export const schema = "navigator-config";

const latestVersion = {
  schema,
  major: 2,
  minor: 0,
};

export const configFileAPI: string = configAPISource as string;

export interface ConfigFunctions {
  theme(userId: string, party: string, role: string): V2.Theme;
  customViews(
    userId: string,
    party: string,
    role: string,
  ): { [id: string]: V2.CustomView };
}

export interface ConfigType {
  theme: V2.Theme;
  customViews: { [id: string]: V2.CustomView };
}

export { V2 as ConfigInterface };

export type LoadConfigResult = Either<Error, ConfigFunctions>;
export type EvalConfigResult = Either<Error, ConfigType>;

/** Loading a config file */
export function loadConfig(source: string): LoadConfigResult {
  try {
    // Apply JSX transform, translate import and export keywords
    const transformedSource = Babel.transform(source, {
      presets: ["es2015", "react"],
    }).code;
    if (transformedSource === undefined) {
      throw new Error(`Babel transform did not return any code.
        This should not happen, contact the app developer.`);
    }

    // Self-made import function (only expose modules already bundled with the navigator)
    const configRequire = (name: string) => {
      const importModule = imports[name];
      if (importModule !== undefined) {
        return importModule;
      } else {
        const importNames = Object.keys(imports)
          .map(x => `'${x}'`)
          .join(", ");
        throw new Error(`Unknown import '${name}'.
            You may only import the following modules: ${importNames}.`);
      }
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const configExports: any = {};

    // Evaluate transformed source
    const moduleFn = new Function("require", "exports", transformedSource);
    moduleFn(configRequire, configExports);

    // Validate version
    if (!("version" in configExports)) {
      throw new Error(`No version exported.
        Use 'export const version = ${JSON.stringify(
          latestVersion,
        )}' to specify a version.`);
    }
    const version = configExports.version;
    if (typeof version !== "object") {
      throw new Error(`Version is not an object.
        Use 'export const version = ${JSON.stringify(
          latestVersion,
        )}' to specify a version.`);
    }
    if (version.schema !== schema) {
      throw new Error(`Schema mismatch (expected '${schema}', got '${
        version.schema
      }').
        Use 'export const version = ${JSON.stringify(
          latestVersion,
        )}' to specify a version.`);
    }
    if (typeof version.major !== "number") {
      throw new Error(`Major version is missing or not a number.
        Use 'export const version = ${JSON.stringify(
          latestVersion,
        )}' to specify a version.`);
    }
    if (typeof version.minor !== "number") {
      throw new Error(`Minor version is missing or not a number.
        Use 'export const version = ${JSON.stringify(
          latestVersion,
        )}' to specify a version.`);
    }
    const major: number = version.major;
    const minor: number = version.minor;

    // Load content depending on version
    if (major === 1) {
      return right(load_v1(configExports, major, minor));
    } else if (major === 2) {
      return right(load_v2(configExports, major, minor));
    } else {
      return left(
        new Error(
          `Don't know how to load version ${major}.${minor}.
        Latest known version is ${latestVersion.major}.${latestVersion.minor}.`,
        ),
      );
    }
  } catch (error) {
    return left(catchToError(error));
  }
}

export function defaultConfig(): Either<Error, ConfigType> {
  return right({
    theme: {},
    customViews: {},
  });
}

/**
 * Evaluate a config
 */
export function evalConfig(
  user: Session.User,
  source: string,
): Either<Error, ConfigType> {
  const resultFn = loadConfig(source);
  switch (resultFn.type) {
    case "left":
      return resultFn;
    case "right":
      try {
        const { id, party, role = "" } = user;
        const result = {
          theme: resultFn.value.theme(id, party, role),
          customViews: resultFn.value.customViews(id, party, role),
        };
        return right(result);
      } catch (error) {
        return left(catchToError(error));
      }
  }
}

/**
 * Some notes:
 * - The cached result is not stored in redux state for two reasons:
 *   - The cached result contains non-serializable values (functions, to be specific).
 *   - The redux store should not contain values computable from other state.
 *     This assumes that the user config only contains referentially transparent functions.
 */
export type EvalConfigCache = { [hash: string]: EvalConfigResult };

/**
 * A cached version of evalConfig
 * Warning: Assumes the loaded config function is referentially transparent!
 */
export function evalConfigCached(
  user: Session.User,
  source: string,
  cache: EvalConfigCache,
): { cache: EvalConfigCache; result: Either<Error, ConfigType> } {
  const hash = `${user.id};${user.party};${user.role};${source}`;
  if (cache[hash]) {
    const result = cache[hash];
    return { result, cache };
  } else {
    const result = evalConfig(user, source);
    // TODO: Implement a proper LRU cache
    if (Object.keys(cache).length > 10) {
      return { result, cache: { [hash]: result } };
    }
    return { result, cache: { ...cache, [hash]: result } };
  }
}

export function prettyPrintConfig(config: ConfigType): Either<Error, string> {
  try {
    // JSON.stringify, but stringify functions
    return right(
      JSON.stringify(
        config,
        (_key, value) => {
          if (typeof value === "function") {
            return value.toString();
          } else {
            return value;
          }
        },
        "  ",
      ),
    );
  } catch (error) {
    return left(catchToError(error));
  }
}
