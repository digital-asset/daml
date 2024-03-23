// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { combineRoutes, Route } from "@da/ui-core";
import * as About from "../applets/about";
import { Action, State, toPage } from "../applets/app";
import * as Contract from "../applets/contract";
import * as Contracts from "../applets/contracts";
import * as CustomView from "../applets/customview";
import * as Info from "../applets/info";
import * as Page from "../applets/page";
import * as Template from "../applets/template";
import * as TemplateContracts from "../applets/templatecontracts";
import * as Templates from "../applets/templates";

/**
 * This module defines and exports all routes for the application and three
 * functions to make use of these routes:
 *
 * pathToAction(path: Path): Action // Generate action to navigate to path
 * stateToPath(state: AppState): Path // Path to show for given state
 * activeRoute(): Route // Returns the currently active route
 */

const set = (a: Page.State) => toPage(Page.setPage(a));

export const signIn = new Route<{}, Action, State>(
  "/sign-in(/)",
  () => undefined,
  ({ session }: State) => (session.type === "required" ? {} : undefined),
);

export const about = new Route<{}, Action, State>(
  "/about(/)",
  () => set({ type: "about", state: About.init() }),
  ({ page }: State) => (page.type === "about" ? {} : undefined),
);

export const info = new Route<{}, Action, State>(
  "/info(/)",
  () => set({ type: "info", state: Info.init() }),
  ({ page }: State) => (page.type === "info" ? {} : undefined),
);

export const contracts = new Route<{}, Action, State>(
  "/contracts(/)",
  () => set({ type: "contracts", state: Contracts.init() }),
  ({ page }: State) => (page.type === "contracts" ? {} : undefined),
);

type ContractParams = { id: string; choice?: string; ifc?: string };

export const contract = new Route<ContractParams, Action, State>(
  "/contracts/:id(/)(:choice)(/)(:ifc)",
  ({ id, choice, ifc }) =>
    set({
      type: "contract",
      state: Contract.init(
        decodeURIComponent(id),
        choice,
        ifc && decodeURIComponent(ifc),
      ),
    }),
  ({ page }: State) => {
    if (page.type === "contract") {
      const { id, choice } = page.state;
      return { id: encodeURIComponent(id), choice };
    } else {
      return undefined;
    }
  },
);

export const templates = new Route<{}, Action, State>(
  "/templates(/)",
  () => set({ type: "templates", state: Templates.init() }),
  ({ page }: State) => (page.type === "templates" ? {} : undefined),
);

export const template = new Route<{ id: string }, Action, State>(
  "/templates/:id(/)",
  ({ id }) => set({ type: "template", state: Template.init(id) }),
  ({ page }: State) =>
    page.type === "template" ? { id: page.state.id } : undefined,
);

export const templateContracts = new Route<{ id: string }, Action, State>(
  "/templates/:id/contracts(/)",
  ({ id }) =>
    set({ type: "templatecontracts", state: TemplateContracts.init(id) }),
  ({ page }: State) =>
    page.type === "templatecontracts" ? { id: page.state.id } : undefined,
);

export const customView = new Route<{ id: string }, Action, State>(
  "/custom/:id(/)",
  ({ id }) => set({ type: "customview", state: CustomView.init(id) }),
  ({ page }: State) =>
    page.type === "customview" ? { id: page.state.id } : undefined,
);

export const config = new Route<{}, Action, State>(
  "/config(/)",
  () => set({ type: "configsource" }),
  ({ page }: State) => (page.type === "configsource" ? {} : undefined),
);

const anything = new Route<{}, Action, State>(
  "/*anything",
  () => set({ type: "contracts", state: Contracts.init() }),
  (_: State) => ({}),
);

// Order matters -- first match is returned.
export const { stateToPath, pathToAction, activeRoute } = combineRoutes([
  signIn,
  about,
  info,
  config,
  contracts,
  contract,
  templateContracts,
  template,
  templates,
  customView,
  anything,
]);
