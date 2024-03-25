// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChoicesButton } from "@da/ui-core";
import * as LedgerWatcher from "@da/ui-core/lib/ledger-watcher";
import * as Session from "@da/ui-core/lib/session";
import { CellRenderParams, ColumnConfig } from "@da/ui-core/lib/Table";
import * as React from "react";
import { connect, ConnectedComponent } from "react-redux";
import { ThunkDispatch } from "redux-thunk";
import Link from "../../components/Link";
import { ConfigInterface, ConfigType } from "../../config";
import * as Routes from "../../routes";
import * as App from "../app";
import * as Contracts from "../contracts";
import * as TemplateContracts from "../templatecontracts";
import * as Templates from "../templates";

/*
 * Note:
 * - Usually, the route defines the initial state of an applet.
 * - However, for dynamically loaded custom views, we also need to have the state,
 *   as the initial state of the custom view depends on the current config, and
 *   the current config depends on the current user and the loaded config source.
 * - Therefore, this applet initializes with an empty state, and sends an action
 *   to do the actual state initialization the first time the component is rendered.
 */

export type Action =
  | { type: "SET_UNKNOWN_ID" }
  | { type: "SET_CONTRACTS"; state: Contracts.State }
  | { type: "SET_TEMPLATES"; state: Templates.State }
  | { type: "SET_TEMPLATE_CONTRACTS"; state: TemplateContracts.State }
  | { type: "TO_CONTRACTS"; action: Contracts.Action }
  | { type: "TO_TEMPLATES"; action: Templates.Action }
  | { type: "TO_TEMPLATE_CONTRACTS"; action: TemplateContracts.Action };

export const setUnknownId = (): Action => ({ type: "SET_UNKNOWN_ID" });
export const setContracts = (state: Contracts.State): Action => ({
  type: "SET_CONTRACTS",
  state,
});
export const setTemplates = (state: Templates.State): Action => ({
  type: "SET_TEMPLATES",
  state,
});
export const setTemplateContracts = (
  state: TemplateContracts.State,
): Action => ({ type: "SET_TEMPLATE_CONTRACTS", state });

export const toContracts = (action: Contracts.Action): Action => ({
  type: "TO_CONTRACTS",
  action,
});
export const toTemplates = (action: Templates.Action): Action => ({
  type: "TO_TEMPLATES",
  action,
});
export const toTemplateContracts = (
  action: TemplateContracts.Action,
): Action => ({ type: "TO_TEMPLATE_CONTRACTS", action });

export interface StateLoading {
  type: "loading";
}

export interface StateUnknownId {
  type: "unknown-id";
}

export interface StateContracts {
  type: "contracts";
  state: Contracts.State;
}

export interface StateTemplates {
  type: "templates";
  state: Templates.State;
}

export interface StateTemplateContracts {
  type: "template-contracts";
  state: TemplateContracts.State;
}

export interface State {
  id: string;
  state:
    | StateLoading
    | StateUnknownId
    | StateContracts
    | StateTemplates
    | StateTemplateContracts;
}

export const init = (id: string): State => ({
  id,
  state: { type: "loading" },
});

export const reduce = (state: State, action: Action): State => {
  switch (action.type) {
    case "SET_UNKNOWN_ID":
      return {
        id: state.id,
        state: { type: "unknown-id" },
      };
    case "SET_CONTRACTS":
      return {
        id: state.id,
        state: { type: "contracts", state: action.state },
      };
    case "SET_TEMPLATES":
      return {
        id: state.id,
        state: { type: "templates", state: action.state },
      };
    case "SET_TEMPLATE_CONTRACTS":
      return {
        id: state.id,
        state: { type: "template-contracts", state: action.state },
      };
    case "TO_CONTRACTS":
      return state.state.type === "contracts"
        ? {
            id: state.id,
            state: {
              type: "contracts",
              state: Contracts.reduce(state.state.state, action.action),
            },
          }
        : state;
    case "TO_TEMPLATES":
      return state.state.type === "templates"
        ? {
            id: state.id,
            state: {
              type: "templates",
              state: Templates.reduce(state.state.state, action.action),
            },
          }
        : state;
    case "TO_TEMPLATE_CONTRACTS":
      return state.state.type === "template-contracts"
        ? {
            id: state.id,
            state: {
              type: "template-contracts",
              state: TemplateContracts.reduce(state.state.state, action.action),
            },
          }
        : state;
  }
};

interface OwnProps {
  state: State;
  config: ConfigType;
  user: Session.User;
  toSelf(action: Action): App.Action;
  toWatcher(action: LedgerWatcher.Action): App.Action;
}

interface DispatchProps {
  dispatch: ThunkDispatch<App.State, undefined, App.Action>;
}

type Props = DispatchProps & OwnProps;

function initContracts(d: ConfigInterface.TableViewSourceContracts) {
  const defaultState = Contracts.init();
  return {
    ...defaultState,
    filter: d.filter || defaultState.filter,
    includeArchived: d.includeArchived || defaultState.includeArchived,
    count: d.count || defaultState.count,
    sort: d.sort || defaultState.sort,
  };
}

function getInitialViewState(view: ConfigInterface.CustomView) {
  const source = view.source;
  switch (source.type) {
    case "contracts":
      return setContracts(initContracts(source));
    case "templates":
      return setTemplates(Templates.init());
    case "template-contracts":
      return setTemplateContracts(TemplateContracts.init(source.template));
  }
}

function findCustomView(
  config: ConfigType,
  id: string,
): ConfigInterface.CustomView | undefined {
  return config.customViews[id];
}

function createColumns(
  config: ConfigType,
  viewId: string,
): // eslint-disable-next-line @typescript-eslint/no-explicit-any
ColumnConfig<any, any>[] {
  const view = findCustomView(config, viewId);
  return view
    ? view.columns.map((col, index) => ({
        key: col.key || "",
        title: col.title || "",
        sortable: col.sortable || true,
        width: col.width || 50,
        weight: col.weight || 1,
        alignment: col.alignment || "left",
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        extractCellData: (rowData: any) => rowData,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        createCell: (props: CellRenderParams<any, any>) => {
          try {
            if (typeof col.createCell !== "function") {
              throw new Error(
                `Function 'createCell' not defined for column ${index}.`,
              );
            }
            const cell = col.createCell(props);
            switch (cell.type) {
              case "text":
                return <span>{cell.value}</span>;
              case "react":
                return cell.value;
              case "choices-button":
                return (
                  <ChoicesButton
                    contract={props.rowData}
                    renderLink={(id, name, inheritedInterface) => (
                      <Link
                        route={Routes.contract}
                        params={{
                          id: encodeURIComponent(id),
                          choice: name,
                          ifc:
                            inheritedInterface &&
                            encodeURIComponent(inheritedInterface),
                        }}>
                        <div>{name}</div>
                      </Link>
                    )}
                  />
                );
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              default:
                throw new Error(`Unknown cell type ${(cell as any).type}`);
            }
          } catch (e) {
            console.log(e);
            return <i>Error, see console</i>;
          }
        },
      }))
    : [];
}

interface ComponentState {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  columns: ColumnConfig<any, any>[];
}

class Component extends React.Component<Props, ComponentState> {
  constructor(props: Props) {
    super(props);

    this.state = {
      columns: createColumns(props.config, props.state.id),
    };
  }

  setInitialViewState(props: Props) {
    const { dispatch, config, state, toSelf } = props;
    const view = findCustomView(config, state.id);
    if (view) {
      dispatch(toSelf(getInitialViewState(view)));
    } else {
      dispatch(toSelf(setUnknownId()));
    }
  }

  UNSAFE_componentWillReceiveProps(nextProps: Props) {
    if (
      this.props.state.id !== nextProps.state.id ||
      this.props.config !== nextProps.config
    ) {
      this.setState({
        columns: createColumns(nextProps.config, nextProps.state.id),
      });
    }
  }

  componentDidMount() {
    this.setInitialViewState(this.props);
  }

  componentDidUpdate(prevProps: Props) {
    if (
      this.props.state.state.type === "loading" ||
      prevProps.state.id !== this.props.state.id ||
      prevProps.config !== this.props.config
    ) {
      // Only set the initial view state if we are switching to a different
      // custom view. Once a view is loaded, only update its state though the reducer.
      this.setInitialViewState(this.props);
    }
  }

  render() {
    const {
      toSelf,
      user,
      state: { id, state },
    } = this.props;

    switch (state.type) {
      case "loading":
        return <p>Loading...</p>;
      case "unknown-id":
        return <p>Unknown custom view with id &apos;{id}&apos;</p>;
      case "contracts":
        return (
          <Contracts.UI
            toSelf={(action: Contracts.Action) => toSelf(toContracts(action))}
            state={state.state}
            user={user}
            columns={this.state.columns}
          />
        );
      case "templates":
        return (
          <Templates.UI
            toSelf={(action: Templates.Action) => toSelf(toTemplates(action))}
            state={state.state}
            user={user}
            columns={this.state.columns}
          />
        );
      case "template-contracts":
        return (
          <TemplateContracts.UI
            toSelf={(action: TemplateContracts.Action) =>
              toSelf(toTemplateContracts(action))
            }
            state={state.state}
            user={user}
            columns={this.state.columns}
          />
        );
    }
  }
}

export const UI: ConnectedComponent<typeof Component, OwnProps> =
  connect()(Component);
