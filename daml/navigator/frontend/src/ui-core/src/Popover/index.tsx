// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Overlay } from "react-overlays";
import styled from "../theme";
import Tooltip from "../Tooltip";

const PositionContainer = styled.div`
  position: absolute;
  z-index: 999;
`;
const Wrapper = styled.div`
  display: inline;
`;

export type InteractionType = "target" | "content" | "outside";

export type Placement =
  | "top"
  | "top-start"
  | "top-end"
  | "bottom"
  | "bottom-start"
  | "bottom-end"
  | "left"
  | "left-start"
  | "left-end"
  | "right"
  | "right-start"
  | "right-end";

export const placementStrings: Placement[] = [
  "top",
  "top-start",
  "top-end",
  "bottom",
  "bottom-start",
  "bottom-end",
  "left",
  "left-start",
  "left-end",
  "right",
  "right-start",
  "right-end",
];

export type PopoverPosition = Placement | "inline";

// eslint-disable-next-line  @typescript-eslint/no-explicit-any
export type AnyReactElement = React.ReactElement<any>;

export interface Props {
  /** Content displayed in the Popover */
  content: JSX.Element;

  /** Target element (anchor for popover position). */
  target: JSX.Element;

  /** Whether to display an arrow */
  arrow?: boolean;

  /** Margin between popover and target */
  margin?: number;

  /** Whether the popover is open (visible) */
  isOpen: boolean;

  /** Popover position, relative to target */
  position: PopoverPosition;

  onInteraction?(type: InteractionType, isOpen: boolean): void;
}

const defaultMargin = 6;

export default class Popover extends React.Component<Props, {}> {
  private target: React.RefObject<HTMLElement> = React.createRef();
  private contentNode: React.RefObject<HTMLDivElement> = React.createRef();
  private onClickDocument: (e: MouseEvent) => void = e =>
    this.handleDocumentClick(e);

  constructor(props: Props) {
    super(props);
  }

  componentDidMount(): void {
    document.addEventListener("click", this.onClickDocument, true);
  }

  componentWillUnmount(): void {
    document.removeEventListener("click", this.onClickDocument, true);
  }

  handleDocumentClick(e: MouseEvent): void {
    if (
      (!this.target.current ||
        !this.target.current.contains(e.target as Node)) &&
      (!this.contentNode.current ||
        !this.contentNode.current.contains(e.target as Node)) &&
      this.props.isOpen
    ) {
      this.onInteraction("outside");
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  cloneTarget(
    target: JSX.Element | undefined,
  ): React.FunctionComponentElement<any> | null {
    if (target === undefined) {
      return null;
    } else {
      return React.cloneElement(target, {
        ref: this.target,
        onClick: (e: React.MouseEvent<HTMLElement>) => {
          if (target.props.onClick) {
            target.props.onClick(e);
          }
          if (this.props.onInteraction) {
            this.onInteraction("target");
          }
        },
      });
    }
  }

  getNextState(type: InteractionType, isOpen: boolean): boolean {
    if (type === "target" && !isOpen) {
      // Opening via target
      return true;
    } else if (type === "target" && isOpen) {
      // Closing via target
      return false;
    } else if (type === "outside" && isOpen) {
      // Closing via outside click
      return false;
    } else {
      return isOpen;
    }
  }

  onInteraction(type: InteractionType): void {
    if (this.props.onInteraction) {
      const isOpen = this.props.isOpen;
      this.props.onInteraction(type, this.getNextState(type, isOpen));
    }
  }

  render(): JSX.Element {
    const {
      arrow = true,
      content,
      isOpen,
      margin = defaultMargin,
      position,
      target,
    } = this.props;

    return (
      <Wrapper>
        {this.cloneTarget(target)}
        {position === "inline" ? (
          isOpen ? (
            <PositionContainer>
              <Tooltip
                placement={"bottom"}
                arrow={arrow}
                margin={margin}
                ref={this.contentNode}
                onClick={() => this.onInteraction("content")}>
                {content}
              </Tooltip>
            </PositionContainer>
          ) : null
        ) : (
          <Overlay
            show={isOpen}
            placement={position}
            container={document.body}
            target={this.target}>
            {({ props }) => (
              <PositionContainer {...props}>
                <Tooltip
                  placement={position}
                  arrow={arrow}
                  margin={margin}
                  ref={this.contentNode}
                  onClick={() => this.onInteraction("content")}>
                  {content}
                </Tooltip>
              </PositionContainer>
            )}
          </Overlay>
        )}
      </Wrapper>
    );
  }
}
