// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from 'react';
import * as ReactDOM from 'react-dom';
import { Overlay } from 'react-overlays';
import styled from '../theme';
import Tooltip from '../Tooltip';

const PositionContainer = styled.div`
  position: absolute;
  z-index: 999;
`
const Wrapper = styled.div`
  display: inline;
`

export type InteractionType
  = 'target'
  | 'content'
  | 'outside';

export type Placement
  = 'top'
  | 'top-start'
  | 'top-end'
  | 'bottom'
  | 'bottom-start'
  | 'bottom-end'
  | 'left'
  | 'left-start'
  | 'left-end'
  | 'right'
  | 'right-start'
  | 'right-end';

export const placementStrings: Placement[] = [
  'top',
  'top-start',
  'top-end',
  'bottom',
  'bottom-start',
  'bottom-end',
  'left',
  'left-start',
  'left-end',
  'right',
  'right-start',
  'right-end',
];

export type PopoverPosition =
  Placement
  | 'inline';

// tslint:disable-next-line no-any
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
  private target: React.ReactInstance | null = null;
  private targetNode: Node | null = null;
  private contentNode: Node | null = null;
  private onClickDocument: (e: MouseEvent) => void
    = (e) => this.handleDocumentClick(e);

  constructor() {
    super();
  }

  componentDidMount() {
    document.addEventListener('click', this.onClickDocument, true);
  }

  componentWillUnmount() {
    document.removeEventListener('click', this.onClickDocument, true);
  }

  handleDocumentClick(e: MouseEvent) {
    if (
      (!this.targetNode || !this.targetNode.contains(e.target as Node)) &&
      (!this.contentNode || !this.contentNode.contains(e.target as Node)) &&
      this.props.isOpen
    ) {
      this.onInteraction('outside');
    }
  }

  cloneTarget(target: JSX.Element | undefined) {
    if (target === undefined) {
      return null;
    }
    else {
      return React.cloneElement(target, {
        ref: (c: React.ReactInstance ) => {
          this.target = c;
          this.targetNode = c ? ReactDOM.findDOMNode(c) : null;
          if (target.props.ref) {
            target.props.ref(c);
          }
        },
        onClick: (e: React.MouseEvent<HTMLElement>) => {
          if (target.props.onClick) {
            target.props.onClick(e);
          }
          if (this.props.onInteraction) {
            this.onInteraction('target');
          }
        },
      });
    }
  }

  getNextState(type: InteractionType, isOpen: boolean): boolean {
    if (type === 'target' && !isOpen) {
      // Opening via target
      return true;
    } else if (type === 'target' && isOpen) {
      // Closing via target
      return false;
    } else if (type === 'outside' && isOpen) {
      // Closing via outside click
      return false;
    } else {
      return isOpen;
    }
  }

  onInteraction(type: InteractionType) {
    if (this.props.onInteraction) {
      const isOpen = this.props.isOpen;
      this.props.onInteraction(type, this.getNextState(type, isOpen));
    }
  }

  render() {
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
        {position === 'inline' ? (
          isOpen ? (
            <PositionContainer>
              <Tooltip
                placement={'bottom'}
                arrow={arrow}
                margin={margin}
                ref={(c) => {
                  this.contentNode = c ? ReactDOM.findDOMNode(c) : null;
                }}
                onClick={() => this.onInteraction('content')}
              >
                {content}
              </Tooltip>
            </PositionContainer>
          ) : null
        ) : (
          <Overlay
            show={isOpen}
            //tslint:disable-next-line:no-any -- library typings are not up to date
            placement={position as any}
            container={document.body}
            target={() => this.target ? ReactDOM.findDOMNode(this.target) : null}
            shouldUpdatePosition={true}
          >
            <PositionContainer>
              <Tooltip
                //tslint:disable-next-line:no-any -- library typings are not up to date
                placement={position as any}
                arrow={arrow}
                margin={margin}
                ref={(c) => {
                  this.contentNode = c ? ReactDOM.findDOMNode(c) : null;
                }}
                onClick={() => this.onInteraction('content')}
              >
                {content}
              </Tooltip>
            </PositionContainer>
          </Overlay>
        )}
      </Wrapper>
    );
  }
}
