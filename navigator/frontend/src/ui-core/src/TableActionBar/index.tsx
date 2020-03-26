// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from 'react';
import Button, { Props as ButtonProps} from '../Button';
import UntypedIcon from '../Icon';
import Link, { Props as LinkProps} from '../Link';
import SearchInput, { Props as SearchInputProps} from '../SearchInput';
import styled, { hardcodedStyle } from '../theme';
import Truncate from '../Truncate';

export {makeTabLink} from './TabLink';

// ------------------------------------------------------------------------------------------------
// The action bar
// ------------------------------------------------------------------------------------------------

export const TableActionBar
  : React.ComponentClass<React.HTMLProps<HTMLDivElement>>
  = styled.div`
  height: ${hardcodedStyle.pageHeaderHeight};
  display: flex;
  border-top-left-radius: ${({ theme }) => theme.radiusBorder};
  border-top-right-radius: ${({ theme }) => theme.radiusBorder};
  color: ${({ theme }) => theme.colorPrimary[1]};
  background-color: ${({ theme }) => theme.colorPrimary[0]};
  padding: 0;
  justify-content: flex-end;
  align-items: center;
`;

// ------------------------------------------------------------------------------------------------
// Action bar generic elements
// ------------------------------------------------------------------------------------------------

/** Simple spacing element */
export const TableActionBarSpace
  : React.ComponentClass<{}>
  = styled.div`
  flex: 1;
`;

/** Simple spacing element */
export const TableActionBarSideMargin
  : React.ComponentClass<{}>
  = styled.div`
  width: ${hardcodedStyle.tableSideMargin};
`;

const StyledSearchIcon = styled(UntypedIcon).attrs({name: 'search'})`
  font-size: 1.75rem;
  color: ${({ theme }) => theme.colorSecondary[1]};
  background-color: ${({ theme }) => theme.colorSecondary[0]};
  padding-right: calc(1rem + 2px);
  padding-bottom: 0.25rem;
`;

/** A generic action bar search input */
export const RawTableActionBarSearchInput
  : React.ComponentClass<SearchInputProps & {width?: string}>
  = styled<SearchInputProps & {width?: string}>(SearchInput)`
  border-width: 0;
  height: 100%;
  flex: 1;
  color: ${({ theme }) => theme.colorSecondary[1]};
  background-color: ${({ theme }) => theme.colorSecondary[0]};
  border-radius: 0;
  border-top-right-radius: ${({ theme }) => theme.radiusBorder};

  &:focus {
    border-width: 0;
  }

  &::placeholder {
    border-width: 0;
    color: ${({ theme }) => theme.colorSecondary[1]};
  }
`;

export const UnstyledTableActionBarSearchInput =
  (props: SearchInputProps & {width?: string; className?: string}) => {
    const {className, ...rest} = props;
    return (
      <div className={className}>
          <RawTableActionBarSearchInput {...rest} />
          <StyledSearchIcon name="search" />
      </div>
    );
};

/** A generic action bar search input */
export const TableActionBarSearchInput
  : React.ComponentClass<SearchInputProps & {width?: string}>
  = styled<SearchInputProps & {width?: string}>(UnstyledTableActionBarSearchInput)`
  width: ${(props) => props.width || '30%'};
  height: 100%;
  display: flex;
  align-items: center;
  background-color: ${({ theme }) => theme.colorSecondary[0]};
`;

/** A generic action bar title. */
export const TableActionBarTitle
  : React.ComponentClass<React.HTMLProps<HTMLSpanElement>>
  = styled(Truncate)`
  flex: 1;
  align-self: center;
  font-size: 1.25rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  margin-right: ${hardcodedStyle.actionBarElementMargin};
`;

/** A generic action bar button */
export const TableActionBarButton
  : React.ComponentClass<ButtonProps>
  = styled(Button).attrs({type: 'inverted-primary'})`
  margin-right: ${hardcodedStyle.actionBarElementMargin};
  font-size: 1.0rem;
  &:hover {
    padding-top: 0.6rem;
    padding-bottom: 0.6rem;
  }
`;

/** A generic action bar link */
export const TableActionBarLink
  : React.ComponentClass<LinkProps>
  = styled(Link)`
  margin-right: ${hardcodedStyle.actionBarElementMargin};
`;

/** A generic action bar checkbox */
export const TableActionBarCheckboxLabel
  : React.ComponentClass<React.HTMLProps<HTMLLabelElement>>
  = styled.label`
  align-self: center;
  cursor: pointer;
  white-space: nowrap;
  user-select: none;
  margin-right: ${hardcodedStyle.actionBarElementMargin};
`;

/** A generic action bar checkbox */
export const TableActionBarCheckbox
  : React.ComponentClass<React.HTMLProps<HTMLInputElement>>
  = styled.input`
  margin-right: 5px;
`;

// ------------------------------------------------------------------------------------------------
// Action bar elements with TableConfig change handlers
// ------------------------------------------------------------------------------------------------

/** A search input, wired up to control a TableConfig */
export function TableActionBarConfigSearchInput(props: {
  readonly config: {readonly search: string},
  onConfigChange?(config: {readonly search: string}): void,
  readonly placeholder: string;
  readonly width?: string;
}){
  return (
    <TableActionBarSearchInput
      onChange={(value) => {
        if (props.onConfigChange) {
          props.onConfigChange({
            ...props.config,
            search: value,
          });
        }
      }}
      placeholder={''}
      initialValue={props.config.search}
      width={props.width}
    />
  );
}

/** A checkbox, wired up to control a TableConfig */
export function TableActionBarConfigCheckbox<
  Config extends {}
>(props: {
  readonly config: Config,
  onConfigChange?(config: Config): void,
  readonly configKey: keyof Config;
  readonly title: string;
}){
  return (
    <TableActionBarCheckboxLabel>
      <TableActionBarCheckbox
        type="checkbox"
        checked={!!props.config[props.configKey]}
        onChange={(e: React.FormEvent<HTMLInputElement>) => {
          if (props.onConfigChange) {
            const el = e.target as HTMLInputElement;
            props.onConfigChange({
              //tslint:disable-next-line:no-any (becuase of TS bug)
              ...props.config as any,
              [props.configKey]: el.checked,
            });
          }
        }}
      />
      {props.title}
    </TableActionBarCheckboxLabel>
  );
}
