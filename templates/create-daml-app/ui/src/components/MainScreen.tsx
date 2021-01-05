// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React from 'react'
import { Image, Menu } from 'semantic-ui-react'
import MainView from './MainView';
import { useParty } from '@daml/react';

type Props = {
  onLogout: () => void;
}

/**
 * React component for the main screen of the `App`.
 */
const MainScreen: React.FC<Props> = ({onLogout}) => {
  return (
    <>
      <Menu icon borderless>
        <Menu.Item>
          <Image
            as='a'
            href='https://www.daml.com/'
            target='_blank'
            src='/daml.svg'
            alt='DAML Logo'
            size='mini'
          />
        </Menu.Item>
        <Menu.Menu position='right' className='test-select-main-menu'>
          <Menu.Item position='right'>
            You are logged in as {useParty()}.
          </Menu.Item>
          <Menu.Item
            position='right'
            active={false}
            className='test-select-log-out'
            onClick={onLogout}
            icon='log out'
          />
        </Menu.Menu>
      </Menu>

      <MainView/>
    </>
  );
};

export default MainScreen;
