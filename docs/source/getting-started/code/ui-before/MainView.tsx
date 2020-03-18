// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React, { useMemo } from 'react';
import { Container, Grid, Header, Icon, Segment, Divider } from 'semantic-ui-react';
import { Party } from '@daml/types';
import { User } from '@daml2ts/create-daml-app/lib/create-daml-app-0.1.0/User';
import { useParty, useExerciseByKey, useStreamFetchByKey, useStreamQuery } from '@daml/react';
import UserList from './UserList';
import PartyListEdit from './PartyListEdit';

// USERS_BEGIN
const MainView: React.FC = () => {
  const username = useParty();
  const myUserResult = useStreamFetchByKey(User, () => username, [username]);
  const myUser = myUserResult.contract?.payload;
  const allUsers = useStreamQuery(User).contracts;
// USERS_END

  // Sorted list of friends of the current user
  const friends = useMemo(() =>
    allUsers
    .map(user => user.payload)
    .filter(user => user.username !== username)
    .sort((x, y) => x.username.localeCompare(y.username)),
    [allUsers, username]);

// STARTFOLLOWING_BEGIN
  const [exerciseFollow] = useExerciseByKey(User.Follow);

  const follow = async (userToFollow: Party): Promise<boolean> => {
    try {
      await exerciseFollow(username, {userToFollow});
      return true;
    } catch (error) {
      alert("Unknown error:\n" + JSON.stringify(error));
      return false;
    }
  }
// STARTFOLLOWING_END

return (
  <Container>
    <Grid centered columns={2}>
      <Grid.Row stretched>
        <Grid.Column>
          <Header as='h1' size='huge' color='blue' textAlign='center' style={{padding: '1ex 0em 0ex 0em'}}>
              {myUser ? `Welcome, ${myUser.username}!` : 'Loading...'}
          </Header>

          <Segment>
            <Header as='h2'>
              <Icon name='user' />
              <Header.Content>
                {myUser?.username ?? 'Loading...'}
                <Header.Subheader>Users I'm following</Header.Subheader>
              </Header.Content>
            </Header>
            <Divider />
            <PartyListEdit
              parties={myUser?.following ?? []}
              onAddParty={follow}
            />
          </Segment>
          <Segment>
            <Header as='h2'>
              <Icon name='globe' />
              <Header.Content>
                The Network
                <Header.Subheader>My followers and users they are following</Header.Subheader>
              </Header.Content>
            </Header>
            <Divider />
// USERLIST_BEGIN
            <UserList
              users={following}
              onFollow={follow}
            />
// USERLIST_END
          </Segment>
        </Grid.Column>
      </Grid.Row>
    </Grid>
  </Container>
);
}

export default MainView;