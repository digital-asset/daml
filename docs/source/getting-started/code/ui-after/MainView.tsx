// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React, { useMemo } from 'react';
import { Container, Grid, Header, Icon, Segment, Divider } from 'semantic-ui-react';
import { Party } from '@daml/types';
import { User } from '@daml2ts/create-daml-app/lib/create-daml-app-0.1.0/User';
import { useParty, useReload, useExerciseByKey, useFetchByKey, useQuery } from '@daml/react';
import UserList from './UserList';
import PartyListEdit from './PartyListEdit';
// IMPORTS_BEGIN
import MessageEdit from './MessageEdit';
import MessageList from './MessageList';
// IMPORTS_END

const MainView: React.FC = () => {
  const username = useParty();
  const myUserResult = useFetchByKey(User, () => username, [username]);
  const myUser = myUserResult.contract?.payload;
  const allUsers = useQuery(User).contracts;

  // Sorted list of friends of the current user
  const friends = useMemo(() =>
    allUsers
    .map(user => user.payload)
    .filter(user => user.username !== username)
    .sort((x, y) => x.username.localeCompare(y.username)),
    [allUsers, username]);

  const [exerciseAddFriend] = useExerciseByKey(User.AddFriend);

  const addFriend = async (friend: Party): Promise<boolean> => {
    try {
      await exerciseAddFriend(username, {friend});
      return true;
    } catch (error) {
      alert("Unknown error:\n" + JSON.stringify(error));
      return false;
    }
  }

  const messageFriend = (friend: Party) =>
    alert('Messaging parties is not yet implemented.');

  const reload = useReload();
  React.useEffect(() => {
    const interval = setInterval(reload, 5000);
    return () => clearInterval(interval);
  }, [reload]);

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
                  <Header.Subheader>Me and my friends</Header.Subheader>
                </Header.Content>
              </Header>
              <Divider />
              <PartyListEdit
                parties={myUser?.friends ?? []}
                onAddParty={addFriend}
                onMessageParty={messageFriend}
              />
            </Segment>
            <Segment>
              <Header as='h2'>
                <Icon name='globe' />
                <Header.Content>
                  The Network
                  <Icon
                    link
                    name='sync alternate'
                    size='small'
                    style={{marginLeft: '0.5em'}}
                    onClick={reload}
                  />
                  <Header.Subheader>Others and their friends</Header.Subheader>
                </Header.Content>
              </Header>
              <Divider />
              <UserList
                users={friends}
                onAddFriend={addFriend}
              />
            </Segment>
// MESSAGES_SEGMENT_BEGIN
            <Segment>
              <Header as='h2'>
                <Icon name='pencil square' />
                <Header.Content>
                  Messages
                  <Header.Subheader>Send a message to a friend</Header.Subheader>
                </Header.Content>
              </Header>
              <MessageEdit
                friends={friends.map(user => user.username)}
              />
            <Divider />
            <MessageList />
            </Segment>
// MESSAGES_SEGMENT_END
          </Grid.Column>
        </Grid.Row>
      </Grid>
    </Container>
  );
}

export default MainView;
