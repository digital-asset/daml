import React from 'react';
import { Container, Grid, Header, Icon, Segment, Divider } from 'semantic-ui-react';
import { Party } from '@daml/types';
import { useParty, useReload, useExerciseByKey, useFetchByKey, useQuery } from '@daml/react';
import UserList from './UserList';
import PartyListEdit from './PartyListEdit';
import { User } from '@daml2ts/create-daml-app/lib/create-daml-app-0.1.0/User';

const MainView: React.FC = () => {
// -- HOOKS_BEGIN
  const username = useParty();
  const myUserResult = useFetchByKey<User, Party>(User, () => username, [username]);
  const myUser = myUserResult.contract?.payload;
  const allUsersResult = useQuery<User, Party>(User);
  const allUsers = allUsersResult.contracts.map((user) => user.payload);
// -- HOOKS_END
  const reload = useReload();

  const [exerciseAddFriend] = useExerciseByKey(User.AddFriend);
  const [exerciseRemoveFriend] = useExerciseByKey(User.RemoveFriend);

  const addFriend = async (friend: Party): Promise<boolean> => {
    try {
      await exerciseAddFriend(username, {friend});
      return true;
    } catch (error) {
      alert("Unknown error:\n" + JSON.stringify(error));
      return false;
    }
  }

  const removeFriend = async (friend: Party): Promise<void> => {
    try {
      await exerciseRemoveFriend(username, {friend});
    } catch (error) {
      alert("Unknown error:\n" + JSON.stringify(error));
    }
  }

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
                onRemoveParty={removeFriend}
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
                users={allUsers.sort((user1, user2) => user1.username.localeCompare(user2.username))}
                onAddFriend={addFriend}
              />
            </Segment>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    </Container>
  );
}

export default MainView;
