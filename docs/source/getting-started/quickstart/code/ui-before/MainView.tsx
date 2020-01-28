import React from 'react';
import { Container, Grid, Header, Icon, Segment, Divider } from 'semantic-ui-react';
import { Party } from '@digitalasset/daml-json-types';
import { User } from '../daml/create-daml-app/User';
import { useParty, useReload, usePseudoExerciseByKey, useFetchByKey, useQuery } from '../daml-react-hooks';
import UserList from './UserList';
import PartyListEdit from './PartyListEdit';

const MainView: React.FC = () => {
// -- HOOKS_BEGIN
  const party = useParty();
  const myUser = useFetchByKey(User, () => party, [party]).contract?.payload;
  const allUsers = useQuery(User, () => ({}), []).contracts.map((user) => user.payload);
// -- HOOKS_END
  const reload = useReload();

  const [exerciseAddFriend] = usePseudoExerciseByKey(User.AddFriend);
  const [exerciseRemoveFriend] = usePseudoExerciseByKey(User.RemoveFriend);

  const addFriend = async (friend: Party): Promise<boolean> => {
    try {
      await exerciseAddFriend({party}, {friend});
      return true;
    } catch (error) {
      alert("Unknown error:\n" + JSON.stringify(error));
      return false;
    }
  }

  const removeFriend = async (friend: Party): Promise<void> => {
    try {
      await exerciseRemoveFriend({party}, {friend});
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
                {myUser ? `Welcome, ${myUser.party}!` : 'Loading...'}
            </Header>

            <Segment>
              <Header as='h2'>
                <Icon name='user' />
                <Header.Content>
                  {myUser?.party ?? 'Loading...'}
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
                users={allUsers.sort((user1, user2) => user1.party.localeCompare(user2.party))}
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
