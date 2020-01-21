import React from 'react';
import MainView from './MainView';
import { Party } from '@digitalasset/daml-json-types';
import { User } from '../daml/create-daml-app/User';
import { useParty, useReload, usePseudoExerciseByKey, useFetchByKey, useQuery } from '../daml-react-hooks';

// -- HOOKS_BEGIN
/**
 * React component to control the `MainView`.
 */
const MainController: React.FC = () => {
  const party = useParty();
  const myUser = useFetchByKey(User, () => party, [party]);
  const allUsers = useQuery(User, () => ({}), []);
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

  const props = {
    myUser: myUser.contract?.payload,
    allUsers: allUsers.contracts.map((user) => user.payload),
    onAddFriend: addFriend,
    onRemoveFriend: removeFriend,
    onReload: reload,
  };

  return MainView(props);
}

export default MainController;
