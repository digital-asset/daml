-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE RankNTypes #-}

-- | Contexts for DAML LF static verification
module DA.Daml.LF.Verify.Context
  ( Env(..)
  , Error(..)
  , MonadEnv
  , UpdateSet(..)
  , UpdCreate(..)
  , UpdArchive(..)
  , UpdChoice(..)
  , runEnv
  , emptyEnv
  , introEnv, extVarEnv, extValEnv, extChEnv
  , lookupVar, lookupVal, lookupChoice
  , concatEnv
  , emptyUpdateSet
  , concatUpdateSet
  , solveValueUpdatesEnv
  , testPrint, lookupChoInHMap
  ) where

import Control.Monad.Error.Class (MonadError (..), throwError)
import Control.Monad.State.Lazy
import Data.Maybe (listToMaybe)
import qualified Data.HashMap.Strict as HM

import DA.Daml.LF.Ast hiding (lookupChoice)

data UpdCreate = UpdCreate
  { _creTemp  :: !(Qualified TypeConName)
    -- ^ Qualified type constructor corresponding to the contract template.
  , _creField :: ![(FieldName, Expr)]
    -- ^ The fields to be verified, together with their value.
  }
  deriving Show
data UpdArchive = UpdArchive
  { _arcTemp  :: !(Qualified TypeConName)
    -- ^ Qualified type constructor corresponding to the contract template.
  , _arcField :: ![(FieldName, Expr)]
    -- ^ The fields to be verified, together with their value.
  }
  deriving Show
data UpdChoice = UpdChoice
  { _choTemp  :: !(Qualified TypeConName)
    -- ^ Qualified type constructor corresponding to the contract template.
  , _choName  :: !ChoiceName
    -- ^ The name of the choice.
  }
  deriving Show

-- | The List of updates being performed
data UpdateSet = UpdateSet
  { _usCre :: ![UpdCreate]
    -- ^ The list of create updates.
  , _usArc :: ![UpdArchive]
    -- ^ The list of archive updates.
  , _usCho :: ![UpdChoice]
    -- ^ The list of choice updates.
  , _usVal :: ![Qualified ExprValName]
    -- ^ The list of referenced values. These will be replaced by their
    -- respective updates after solving.
  }
  deriving Show

emptyUpdateSet :: UpdateSet
emptyUpdateSet = UpdateSet [] [] [] []

concatUpdateSet :: UpdateSet -> UpdateSet -> UpdateSet
concatUpdateSet (UpdateSet cres1 arcs1 chos1 vals1) (UpdateSet cres2 arcs2 chos2 vals2) =
  UpdateSet (cres1 ++ cres2) (arcs1 ++ arcs2) (chos1 ++ chos2) (vals1 ++ vals2)

-- | The environment for the DAML-LF verifier
data Env = Env
  { _envvars :: ![ExprVarName]
    -- TODO: there seems to be a bug which makes this an infinite list...
    -- ^ The skolemised term variables.
  , _envvals :: !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet))
    -- ^ The bound values.
  , _envchs :: !(HM.HashMap (Qualified TypeConName, ChoiceName) UpdateSet)
    -- ^ The set of relevant choices.
  , _envdats :: !(HM.HashMap TypeConName DefDataType)
    -- ^ The set of data constructors.
  -- TODO: split this off into data types for readability?
  }
  deriving Show

emptyEnv :: Env
emptyEnv = Env [] HM.empty HM.empty HM.empty

concatEnv :: Env -> Env -> Env
concatEnv (Env vars1 vals1 chs1 dats1) (Env vars2 vals2 chs2 dats2) =
  Env (vars1 ++ vars2) (vals1 `HM.union` vals2) (chs1 `HM.union` chs2) (dats1 `HM.union` dats2)
  -- TODO: union makes me slightly nervous, as it allows overlapping keys
  -- (and just uses the first)

-- | Type class constraint with the required monadic effects for functions
-- manipulating the verification environment.
type MonadEnv m = (MonadError Error m, MonadState Env m)

runEnv :: StateT Env (Either Error) () -> Env -> Either Error Env
runEnv comp env0 = do
  (_res, env) <- runStateT comp env0
  return env

-- TODO: some of these might not be needed anymore.
-- | Run a computation in the current environment, with an additional
-- environment extension.
introEnv :: MonadEnv m => Env -> m ()
introEnv env = modify (concatEnv env)

extVarEnv :: MonadEnv m => ExprVarName -> m ()
extVarEnv x = get >>= \env@Env{..} -> put env{_envvars = x : _envvars}

extValEnv :: MonadEnv m => Qualified ExprValName -> Expr -> UpdateSet -> m ()
extValEnv val expr upd = get >>= \env@Env{..} -> put env{_envvals = HM.insert val (expr, upd) _envvals}

extChEnv :: MonadEnv m => Qualified TypeConName -> ChoiceName -> UpdateSet -> m ()
extChEnv tc ch upd = get >>= \env@Env{..} -> put env{_envchs = HM.insert (tc, ch) upd _envchs}

lookupVar :: MonadEnv m => ExprVarName -> m ()
lookupVar (ExprVarName "self") = return ()
lookupVar (ExprVarName "this") = return ()
-- TODO: Is there a nicer way to handle this instead of hardcoding?
lookupVar x = get >>= \ env -> unless (elem x $ _envvars env)
  (throwError $ UnboundVar x)

lookupVal :: MonadEnv m => Qualified ExprValName -> m (Expr, UpdateSet)
lookupVal val = do
  env <- get
  case lookupValInHMap (_envvals env) val of
    Just res -> return res
    Nothing -> throwError (UnknownValue val)

lookupChoice :: MonadEnv m => Qualified TypeConName -> ChoiceName
  -> m UpdateSet
lookupChoice _tem ch = do
  env <- get
  case lookupChoInHMap (_envchs env) ch of
    Nothing -> throwError (UnknownChoice ch)
    Just upd -> return upd

-- TODO: There seems to be something wrong with the qualifiers. This is a
-- temporary solution.
lookupValInHMap :: (HM.HashMap (Qualified ExprValName) (Expr, UpdateSet))
  -> Qualified ExprValName -> Maybe (Expr, UpdateSet)
lookupValInHMap hmap val = listToMaybe $ HM.elems
  $ HM.filterWithKey (\name _ -> (qualObject name) == (qualObject val)) hmap

lookupChoInHMap :: (HM.HashMap (Qualified TypeConName, ChoiceName) UpdateSet)
  -> ChoiceName -> Maybe UpdateSet
lookupChoInHMap hmap cho = listToMaybe $ HM.elems
  $ HM.filterWithKey (\(_, name) _ -> cho == name) hmap

solveValueUpdatesEnv :: Env -> Env
solveValueUpdatesEnv env =
  let hmap0 = _envvals env
      hmap1 = foldl solveValueUpdate hmap0 (HM.keys hmap0)
  in env{_envvals = hmap1}

solveValueUpdate :: (HM.HashMap (Qualified ExprValName) (Expr, UpdateSet))
  -> (Qualified ExprValName) -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)
solveValueUpdate hmap0 val0 =
  let (hmap1, _) = step (hmap0, emptyUpdateSet) val0
  in hmap1
  where
    step :: ((HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)), UpdateSet)
      -> (Qualified ExprValName)
      -> ((HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)), UpdateSet)
    step (hmap, baseupd) val = case lookupValInHMap hmap val of
      Nothing -> error ("Impossible! Undefined value: " ++ (show $ unExprValName $ qualObject val))
      Just (expr, valupd) ->
        let (nhmap, nvalupd) = foldl step (hmap, valupd{_usVal = []}) (_usVal valupd)
        in (HM.insert val (expr, nvalupd) nhmap, concatUpdateSet baseupd nvalupd)

data Error
  = UnknownValue (Qualified ExprValName)
  | UnknownChoice ChoiceName
  | UnboundVar ExprVarName
  | ExpectRecord
  | CyclicModules [ModuleName]

instance Show Error where
  show (UnknownValue qname) = ("Impossible: Unknown value definition: "
    ++ (show $ unExprValName $ qualObject qname))
  show (UnknownChoice ch) = ("Impossible: Unknown choice definition: " ++ (show ch))
  show (UnboundVar name) = ("Impossible: Unbound term variable: " ++ (show name))
  show ExpectRecord = "Impossible: Expected a record type"
  show (CyclicModules mods) = "Cyclic modules: " ++ (show mods)

-- | For testing purposes: print the stored value definitions in the environment.
-- TODO: Remove
testPrint :: (HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)) -> IO ()
testPrint hmap = putStrLn "Test print keys:"
  >> mapM_ print (HM.keys hmap)
