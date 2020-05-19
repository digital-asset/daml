-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}

-- | Contexts for DAML LF static verification
module DA.Daml.LF.Verify.Context
  ( Phase(..)
  , GenPhase
  , BoolExpr(..)
  , Cond(..)
  , Env(..)
  , Error(..)
  , MonadEnv
  , UpdateSet(..)
  , Upd(..)
  , ChoiceData(..)
  , UpdChoice(..)
  , Skolem(..)
  , getEnv
  , runEnv
  , genRenamedVar
  , emptyEnv
  , extVarEnv, extRecEnv, extValEnv, extChEnv, extDatsEnv, extCidEnv, extCtrRec
  , extRecEnvLvl1
  , lookupVar, lookupRec, lookupVal, lookupChoice, lookupDataCon, lookupCid
  , concatEnv
  , emptyUpdateSet
  , concatUpdateSet
  , addUpd
  , conditionalUpdateSet
  , solveValueReferences
  , solveChoiceReferences
  , fieldName2VarName
  , recTypConFields, recTypFields, recExpFields
  ) where

import Control.Monad.Error.Class (MonadError (..), throwError)
import Control.Monad.State.Lazy
import Data.Hashable
import GHC.Generics
import Data.Maybe (isJust, fromMaybe)
import Data.List (find)
import Data.Bifunctor
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import Debug.Trace

import DA.Daml.LF.Ast hiding (lookupChoice)
import DA.Daml.LF.Verify.Subst

-- TODO: Move these data types to a seperate file?
-- | Data type denoting the phase of the constraint generator.
data Phase
  = ValueGathering
  -- ^ The value phase gathers all value and data type definitions across modules.
  | ChoiceGathering
  -- ^ The choice phase gathers the updates performed in choice definitions.
  | Solving
  -- ^ During the solving phase, all definitions have been loaded and updates
  -- have been inlined.

-- | Data type denoting a boolean condition expression. This data type was
-- introduced as DAML-LF does not have build-in boolean operators, and using
-- prelude functions gets messy.
data BoolExpr
  = BExpr Expr
  -- ^ A daml-lf expression.
  | BAnd BoolExpr BoolExpr
  -- ^ And operator.
  | BNot BoolExpr
  -- ^ Not operator.
  deriving Show

-- | Data type denoting a potentially conditional value.
data Cond a
  = Determined a
  -- ^ Non-conditional value.
  | Conditional BoolExpr [Cond a] [Cond a]
  -- ^ Conditional value, with a (Boolean) condition, at least one value in case
  -- the condition holds, and at least one value in case it doesn't.
  -- Note that these branch lists should not be empty.
  -- TODO: Encode this invariant in the type system?
  deriving (Show, Functor)

-- | Construct a simple conditional.
createCond :: BoolExpr
  -- ^ The condition to depend on.
  -> a
  -- ^ The value in case the condition holds.
  -> a
  -- ^ The value in case the condition does not hold.
  -> Cond a
createCond cond x y = Conditional cond [Determined x] [Determined y]

-- | Shift the conditional inside the update set.
introCond :: Cond (UpdateSet ph) -> UpdateSet ph
introCond (Determined upds) = upds
introCond (Conditional e updx updy) = case getPhase updx of
  UpdateSetVG{} -> UpdateSetVG
    (buildCond updx updy _usvgUpdate)
    (buildCond updx updy _usvgChoice)
    (buildCond updx updy _usvgValue)
  UpdateSetCG{} -> UpdateSetCG
    (buildCond updx updy _uscgUpdate)
    (buildCond updx updy _uscgChoice)
  UpdateSetS{} -> UpdateSetS
    (buildCond updx updy _ussUpdate)
  where
    -- | Construct a single conditional, if the input is not empty.
    buildCond :: [Cond (UpdateSet ph)]
      -- ^ The input for the true case.
      -> [Cond (UpdateSet ph)]
      -- ^ The input for the false case.
      -> (UpdateSet ph -> [Cond a])
      -- ^ The fetch function.
      -> [Cond a]
    buildCond updx updy get =
      let xs = concatCond updx get
          ys = concatCond updy get
      in [Conditional e xs ys | not (null xs && null ys)]

    -- TODO: Temporary solution. Make introCond a part of the GenPhase class instead.
    getPhase :: [Cond (UpdateSet ph)] -> UpdateSet ph
    getPhase lst = case head lst of
      Determined upds -> upds
      Conditional _ xs _ -> getPhase xs

-- | Fetch the conditionals from the conditional update set, and flatten the
-- two layers into one.
concatCond :: [Cond (UpdateSet ph)]
  -- ^ The conditional update set to fetch from.
  -> (UpdateSet ph -> [Cond a])
  -- ^ The fetch function.
  -> [Cond a]
concatCond upds get =
  let upds' = map introCond upds
  in concatMap get upds'

-- | Data type denoting an update.
data Upd
  = UpdCreate
  -- ^ Data type denoting a create update.
    { _creTemp  :: !(Qualified TypeConName)
     -- ^ Qualified type constructor corresponding to the contract template.
    , _creField :: ![(FieldName, Expr)]
      -- ^ The fields to be verified, together with their value.
    }
  | UpdArchive
  -- ^ Data type denoting an archive update.
    { _arcTemp  :: !(Qualified TypeConName)
      -- ^ Qualified type constructor corresponding to the contract template.
    , _arcField :: ![(FieldName, Expr)]
      -- ^ The fields to be verified, together with their value.
    }
  deriving Show

-- | Data type denoting an exercised choice.
data UpdChoice = UpdChoice
  { _choTemp  :: !(Qualified TypeConName)
    -- ^ Qualified type constructor corresponding to the contract template.
  , _choName  :: !ChoiceName
    -- ^ The name of the choice.
  }
  deriving (Eq, Generic, Hashable, Show)

-- | The collection of updates being performed.
data UpdateSet (ph :: Phase) where
  UpdateSetVG ::
    { _usvgUpdate :: ![Cond Upd]
      -- ^ The list of updates.
    , _usvgChoice :: ![Cond UpdChoice]
      -- ^ The list of exercised choices.
    , _usvgValue :: ![Cond (Qualified ExprValName)]
      -- ^ The list of referenced values.
    } -> UpdateSet 'ValueGathering
  UpdateSetCG ::
    { _uscgUpdate :: ![Cond Upd]
      -- ^ The list of updates.
    , _uscgChoice :: ![Cond UpdChoice]
      -- ^ The list of exercised choices.
    } -> UpdateSet 'ChoiceGathering
  UpdateSetS ::
    { _ussUpdate :: ![Cond Upd]
      -- ^ The list of updates.
    } -> UpdateSet 'Solving

class GenPhase ph where
  emptyUpdateSet :: UpdateSet ph
  emptyEnv :: Env ph

instance GenPhase 'ValueGathering where
  emptyUpdateSet = UpdateSetVG [] [] []
  emptyEnv = EnvVG [] HM.empty HM.empty HM.empty []

instance GenPhase 'ChoiceGathering where
  emptyUpdateSet = UpdateSetCG [] []
  emptyEnv = EnvCG [] HM.empty HM.empty HM.empty [] HM.empty

instance GenPhase 'Solving where
  emptyUpdateSet = UpdateSetS []
  emptyEnv = EnvS [] HM.empty HM.empty HM.empty [] HM.empty

-- | Combine two update sets.
concatUpdateSet :: UpdateSet ph
  -- ^ The first update set to be combined.
  -> UpdateSet ph
  -- ^ The second update set to be combined.
  -> UpdateSet ph
concatUpdateSet (UpdateSetVG upd1 cho1 val1) (UpdateSetVG upd2 cho2 val2) =
  UpdateSetVG (upd1 ++ upd2) (cho1 ++ cho2) (val1 ++ val2)
concatUpdateSet (UpdateSetCG upd1 cho1) (UpdateSetCG upd2 cho2) =
  UpdateSetCG (upd1 ++ upd2) (cho1 ++ cho2)
concatUpdateSet (UpdateSetS upd1) (UpdateSetS upd2) =
  UpdateSetS (upd1 ++ upd2)

-- | Add a single Upd to an UpdateSet
addUpd :: UpdateSet ph
  -- ^ The update set to extend.
  -> Upd
  -- ^ The update to add.
  -> UpdateSet ph
addUpd upds@UpdateSetVG{..} upd = upds{_usvgUpdate = Determined upd : _usvgUpdate}
addUpd upds@UpdateSetCG{..} upd = upds{_uscgUpdate = Determined upd : _uscgUpdate}
addUpd upds@UpdateSetS{..} upd = upds{_ussUpdate = Determined upd : _ussUpdate}

-- | Make an update set conditional. A second update set can also be introduced
-- for the case where the condition does not hold.
conditionalUpdateSet :: Expr
  -- ^ The condition on which to combine the two update sets.
  -> UpdateSet ph
  -- ^ The update set in case the condition holds.
  -> UpdateSet ph
  -- ^ The update set in case the condition does not hold.
  -> UpdateSet ph
conditionalUpdateSet exp upd1 upd2 =
  introCond $ createCond (BExpr exp) upd1 upd2

-- | Refresh a given expression variable by producing a fresh renamed variable.
-- TODO: when a renamed var gets renamed again, it might overlap again.
-- We should have an additional field in VarName to denote its number.
genRenamedVar :: MonadEnv m ph
  => ExprVarName
  -- ^ The variable to be renamed.
  -> m ExprVarName
genRenamedVar (ExprVarName x) = ExprVarName . T.append x . T.pack <$> fresh

-- | Data type denoting a skolemized variable.
data Skolem
  = SkolVar ExprVarName
    -- ^ Skolemised term variable.
  | SkolRec ExprVarName [FieldName]
    -- ^ List of skolemised field names, with their variable.
    -- e.g. `this.field`
  deriving (Eq, Show)

-- | Data type denoting a contract id.
data Cid
  = CidVar ExprVarName
    -- ^ An expression variable denoting a contract id.
  | CidRec ExprVarName FieldName
    -- ^ A record projection denoting a contract id.
  deriving (Generic, Hashable, Eq, Show)

-- | Convert an expression to a contract id, if possible.
expr2cid :: MonadEnv m ph
  => Expr
  -- ^ The expression to be converted.
  -> m Cid
expr2cid (EVar x) = return $ CidVar x
expr2cid (ERecProj _ f (EVar x)) = return $ CidRec x f
expr2cid (EStructProj f (EVar x)) = return $ CidRec x f
expr2cid _ = throwError ExpectCid

-- | Data type containing the data stored for a choice definition.
data ChoiceData (ph :: Phase) = ChoiceData
  { _cdSelf :: ExprVarName
    -- ^ The variable denoting `self`.
  , _cdThis :: ExprVarName
    -- ^ The variable denoting `this`.
  , _cdArgs :: ExprVarName
    -- ^ The variable denoting `args`.
  , _cdUpds :: Expr -> Expr -> Expr -> UpdateSet ph
    -- ^ Function from self, this and args to the updates performed by this choice.
  , _cdType :: Type
    -- ^ The return type of this choice.
  }

-- TODO: Could we alternatively just declare the variables that occur in the updates and drop the skolems?
-- | The environment for the DAML-LF verifier
data Env (ph :: Phase) where
  EnvVG ::
    { _envvgskol :: ![Skolem]
      -- ^ The skolemised term variables and fields.
    , _envvgvals :: !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering))
      -- ^ The bound values.
    , _envvgdats :: !(HM.HashMap TypeConName DefDataType)
      -- ^ The set of data constructors.
    , _envvgcids :: !(HM.HashMap Cid (ExprVarName, [ExprVarName]))
      -- ^ The set of fetched cid's mapped to their current variable name, along
      -- with a list of any potential old variable names.
    , _envvgctrs :: ![(Expr, Expr)]
      -- ^ Additional equality constraints.
    } -> Env 'ValueGathering
  EnvCG ::
    { _envcgskol :: ![Skolem]
      -- ^ The skolemised term variables and fields.
    , _envcgvals :: !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ChoiceGathering))
      -- ^ The bound values.
    , _envcgdats :: !(HM.HashMap TypeConName DefDataType)
      -- ^ The set of data constructors.
    , _envcgcids :: !(HM.HashMap Cid (ExprVarName, [ExprVarName]))
      -- ^ The set of fetched cid's mapped to their current variable name, along
      -- with a list of any potential old variable names.
    , _envcgctrs :: ![(Expr, Expr)]
      -- ^ Additional equality constraints.
    , _envcgchs :: !(HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering))
      -- ^ The set of relevant choices.
    } -> Env 'ChoiceGathering
  EnvS ::
    { _envsskol :: ![Skolem]
      -- ^ The skolemised term variables and fields.
    , _envsvals :: !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'Solving))
      -- ^ The bound values.
    , _envsdats :: !(HM.HashMap TypeConName DefDataType)
      -- ^ The set of data constructors.
    , _envscids :: !(HM.HashMap Cid (ExprVarName, [ExprVarName]))
      -- ^ The set of fetched cid's mapped to their current variable name, along
      -- with a list of any potential old variable names.
    , _envsctrs :: ![(Expr, Expr)]
      -- ^ Additional equality constraints.
    , _envschs :: !(HM.HashMap UpdChoice (ChoiceData 'Solving))
      -- ^ The set of relevant choices.
    } -> Env 'Solving

-- | Combine two environments.
concatEnv :: Env ph
  -- ^ The first environment to be combined.
  -> Env ph
  -- ^ The second environment to be combined.
  -> Env ph
concatEnv (EnvVG vars1 vals1 dats1 cids1 ctrs1) (EnvVG vars2 vals2 dats2 cids2 ctrs2) =
  EnvVG (vars1 ++ vars2) (vals1 `HM.union` vals2) (dats1 `HM.union` dats2)
    (cids1 `HM.union` cids2) (ctrs1 ++ ctrs2)
concatEnv (EnvCG vars1 vals1 dats1 cids1 ctrs1 chos1) (EnvCG vars2 vals2 dats2 cids2 ctrs2 chos2) =
  EnvCG (vars1 ++ vars2) (vals1 `HM.union` vals2) (dats1 `HM.union` dats2)
    (cids1 `HM.union` cids2) (ctrs1 ++ ctrs2) (chos1 `HM.union` chos2)
concatEnv (EnvS vars1 vals1 dats1 cids1 ctrs1 chos1) (EnvS vars2 vals2 dats2 cids2 ctrs2 chos2) =
  EnvS (vars1 ++ vars2) (vals1 `HM.union` vals2) (dats1 `HM.union` dats2)
    (cids1 `HM.union` cids2) (ctrs1 ++ ctrs2) (chos1 `HM.union` chos2)
  -- TODO: union makes me slightly nervous, as it allows overlapping keys
  -- (and just uses the first). `unionWith concatUpdateSet` would indeed be better,
  -- but this still makes me nervous as the expr and exprvarnames wouldn't be merged.

-- | Convert a fieldname into an expression variable name.
fieldName2VarName :: FieldName -> ExprVarName
fieldName2VarName = ExprVarName . unFieldName

-- | Type class constraint with the required monadic effects for functions
-- manipulating the verification environment.
type MonadEnv m ph = (MonadError Error m, MonadState (Int,Env ph) m)

-- | Fetch the current environment.
getEnv :: MonadEnv m ph => m (Env ph)
getEnv = snd <$> get

-- | Set the current environment.
putEnv :: MonadEnv m ph => Env ph -> m ()
putEnv env = get >>= \(uni,_) -> put (uni,env)

-- | Generate a new unique name.
fresh :: MonadEnv m ph => m String
fresh = do
  (cur,env) <- get
  put (cur + 1,env)
  return $ show cur

-- | Evaluate the MonadEnv to produce an error message or the final environment.
runEnv :: StateT (Int, Env ph) (Either Error) ()
  -- ^ The monadic computation to be evaluated.
  -> Env ph
  -- ^ The initial environment to start from.
  -> Either Error (Env ph)
runEnv comp env0 = do
  (_res, (_uni,env1)) <- runStateT comp (0,env0)
  return env1

-- | Skolemise an expression variable and extend the environment.
extVarEnv :: MonadEnv m ph
  => ExprVarName
  -- ^ The expression variable to be skolemised.
  -> m ()
extVarEnv x = extSkolEnv (SkolVar x)

-- | Skolemise a list of record projection and extend the environment.
extRecEnv :: MonadEnv m ph
  => ExprVarName
  -- ^ The variable on which is being projected.
  -> [FieldName]
  -- ^ The fields which should be skolemised.
  -> m ()
extRecEnv x fs = extSkolEnv (SkolRec x fs)

-- | Extend the environment with a new skolem variable.
extSkolEnv :: MonadEnv m ph
  => Skolem
  -- ^ The skolem variable to add.
  -> m ()
extSkolEnv skol = getEnv >>= \case
  env@EnvVG{..} -> putEnv env{_envvgskol = skol : _envvgskol}
  env@EnvCG{..} -> putEnv env{_envcgskol = skol : _envcgskol}
  env@EnvS{..} -> putEnv env{_envsskol = skol : _envsskol}

-- | Extend the environment with a new value definition.
extValEnv :: MonadEnv m ph
  => Qualified ExprValName
  -- ^ The name of the value being defined.
  -> Expr
  -- ^ The (partially) evaluated value definition.
  -> UpdateSet ph
  -- ^ The updates performed by this value.
  -> m ()
extValEnv val expr upd = getEnv >>= \case
  env@EnvVG{..} -> putEnv env{_envvgvals = HM.insert val (expr, upd) _envvgvals}
  env@EnvCG{..} -> putEnv env{_envcgvals = HM.insert val (expr, upd) _envcgvals}
  env@EnvS{..} -> putEnv env{_envsvals = HM.insert val (expr, upd) _envsvals}

-- | Extends the environment with a new choice.
extChEnv :: MonadEnv m ph
  => Qualified TypeConName
  -- ^ The type of the template on which this choice is defined.
  -> ChoiceName
  -- ^ The name of the new choice.
  -> ExprVarName
  -- ^ Variable to bind the ContractId on which this choice is exercised on to.
  -> ExprVarName
  -- ^ Variable to bind the contract on which this choice is exercised on to.
  -> ExprVarName
  -- ^ Variable to bind the choice argument to.
  -> UpdateSet ph
  -- ^ The updates performed by the new choice.
  -> Type
  -- ^ The result type of the new choice.
  -> m ()
extChEnv tc ch self this arg upd typ =
  let substUpd sExp tExp aExp = substituteTm (createExprSubst [(self,sExp),(this,tExp),(arg,aExp)]) upd
  in getEnv >>= \case
    EnvVG{} -> error "Impossible: extChEnv is not used in the value gathering phase"
    env@EnvCG{..} -> putEnv env{_envcgchs = HM.insert (UpdChoice tc ch) (ChoiceData self this arg substUpd typ) _envcgchs}
    env@EnvS{..} -> putEnv env{_envschs = HM.insert (UpdChoice tc ch) (ChoiceData self this arg substUpd typ) _envschs}

-- | Extend the environment with a list of new data type definitions.
extDatsEnv :: MonadEnv m ph
  => HM.HashMap TypeConName DefDataType
  -- ^ A hashmap of the data constructor names, with their corresponding definitions.
  -> m ()
extDatsEnv hmap = getEnv >>= \case
    env@EnvVG{..} -> putEnv env{_envvgdats = hmap `HM.union` _envvgdats}
    env@EnvCG{..} -> putEnv env{_envcgdats = hmap `HM.union` _envcgdats}
    env@EnvS{..} -> putEnv env{_envsdats = hmap `HM.union` _envsdats}

-- | Extend the environment with a new contract id, and the variable to which
-- the fetched contract is bound.
extCidEnv :: MonadEnv m ph
  => Expr
  -- ^ The contract id expression.
  -> ExprVarName
  -- ^ The variable name to which the fetched contract is bound.
  -> m ()
extCidEnv exp var = do
  prev <- do
    { (cur, old) <- lookupCid exp
    ; return $ cur : old }
    `catchError` (\_ -> return [])
  cid <- expr2cid exp
  let new = (var, prev)
  getEnv >>= \case
    env@EnvVG{..} -> putEnv env{_envvgcids = HM.insert cid new _envvgcids}
    env@EnvCG{..} -> putEnv env{_envcgcids = HM.insert cid new _envcgcids}
    env@EnvS{..} -> putEnv env{_envscids = HM.insert cid new _envscids}

-- | Extend the environment with additional equality constraints, between a
-- variable and its field values.
extCtrRec :: MonadEnv m ph
  => ExprVarName
  -- ^ The variable to be asserted.
  -> [(FieldName, Expr)]
  -- ^ The fields with their values.
  -> m ()
extCtrRec var fields = do
  let ctrs = map (\(f, e) -> (EStructProj f (EVar var), e)) fields
  getEnv >>= \case
    env@EnvVG{..} -> putEnv env{_envvgctrs = ctrs ++ _envvgctrs}
    env@EnvCG{..} -> putEnv env{_envcgctrs = ctrs ++ _envcgctrs}
    env@EnvS{..} -> putEnv env{_envsctrs = ctrs ++ _envsctrs}

-- TODO: Is one layer of recursion enough?
-- | Recursively skolemise the given record fields, when they have a record
-- type. Note that this only works 1 level deep.
extRecEnvLvl1 :: MonadEnv m ph
  => [(FieldName, Type)]
  -- ^ The record fields to skolemise, together with their types.
  -> m ()
extRecEnvLvl1 = mapM_ step
  where
    step :: MonadEnv m ph => (FieldName, Type) -> m ()
    step (f,typ) = do
      { fsRec <- recTypFields typ
      ; extRecEnv (fieldName2VarName f) fsRec
      }
      -- TODO: Temporary fix
      `catchError` (\_ -> return ())

-- | Lookup an expression variable in the environment. Returns `True` if this variable
-- has been skolemised, or `False` otherwise.
lookupVar :: MonadEnv m ph
  => ExprVarName
  -- ^ The expression variable to look up.
  -> m Bool
lookupVar x = getEnv >>= \case
  EnvVG{..} -> return $ elem (SkolVar x) _envvgskol
  EnvCG{..} -> return $ elem (SkolVar x) _envcgskol
  EnvS{..} -> return $ elem (SkolVar x) _envsskol

-- | Lookup a record project in the environment. Returns a boolean denoting
-- whether or not the record projection has been skolemised.
lookupRec :: MonadEnv m ph
  => ExprVarName
  -- ^ The expression variable on which is being projected.
  -> FieldName
  -- ^ The field name which is being projected.
  -> m Bool
lookupRec x f = do
  skols <- getEnv >>= \case
    EnvVG{..} -> return _envvgskol
    EnvCG{..} -> return _envcgskol
    EnvS{..} -> return _envsskol
  let fields = [ fs | SkolRec y fs <- skols, x == y ]
  if not (null fields)
    then return (elem f $ head fields)
    else return False

-- | Lookup a value name in the environment. Returns its (partially) evaluated
-- definition, together with the updates it performs.
lookupVal :: MonadEnv m ph
  => Qualified ExprValName
  -- ^ The value name to lookup.
  -> m (Expr, UpdateSet ph)
lookupVal val = do
  vals <- getEnv >>= \case
    EnvVG{..} -> return _envvgvals
    EnvCG{..} -> return _envcgvals
    EnvS{..} -> return _envsvals
  case HM.lookup val vals of
    Just res -> return res
    Nothing -> throwError (UnknownValue val)

-- | Lookup a choice name in the environment. Returns a function which, once
-- self, this and args have been instantiated, returns the set of updates it
-- performs. Also returns the return type of the choice.
lookupChoice :: MonadEnv m ph
  => Qualified TypeConName
  -- ^ The template name in which this choice is defined.
  -> ChoiceName
  -- ^ The choice name to lookup.
  -> m (Expr -> Expr -> Expr -> UpdateSet ph, Type)
lookupChoice tem ch = do
  chs <- getEnv >>= \case
    EnvVG{..} -> return HM.empty
    EnvCG{..} -> return _envcgchs
    EnvS{..} -> return _envschs
  case HM.lookup (UpdChoice tem ch) chs of
    Nothing -> throwError (UnknownChoice ch)
    Just ChoiceData{..} -> return (_cdUpds, _cdType)

-- | Lookup a data type definition in the environment.
lookupDataCon :: MonadEnv m ph
  => TypeConName
  -- ^ The data constructor to lookup.
  -> m DefDataType
lookupDataCon tc = do
  dats <- getEnv >>= \case
    EnvVG{..} -> return _envvgdats
    EnvCG{..} -> return _envcgdats
    EnvS{..} -> return _envsdats
  case HM.lookup tc dats of
    Nothing -> throwError (UnknownDataCons tc)
    Just def -> return def

-- | Lookup a contract id in the environment. Returns the variable its fetched
-- contract is bound to, along with a list of any previous bindings.
lookupCid :: MonadEnv m ph
  => Expr
  -- ^ The contract id to lookup.
  -> m (ExprVarName, [ExprVarName])
lookupCid exp = do
  cid <- expr2cid exp
  cids <- getEnv >>= \case
    EnvVG{..} -> return _envvgcids
    EnvCG{..} -> return _envcgcids
    EnvS{..} -> return _envscids
  case HM.lookup cid cids of
    Nothing -> throwError $ UnknownCid cid
    Just var -> return var

-- | Solves the value references by computing the closure of all referenced
-- values, for each value in the environment.
-- It thus empties `_usValue` by collecting all updates made by this closure.
solveValueReferences :: Env 'ValueGathering -> Env 'ChoiceGathering
solveValueReferences EnvVG{..} =
  let valhmap = foldl (\hmap ref -> snd $ solveReference lookup_ref get_refs ext_upds intro_cond empty_upds [] hmap ref) _envvgvals (HM.keys _envvgvals)
  in EnvCG _envvgskol (convertHMap valhmap) _envvgdats _envvgcids _envvgctrs HM.empty
  where
    lookup_ref :: Qualified ExprValName
      -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering)
      -> (Expr, UpdateSet 'ValueGathering)
    lookup_ref ref hmap = fromMaybe (error "Impossible: Undefined value ref while solving")
      (HM.lookup ref hmap)

    get_refs :: (Expr, UpdateSet 'ValueGathering)
      -> ([Cond (Qualified ExprValName)], (Expr, UpdateSet 'ValueGathering))
    get_refs (e, upds@UpdateSetVG{..}) = (_usvgValue, (e, upds{_usvgValue = []}))

    ext_upds :: (Expr, UpdateSet 'ValueGathering) -> (Expr, UpdateSet 'ValueGathering)
      -> (Expr, UpdateSet 'ValueGathering)
    ext_upds (e, upds1)  (_, upds2) = (e, concatUpdateSet upds1 upds2)

    intro_cond :: Cond (Expr, UpdateSet 'ValueGathering)
      -> (Expr, UpdateSet 'ValueGathering)
    -- Note that the expression is not important here, as it will be ignored in
    -- `ext_upds` later on.
    intro_cond (Determined x) = x
    intro_cond (Conditional cond cx cy) =
      let xs = map intro_cond cx
          ys = map intro_cond cy
          e = fst $ head xs
          updx = foldl concatUpdateSet emptyUpdateSet $ map snd xs
          updy = foldl concatUpdateSet emptyUpdateSet $ map snd ys
      in (e, introCond $ createCond cond updx updy)

    empty_upds :: (Expr, UpdateSet 'ValueGathering)
      -> (Expr, UpdateSet 'ValueGathering)
    empty_upds (e, _) = (e, emptyUpdateSet)

    convertHMap :: HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering)
      -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ChoiceGathering)
    convertHMap = HM.map (second updateSetVG2CG)

    updateSetVG2CG :: UpdateSet 'ValueGathering -> UpdateSet 'ChoiceGathering
    updateSetVG2CG UpdateSetVG{..} = if null _usvgValue
      then UpdateSetCG _usvgUpdate _usvgChoice
      else error "Impossible: There should be no references remaining after value solving"

-- | Solves the choice references by computing the closure of all referenced
-- choices, for each choice in the environment.
-- It thus empties `_usChoice` by collecting all updates made by this closure.
solveChoiceReferences :: Env 'ChoiceGathering -> Env 'Solving
solveChoiceReferences EnvCG{..} =
  let chhmap = foldl (\hmap ref -> snd $ solveReference lookup_ref get_refs ext_upds intro_cond empty_upds [] hmap ref) _envcgchs (HM.keys _envcgchs)
      chshmap = convertChHMap chhmap
      valhmap = HM.map (inlineChoices chshmap) _envcgvals
  in EnvS _envcgskol valhmap _envcgdats _envcgcids _envcgctrs chshmap
  where
    lookup_ref :: UpdChoice
      -> HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering)
      -> ChoiceData 'ChoiceGathering
    lookup_ref upd hmap = fromMaybe (error "Impossible: Undefined choice ref while solving")
      (HM.lookup upd hmap)

    get_refs :: ChoiceData 'ChoiceGathering
      -> ([Cond UpdChoice], ChoiceData 'ChoiceGathering)
    -- TODO: This is gonna result in a ton of substitutions
    get_refs chdat@ChoiceData{..} =
      -- TODO: This seems to be a rather common pattern. Abstract to reduce duplication.
      let chos = _uscgChoice $ _cdUpds (EVar _cdSelf) (EVar _cdThis) (EVar _cdArgs)
          updfunc1 (selfexp :: Expr) (thisexp :: Expr) (argsexp :: Expr) =
            let upds@UpdateSetCG{..} = _cdUpds selfexp thisexp argsexp
            in upds{_uscgChoice = []}
      in (chos, chdat{_cdUpds = updfunc1})

    ext_upds :: ChoiceData 'ChoiceGathering
      -> ChoiceData 'ChoiceGathering
      -> ChoiceData 'ChoiceGathering
    ext_upds chdat1 chdat2 =
      let updfunc (selfexp :: Expr) (thisexp :: Expr) (argsexp :: Expr) =
            _cdUpds chdat1 selfexp thisexp argsexp `concatUpdateSet`
              _cdUpds chdat2 selfexp thisexp argsexp
      in chdat1{_cdUpds = updfunc}

    intro_cond :: GenPhase ph
      => Cond (ChoiceData ph)
      -> ChoiceData ph
    -- Note that the expression and return type is not important here, as it
    -- will be ignored in `ext_upds` later on.
    intro_cond (Determined x) = x
    intro_cond (Conditional cond cdatxs cdatys) =
      let datxs = map intro_cond cdatxs
          datys = map intro_cond cdatys
          updfunc (selfexp :: Expr) (thisexp :: Expr) (argsexp :: Expr) =
            introCond (createCond cond
              (foldl
                (\upd dat -> upd `concatUpdateSet` _cdUpds dat selfexp thisexp argsexp)
                emptyUpdateSet datxs)
              (foldl
                (\upd dat -> upd `concatUpdateSet` _cdUpds dat selfexp thisexp argsexp)
                emptyUpdateSet datys))
      in (head datxs){_cdUpds = updfunc}

    empty_upds :: ChoiceData 'ChoiceGathering
      -> ChoiceData 'ChoiceGathering
    empty_upds dat = dat{_cdUpds = \ _ _ _ -> emptyUpdateSet}

    inlineChoices :: HM.HashMap UpdChoice (ChoiceData 'Solving)
      -> (Expr, UpdateSet 'ChoiceGathering)
      -> (Expr, UpdateSet 'Solving)
    inlineChoices chshmap (exp, UpdateSetCG{..}) =
      let lookupRes = map
            (intro_cond . fmap (\ch -> fromMaybe (error "Impossible: missing choice while solving") (HM.lookup ch chshmap)))
            _uscgChoice
          chupds = concatMap (\ChoiceData{..} -> _ussUpdate $ _cdUpds (EVar _cdSelf) (EVar _cdThis) (EVar _cdArgs)) lookupRes
      in (exp, UpdateSetS (_uscgUpdate ++ chupds))

    convertChHMap :: HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering)
      -> HM.HashMap UpdChoice (ChoiceData 'Solving)
    convertChHMap = HM.map (\chdat@ChoiceData{..} ->
      chdat{_cdUpds = \(selfExp :: Expr) (thisExp :: Expr) (argsExp :: Expr) ->
        updateSetCG2S $ _cdUpds selfExp thisExp argsExp})

    updateSetCG2S :: UpdateSet 'ChoiceGathering -> UpdateSet 'Solving
    updateSetCG2S UpdateSetCG{..} = if null _uscgChoice
      then UpdateSetS _uscgUpdate
      else error "Impossible: There should be no references remaining after choice solving"

-- | Solves a single reference by recursively inlining the references into updates.
solveReference :: forall updset ref. (Eq ref, Hashable ref)
  => (ref -> HM.HashMap ref updset -> updset)
  -- ^ Function for looking up references in the update set.
  -> (updset -> ([Cond ref], updset))
  -- ^ Function popping the references from the update set.
  -> (updset -> updset -> updset)
  -- ^ Function for concatinating update sets.
  -> (Cond updset -> updset)
  -- ^ Function for moving conditionals inside the update set.
  -> (updset -> updset)
  -- ^ Function for emptying a given update set of all updates.
  -> [ref]
  -- ^ The references which have already been visited.
  -> HM.HashMap ref updset
  -- ^ The hashmap mapping references to update sets.
  -> ref
  -- ^ The reference to be solved.
  -> (updset, HM.HashMap ref updset)
solveReference lookup getRefs extUpds introCond emptyUpds vis hmap0 ref0 =
  -- Lookup updates performed by the given reference, and split in new
  -- references and reference-free updates.
  let upd0 = lookup ref0 hmap0
      (refs, upd1) = getRefs upd0
  -- Check for loops. If the references has already been visited, then the
  -- reference should be flagged as recursive.
  in if ref0 `elem` vis
  -- TODO: Recursion!
    then trace "Recursion!" (upd1, hmap0) -- TODO: At least remove the references?
    -- When no recursion has been detected, continue inlining the references.
    else let (upd2, hmap1) = foldl handle_ref (upd1, hmap0) refs
      in (upd1, HM.insert ref0 upd2 hmap1)
  where
    -- | Extend the closure by computing and adding the reference closure for
    -- the given reference.
    handle_ref :: (updset, HM.HashMap ref updset)
      -- ^ The current closure (update set) and the current map for reference to update.
      -> Cond ref
      -- ^ The reference to be computed and added.
      -> (updset, HM.HashMap ref updset)
    -- For a simple reference, the closure is computed straightforwardly.
    handle_ref (upd_i0, hmap_i0) (Determined ref_i) =
      let (upd_i1, hmap_i1) =
            solveReference lookup getRefs extUpds introCond emptyUpds (ref0:vis) hmap_i0 ref_i
      in (extUpds upd_i0 upd_i1, hmap_i1)
    -- A conditional reference is more involved, as the conditional needs to be
    -- preserved in the computed closure (update set).
    handle_ref (upd_i0, hmap_i0) (Conditional cond refs_ia refs_ib) =
          -- Construct an update set without any updates.
      let upd_i0_empty = emptyUpds upd_i0
          -- Compute the closure for the true-case.
          (upd_ia, hmap_ia) = foldl handle_ref (upd_i0_empty, hmap_i0) refs_ia
          -- Compute the closure for the false-case.
          (upd_ib, hmap_ib) = foldl handle_ref (upd_i0_empty, hmap_ia) refs_ib
          -- Move the conditional inwards, in the update set.
          upd_i1 = extUpds upd_i0 $ introCond $ createCond cond upd_ia upd_ib
      in (upd_i1, hmap_ib)

-- TODO: This should work recursively
-- | Lookup the field names and corresponding types, for a given record type
-- constructor name.
recTypConFields :: MonadEnv m ph
  => TypeConName
  -- ^ The record type constructor name to lookup.
  -> m [(FieldName,Type)]
recTypConFields tc = lookupDataCon tc >>= \dat -> case dataCons dat of
  DataRecord fields -> return fields
  _ -> throwError ExpectRecord

-- | Lookup the fields for a given record type.
recTypFields :: MonadEnv m ph
  => Type
  -- ^ The type to lookup.
  -> m [FieldName]
recTypFields (TCon tc) = do
  fields <- recTypConFields $ qualObject tc
  return $ map fst fields
recTypFields (TStruct fs) = return $ map fst fs
recTypFields _ = throwError ExpectRecord

-- | Lookup the record fields and corresponding values from a given expression.
recExpFields :: MonadEnv m ph
  => Expr
  -- ^ The expression to lookup.
  -> m [(FieldName, Expr)]
recExpFields (EVar x) = do
  skols <- getEnv >>= \case
    EnvVG{..} -> return _envvgskol
    EnvCG{..} -> return _envcgskol
    EnvS{..} -> return _envsskol
  let fss = [ fs | SkolRec y fs <- skols, x == y ]
  if not (null fss)
    -- TODO: I would prefer `this.amount` here
    then return $ zip (head fss) (map (EVar . fieldName2VarName) $ head fss)
    else throwError $ UnboundVar x
recExpFields (ERecCon _ fs) = return fs
recExpFields (EStructCon fs) = return fs
recExpFields (ERecUpd _ f recExp fExp) = do
  fs <- recExpFields recExp
  unless (isJust $ find (\(n, _) -> n == f) fs) (throwError $ UnknownRecField f)
  return $ (f, fExp) : [(n, e) | (n, e) <- fs, n /= f]
recExpFields (ERecProj _ f e) = do
  fields <- recExpFields e
  case lookup f fields of
    Just e' -> recExpFields e'
    Nothing -> throwError $ UnknownRecField f
recExpFields (EStructProj f e) = do
  fields <- recExpFields e
  case lookup f fields of
    Just e' -> recExpFields e'
    Nothing -> throwError $ UnknownRecField f
recExpFields _ = throwError ExpectRecord

instance SubstTm BoolExpr where
  substituteTm s (BExpr e) = BExpr (substituteTm s e)
  substituteTm s (BAnd e1 e2) = BAnd (substituteTm s e1) (substituteTm s e2)
  substituteTm s (BNot e) = BNot (substituteTm s e)

instance SubstTm a => SubstTm (Cond a) where
  substituteTm s (Determined x) = Determined $ substituteTm s x
  substituteTm s (Conditional e x y) =
    Conditional (substituteTm s e) (map (substituteTm s) x) (map (substituteTm s) y)

instance SubstTm (UpdateSet ph) where
  substituteTm s UpdateSetVG{..} = UpdateSetVG susUpdate _usvgChoice _usvgValue
    where susUpdate = map (substituteTm s) _usvgUpdate
  substituteTm s UpdateSetCG{..} = UpdateSetCG susUpdate _uscgChoice
    where susUpdate = map (substituteTm s) _uscgUpdate
  substituteTm s UpdateSetS{..} = UpdateSetS susUpdate
    where susUpdate = map (substituteTm s) _ussUpdate

instance SubstTm Upd where
  substituteTm s UpdCreate{..} = UpdCreate _creTemp
    (map (second (substituteTm s)) _creField)
  substituteTm s UpdArchive{..} = UpdArchive _arcTemp
    (map (second (substituteTm s)) _arcField)

-- | Data type representing an error.
data Error
  = UnknownValue (Qualified ExprValName)
  | UnknownDataCons TypeConName
  | UnknownChoice ChoiceName
  | UnboundVar ExprVarName
  | UnknownRecField FieldName
  | UnknownCid Cid
  | UnknownTmpl TypeConName
  | ExpectRecord
  | ExpectCid
  | CyclicModules [ModuleName]

instance Show Error where
  show (UnknownValue qname) = "Impossible: Unknown value definition: "
    ++ (show $ unExprValName $ qualObject qname)
  show (UnknownDataCons tc) = "Impossible: Unknown data constructor: " ++ show tc
  show (UnknownChoice ch) = "Impossible: Unknown choice definition: " ++ show ch
  show (UnboundVar name) = "Impossible: Unbound term variable: " ++ show name
  show (UnknownRecField f) = "Impossible: Unknown record field: " ++ show f
  show (UnknownCid cid) = "Impossible: Unknown contract id: " ++ show cid
  show (UnknownTmpl tem) = "Impossible: Unknown template: " ++ show tem
  show ExpectRecord = "Impossible: Expected a record type"
  show ExpectCid = "Impossible: Expected a contract id"
  show (CyclicModules mods) = "Cyclic modules: " ++ show mods
