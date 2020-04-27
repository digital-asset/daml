-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
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
  , Skolem(..)
  , runEnv
  , genRenamedVar
  , emptyEnv
  , extVarEnv, extRecEnv, extValEnv, extChEnv, extDatsEnv, extCidEnv
  , extRecEnvLvl1
  , lookupVar, lookupRec, lookupVal, lookupChoice, lookupDataCon, lookupCid
  , concatEnv
  , emptyUpdateSet
  , concatUpdateSet
  , solveValueUpdatesEnv
  , lookupChoInHMap
  , fieldName2VarName
  , recTypConFields, recTypFields, recExpFields
  ) where

import Control.Monad.Error.Class (MonadError (..), throwError)
import Control.Monad.State.Lazy
import Data.Hashable
import GHC.Generics
import Data.Maybe (listToMaybe, isJust)
import Data.List (find)
import Data.Bifunctor
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T

import DA.Daml.LF.Ast hiding (lookupChoice)
import DA.Daml.LF.Verify.Subst

-- | Data type denoting a create update.
data UpdCreate = UpdCreate
  { _creTemp  :: !(Qualified TypeConName)
    -- ^ Qualified type constructor corresponding to the contract template.
  , _creField :: ![(FieldName, Expr)]
    -- ^ The fields to be verified, together with their value.
  }
  deriving Show
-- | Data type denoting an archive update.
data UpdArchive = UpdArchive
  { _arcTemp  :: !(Qualified TypeConName)
    -- ^ Qualified type constructor corresponding to the contract template.
  , _arcField :: ![(FieldName, Expr)]
    -- ^ The fields to be verified, together with their value.
  }
  deriving Show
-- | Data type denoting an exercised choice update.
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
    -- Note that this should be empty after the `ValuePhase`.
  }
  deriving Show

-- | Create an empty update set.
emptyUpdateSet :: UpdateSet
emptyUpdateSet = UpdateSet [] [] [] []

-- | Combine two update sets.
concatUpdateSet :: UpdateSet
  -- ^ The first update set to be combined.
  -> UpdateSet
  -- ^ The second update set to be combined.
  -> UpdateSet
concatUpdateSet (UpdateSet cres1 arcs1 chos1 vals1) (UpdateSet cres2 arcs2 chos2 vals2) =
  UpdateSet (cres1 ++ cres2) (arcs1 ++ arcs2) (chos1 ++ chos2) (vals1 ++ vals2)

-- | Refresh a given expression variable by producing a fresh renamed variable.
-- TODO: when a renamed var gets renamed again, it might overlap again.
-- We should have an additional field in VarName to denote it's number.
genRenamedVar :: MonadEnv m
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
expr2cid :: MonadEnv m
  => Expr
  -- ^ The expression to be converted.
  -> m Cid
expr2cid (EVar x) = return $ CidVar x
expr2cid (ERecProj _ f (EVar x)) = return $ CidRec x f
expr2cid _ = throwError ExpectCid

-- TODO: Could we alternatively just declare the variables that occur in the updates and drop the skolems?
-- | The environment for the DAML-LF verifier
data Env = Env
  { _envskol :: ![Skolem]
    -- ^ The skolemised term variables and fields.
  , _envvals :: !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet))
    -- ^ The bound values.
  , _envchs :: !(HM.HashMap (Qualified TypeConName, ChoiceName)
      (ExprVarName,ExprVarName,ExprVarName,Expr -> Expr -> Expr -> UpdateSet))
    -- ^ The set of relevant choices, mapping to functions from self, this and args to its updates.
  , _envdats :: !(HM.HashMap TypeConName DefDataType)
    -- ^ The set of data constructors.
  , _envcids :: !(HM.HashMap Cid ExprVarName)
    -- ^ The set of fetched cid's mapped to their variable name.
  }

-- | Construct an empty environment.
emptyEnv :: Env
emptyEnv = Env [] HM.empty HM.empty HM.empty HM.empty

-- | Combine two environments.
concatEnv :: Env
  -- ^ The first environment to be combined.
  -> Env
  -- ^ The second environment to be combined.
  -> Env
concatEnv (Env vars1 vals1 chs1 dats1 cids1) (Env vars2 vals2 chs2 dats2 cids2) =
  Env (vars1 ++ vars2) (vals1 `HM.union` vals2) (chs1 `HM.union` chs2)
    (dats1 `HM.union` dats2) (cids1 `HM.union` cids2)
  -- TODO: union makes me slightly nervous, as it allows overlapping keys
  -- (and just uses the first). `unionWith concatUpdateSet` would indeed be better,
  -- but this still makes me nervous as the expr and exprvarnames wouldn't be merged.

-- | Convert a fieldname into an expression variable name.
fieldName2VarName :: FieldName -> ExprVarName
fieldName2VarName = ExprVarName . unFieldName

-- | Type class constraint with the required monadic effects for functions
-- manipulating the verification environment.
type MonadEnv m = (MonadError Error m, MonadState (Int,Env) m)

-- | Fetch the current environment.
getEnv :: MonadEnv m => m Env
getEnv = snd <$> get

-- | Set the current environment.
putEnv :: MonadEnv m => Env -> m ()
putEnv env = get >>= \(uni,_) -> put (uni,env)

-- | Generate a new unique name.
fresh :: MonadEnv m => m String
fresh = do
  (cur,env) <- get
  put (cur + 1,env)
  return $ show cur

-- | Evaluate the MonadEnv to produce an error message or the final environment.
runEnv :: StateT (Int,Env) (Either Error) ()
  -- ^ The monadic computation to be evaluated.
  -> Env
  -- ^ The initial environment to start from.
  -> Either Error Env
runEnv comp env0 = do
  (_res, (_uni,env1)) <- runStateT comp (0,env0)
  return env1

-- | Skolemise an expression variable and extend the environment.
extVarEnv :: MonadEnv m
  => ExprVarName
  -- ^ The expression variable to be skolemised.
  -> m ()
extVarEnv x = do
  env@Env{..} <- getEnv
  putEnv env{_envskol = SkolVar x : _envskol}

-- | Skolemise a list of record projection and extend the environment.
extRecEnv :: MonadEnv m
  => ExprVarName
  -- ^ The variable on which is being projected.
  -> [FieldName]
  -- ^ The fields which should be skolemised.
  -> m ()
extRecEnv x fs = do
  env@Env{..} <- getEnv
  putEnv env{_envskol = SkolRec x fs : _envskol}

-- | Extend the environment with a new value definition.
extValEnv :: MonadEnv m
  => Qualified ExprValName
  -- ^ The name of the value being defined.
  -> Expr
  -- ^ The (partially) evaluated value definition.
  -> UpdateSet
  -- ^ The updates performed by this value.
  -> m ()
extValEnv val expr upd = do
  env@Env{..} <- getEnv
  putEnv env{_envvals = HM.insert val (expr, upd) _envvals}

-- | Extends the environment with a new choice.
extChEnv :: MonadEnv m
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
  -> UpdateSet
  -- ^ The updates performed by the new choice.
  -> m ()
extChEnv tc ch self this arg upd = do
  env@Env{..} <- getEnv
  let substUpd sExp tExp aExp = substituteTmUpd (createExprSubst [(self,sExp),(this,tExp),(arg,aExp)]) upd
  putEnv env{_envchs = HM.insert (tc, ch) (self,this,arg,substUpd) _envchs}

-- | Extend the environment with a list of new data type definitions.
extDatsEnv :: MonadEnv m
  => HM.HashMap TypeConName DefDataType
  -- ^ A hashmap of the data constructor names, with their corresponding definitions.
  -> m ()
extDatsEnv hmap = do
  env@Env{..} <- getEnv
  putEnv env{_envdats = hmap `HM.union` _envdats}

-- | Extend the environment with a new contract id, and the variable to which
-- the fetched contract is bound.
extCidEnv :: MonadEnv m
  => Expr
  -- ^ The contract id expression.
  -> ExprVarName
  -- ^ The variable name to which the fetched contract is bound.
  -> m ()
extCidEnv exp var = do
  cid <- expr2cid exp
  env@Env{..} <- getEnv
  putEnv env{_envcids = HM.insert cid var _envcids}

-- TODO: Is one layer of recursion enough?
-- | Recursively skolemise the given record fields, when they have a record
-- type. Note that this only works 1 level deep.
extRecEnvLvl1 :: MonadEnv m
  => [(FieldName, Type)]
  -- ^ The record fields to skolemise, together with their types.
  -> m ()
extRecEnvLvl1 = mapM_ step
  where
    step :: MonadEnv m => (FieldName, Type) -> m ()
    step (f,typ) = do
      { fsRec <- recTypFields typ
      ; extRecEnv (fieldName2VarName f) fsRec
      }
      -- TODO: Temporary fix
      `catchError` (\_ -> return ())

-- | Lookup an expression variable in the environment. Succeeds if this variable
-- has been skolemised, or throws an error if it hasn't.
lookupVar :: MonadEnv m
  => ExprVarName
  -- ^ The expression variable to look up.
  -> m ()
-- TODO: It might be nicer to return a bool here as well?
lookupVar x = getEnv >>= \ env -> unless (elem (SkolVar x) $ _envskol env)
  (throwError $ UnboundVar x)

-- | Lookup a record project in the environment. Returns a boolean denoting
-- whether or not the record projection has been skolemised.
lookupRec :: MonadEnv m
  => ExprVarName
  -- ^ The expression variable on which is being projected.
  -> FieldName
  -- ^ The field name which is being projected.
  -> m Bool
lookupRec x f = do
  env <- getEnv
  let fields = [ fs | SkolRec y fs <- _envskol env , x == y ]
  if not (null fields)
    then return (elem f $ head fields)
    else return False

-- | Lookup a value name in the environment. Returns its (partially) evaluated
-- definition, together with the updates it performs.
lookupVal :: MonadEnv m
  => Qualified ExprValName
  -- ^ The value name to lookup.
  -> m (Expr, UpdateSet)
lookupVal val = do
  env <- getEnv
  case lookupValInHMap (_envvals env) val of
    Just res -> return res
    Nothing -> throwError (UnknownValue val)

-- | Lookup a choice name in the environment. Returns a function which, once
-- self, this and args have been instantiated, returns the set of updates it
-- performs.
lookupChoice :: MonadEnv m
  => Qualified TypeConName
  -- ^ The template name in which this choice is defined.
  -> ChoiceName
  -- ^ The choice name to lookup.
  -> m (Expr -> Expr -> Expr -> UpdateSet)
lookupChoice tem ch = do
  env <- getEnv
  case lookupChoInHMap (_envchs env) (qualObject tem) ch of
    Nothing -> throwError (UnknownChoice ch)
    Just (_,_,_,upd) -> return upd

-- | Lookup a data type definition in the environment.
lookupDataCon :: MonadEnv m
  => TypeConName
  -- ^ The data constructor to lookup.
  -> m DefDataType
lookupDataCon tc = do
  env <- getEnv
  case HM.lookup tc (_envdats env) of
    Nothing -> throwError (UnknownDataCons tc)
    Just def -> return def

-- | Lookup a contract id in the environment. Returns the variable its fetched
-- contract is bound to.
lookupCid :: MonadEnv m
  => Expr
  -- ^ The contract id to lookup.
  -> m ExprVarName
lookupCid exp = do
  env <- getEnv
  cid <- expr2cid exp
  case HM.lookup cid (_envcids env) of
    Nothing -> throwError $ UnknownCid cid
    Just var -> return var

-- TODO: There seems to be something wrong with the PackageRef in Qualified.
-- | Helper function to lookup a value definition in a HashMap.
lookupValInHMap :: HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)
  -- ^ The HashMap in which to look.
  -> Qualified ExprValName
  -- ^ The value name to lookup.
  -> Maybe (Expr, UpdateSet)
lookupValInHMap hmap val = listToMaybe $ HM.elems
  $ HM.filterWithKey (\name _ -> qualObject name == qualObject val && qualModule name == qualModule val) hmap

-- TODO: Does this really need to be a seperate function?
-- | Helper function to lookup a choice in a HashMap. Returns the variables for
-- self, this and args used, as well as a function which, given the values for
-- self, this and args, produces the list of updates performed by this choice.
lookupChoInHMap :: HM.HashMap (Qualified TypeConName, ChoiceName)
    (ExprVarName, ExprVarName, ExprVarName, Expr -> Expr -> Expr -> UpdateSet)
  -- ^ The HashMap in which to look.
  -> TypeConName
  -- ^ The template in which the choice is defined.
  -> ChoiceName
  -- ^ The choice name to lookup.
  -> Maybe (ExprVarName, ExprVarName, ExprVarName, Expr -> Expr -> Expr -> UpdateSet)
-- TODO: This TypeConName should be qualified
-- TODO: The type con name really should be taken into account here
lookupChoInHMap hmap _tem cho = listToMaybe $ HM.elems
  $ HM.filterWithKey (\(_t, c) _ -> c == cho) hmap

-- | Solves the value references by computing the closure of all referenced
-- values, for each value in the environment.
-- It thus empties `_usVal` by collecting all updates made by this closure.
-- TODO: There are undoubtedly more efficient algorithms for computing this.
solveValueUpdatesEnv :: Env -> Env
solveValueUpdatesEnv env =
  let hmap0 = _envvals env
      hmap1 = foldl solveValueUpdate hmap0 (HM.keys hmap0)
  in env{_envvals = hmap1}

-- | Compute the value reference closure for a single value.
solveValueUpdate :: HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)
  -- ^ The HashMap containing the updates for each value.
  -> Qualified ExprValName
  -- ^ The current value to solve.
  -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)
solveValueUpdate hmap0 val0 =
  let (hmap1, _, _) = step (hmap0, emptyUpdateSet, []) val0
  in hmap1
  where
    step :: (HM.HashMap (Qualified ExprValName) (Expr, UpdateSet), UpdateSet, [Qualified ExprValName])
      -> Qualified ExprValName
      -> (HM.HashMap (Qualified ExprValName) (Expr, UpdateSet), UpdateSet, [Qualified ExprValName])
    step inp@(hmap, baseupd, vis) val =
      if val `elem` vis
      -- TODO: Mutual recursion! For now, just ignore...
      then inp
      else case lookupValInHMap hmap val of
        Nothing -> error ("Impossible! Undefined value: " ++ (show $ unExprValName $ qualObject val))
        Just (expr, valupd) ->
          let (nhmap, nvalupd, _) = foldl step (hmap, valupd{_usVal = []}, val:vis) (_usVal valupd)
          in (HM.insert val (expr, nvalupd) nhmap, concatUpdateSet baseupd nvalupd, val:vis)

-- TODO: This should work recursively
-- | Lookup the field names and corresponding types, for a given record type
-- constructor name.
recTypConFields :: MonadEnv m
  => TypeConName
  -- ^ The record type constructor name to lookup.
  -> m [(FieldName,Type)]
recTypConFields tc = lookupDataCon tc >>= \dat -> case dataCons dat of
  DataRecord fields -> return fields
  _ -> throwError ExpectRecord

-- | Lookup the fields for a given record type.
recTypFields :: MonadEnv m
  => Type
  -- ^ The type to lookup.
  -> m [FieldName]
recTypFields (TCon tc) = do
  fields <- recTypConFields $ qualObject tc
  return $ map fst fields
recTypFields (TStruct fs) = return $ map fst fs
recTypFields _ = throwError ExpectRecord

-- | Lookup the record fields and corresponding values from a given expression.
recExpFields :: MonadEnv m
  => Expr
  -- ^ The expression to lookup.
  -> m [(FieldName, Expr)]
recExpFields (EVar x) = do
  env <- getEnv
  let fss = [ fs | SkolRec y fs <- _envskol env , x == y ]
  if not (null fss)
    -- TODO: I would prefer `this.amount` here
    then return $ zip (head fss) (map (EVar . fieldName2VarName) $ head fss)
    else throwError $ UnboundVar x
recExpFields (ERecCon _ fs) = return fs
recExpFields (ERecUpd _ f recExp fExp) = do
  fs <- recExpFields recExp
  unless (isJust $ find (\(n, _) -> n == f) fs) (throwError $ UnknownRecField f)
  return $ (f, fExp) : [(n, e) | (n, e) <- fs, n /= f]
recExpFields (ERecProj _ f e) = do
  fields <- recExpFields e
  case lookup f fields of
    Just e' -> recExpFields e'
    Nothing -> throwError $ UnknownRecField f
recExpFields _ = throwError ExpectRecord

-- | Apply an expression substitution to an update set.
substituteTmUpd :: ExprSubst
  -- ^ The substitution to apply.
  -> UpdateSet
  -- ^ The update set on which to apply the substitution.
  -> UpdateSet
substituteTmUpd s UpdateSet{..} = UpdateSet susCre susArc _usCho _usVal
  where
    susCre = map (substituteTmUpdCreate s) _usCre
    susArc = map (substituteTmUpdArchive s) _usArc

-- | Apply an expression substitution to a create update.
substituteTmUpdCreate :: ExprSubst
  -- ^ The substitution to apply.
  -> UpdCreate
  -- ^ The create update on which to apply the substitution.
  -> UpdCreate
substituteTmUpdCreate s UpdCreate{..} = UpdCreate _creTemp
  (map (second (substituteTmTm s)) _creField)

-- | Apply an expression substitution to an archive update.
substituteTmUpdArchive :: ExprSubst
  -- ^ The substitution to apply.
  -> UpdArchive
  -- ^ The archive update on which to apply the substitution.
  -> UpdArchive
substituteTmUpdArchive s UpdArchive{..} = UpdArchive _arcTemp
  (map (second (substituteTmTm s)) _arcField)

-- | Data type representing an error.
data Error
  = UnknownValue (Qualified ExprValName)
  | UnknownDataCons TypeConName
  | UnknownChoice ChoiceName
  | UnboundVar ExprVarName
  | UnknownRecField FieldName
  | UnknownCid Cid
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
  show ExpectRecord = "Impossible: Expected a record type"
  show ExpectCid = "Impossible: Expected a contract id"
  show (CyclicModules mods) = "Cyclic modules: " ++ show mods
