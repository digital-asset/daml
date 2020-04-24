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
  , emptyEnv
  , introEnv
  , extVarEnv, extRecEnv, extValEnv, extChEnv, extDatsEnv, extCidEnv
  , extRecEnvLvl1
  , lookupVar, lookupRec, lookupVal, lookupChoice, lookupDataCon, lookupCid
  , concatEnv
  , emptyUpdateSet
  , concatUpdateSet
  , solveValueUpdatesEnv
  , testPrint, lookupChoInHMap
  , fieldName2VarName
  , recTypConFields, recTypFields, recExpFields
  ) where

import Control.Monad.Error.Class (MonadError (..), throwError)
import Control.Monad.State.Lazy
import Data.Hashable
import GHC.Generics
import Data.Maybe (listToMaybe, isJust)
import Data.List (find)
import qualified Data.Map.Strict as Map
import qualified Data.HashMap.Strict as HM
import Debug.Trace

import DA.Daml.LF.Ast hiding (lookupChoice)
import DA.Daml.LF.Verify.Subst

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

data Skolem
  = SkolVar ExprVarName
    -- ^ Skolemised term variable.
  | SkolRec ExprVarName [FieldName]
    -- ^ List of skolemised field names, with their variable.
    -- e.g. `this.field`
  deriving (Eq, Show)

expr2cid :: MonadEnv m => Expr -> m Cid
expr2cid (EVar x) = return $ CidVar x
expr2cid (ERecProj _ f (EVar x)) = return $ CidRec x f
-- TODO: Temporary solution
expr2cid (ERecProj _ f (ERecCon tc _)) = return $ CidRec (ExprVarName $ head $ unTypeConName $ qualObject $ tcaTypeCon tc) f
expr2cid expr = error ("Expected cid: " ++ show expr)
-- expr2cid _ = throwError ExpectCid

data Cid
  = CidVar ExprVarName
  | CidRec ExprVarName FieldName
  deriving (Generic, Hashable, Eq, Show)

-- | The environment for the DAML-LF verifier
data Env = Env
  { _envskol :: ![Skolem]
    -- ^ The skolemised term variables and fields.
  , _envvals :: !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet))
    -- ^ The bound values.
  , _envchs :: !(HM.HashMap (Qualified TypeConName, ChoiceName) (Expr -> Expr -> Expr -> UpdateSet))
    -- ^ The set of relevant choices, mapping to functions from self, this and args to its updates.
  , _envdats :: !(HM.HashMap TypeConName DefDataType)
    -- ^ The set of data constructors.
  , _envcids :: !(HM.HashMap Cid ExprVarName)
    -- ^ The set of fetched cid's mapped to their variable name.
  -- TODO: split this off into data types for readability?
  }

emptyEnv :: Env
emptyEnv = Env [] HM.empty HM.empty HM.empty HM.empty

concatEnv :: Env -> Env -> Env
concatEnv (Env vars1 vals1 chs1 dats1 cids1) (Env vars2 vals2 chs2 dats2 cids2) =
  Env (vars1 ++ vars2) (vals1 `HM.union` vals2) (chs1 `HM.union` chs2)
    (dats1 `HM.union` dats2) (cids1 `HM.union` cids2)
  -- TODO: union makes me slightly nervous, as it allows overlapping keys
  -- (and just uses the first)

fieldName2VarName :: FieldName -> ExprVarName
fieldName2VarName = ExprVarName . unFieldName

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
-- TODO: Check for doubles when adding
extVarEnv x = get >>= \env@Env{..} -> put env{_envskol = SkolVar x : _envskol}

-- TODO: Problem! ExprVarName is always `this` or `arg`, so it overwrites!!
-- Possible solution: pass in the current template?
extRecEnv :: MonadEnv m => ExprVarName -> [FieldName] -> m ()
-- TODO: Check for doubles when adding
extRecEnv x fs = get >>= \env@Env{..} -> put env{_envskol = SkolRec x fs : _envskol}

extValEnv :: MonadEnv m => Qualified ExprValName -> Expr -> UpdateSet -> m ()
extValEnv val expr upd = get >>= \env@Env{..} -> put env{_envvals = HM.insert val (expr, upd) _envvals}

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
  env@Env{..} <- get
  let substUpd = \sExp tExp aExp -> trace ("Subst " ++ (show arg) ++ "with" ++ (show aExp)) $ substituteTmUpd (Map.fromList [(self,sExp),(this,tExp),(arg,aExp)]) upd
  put env{_envchs = HM.insert (tc, ch) substUpd _envchs}

extDatsEnv :: MonadEnv m => HM.HashMap TypeConName DefDataType -> m ()
extDatsEnv hmap = get >>= \env@Env{..} -> put env{_envdats = hmap `HM.union` _envdats}

extCidEnv :: MonadEnv m => Expr -> ExprVarName -> m ()
extCidEnv exp var = do
  cid <- expr2cid exp
  env@Env{..} <- get
  put env{_envcids = HM.insert cid var _envcids}

-- TODO: Is one layer of recursion enough?
extRecEnvLvl1 :: MonadEnv m => [(FieldName, Type)] -> m ()
extRecEnvLvl1 = mapM_ step
  where
    step :: MonadEnv m => (FieldName, Type) -> m ()
    step (f,typ) = do
      { fsRec <- recTypFields typ
      ; extRecEnv (fieldName2VarName f) fsRec
      }
      -- TODO: Temporary fix
      `catchError` (\_ -> return ())

lookupVar :: MonadEnv m => ExprVarName -> m ()
lookupVar (ExprVarName "self") = return ()
lookupVar (ExprVarName "this") = return ()
-- TODO: Is there a nicer way to handle this instead of hardcoding?
lookupVar x = get >>= \ env -> unless (elem (SkolVar x) $ _envskol env)
  (throwError $ UnboundVar x)

lookupRec :: MonadEnv m => ExprVarName -> FieldName -> m Bool
lookupRec x f = do
  env <- get
  let fields = [ fs | SkolRec y fs <- _envskol env , x == y ]
  if not (null fields)
    then return (elem f $ head fields)
    else return False

lookupVal :: MonadEnv m => Qualified ExprValName -> m (Expr, UpdateSet)
lookupVal val = do
  env <- get
  case lookupValInHMap (_envvals env) val of
    Just res -> return res
    Nothing -> throwError (UnknownValue val)

lookupChoice :: MonadEnv m => Qualified TypeConName -> ChoiceName
  -> m (Expr -> Expr -> Expr -> UpdateSet)
-- TODO: Actually bind this, instead of hardcoding
-- lookupChoice tem (ChoiceName "Archive") = do
--   fs <- recTypConFields $ qualObject tem
--   let fields = map ((\f -> (f, EVar $ fieldName2VarName f)) . fst) fs
--   return emptyUpdateSet{_usArc = [UpdArchive tem fields]}
lookupChoice tem ch = do
  env <- get
  case lookupChoInHMap (_envchs env) (qualObject tem) ch of
    Nothing -> throwError (UnknownChoice ch)
    Just upd -> return upd

lookupDataCon :: MonadEnv m => TypeConName -> m DefDataType
lookupDataCon tc = do
  env <- get
  case HM.lookup tc (_envdats env) of
    Nothing -> throwError (UnknownDataCons tc)
    Just def -> return def

lookupCid :: MonadEnv m => Expr -> m ExprVarName
lookupCid exp = do
  env <- get
  cid <- expr2cid exp
  case HM.lookup cid (_envcids env) of
    Nothing -> trace (show $ _envcids env) $ throwError $ UnknownCid cid
    Just var -> return var

lookupValInHMap :: HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)
  -> Qualified ExprValName -> Maybe (Expr, UpdateSet)
-- TODO: Temp print
lookupValInHMap hmap val =
  -- TODO: There seems to be something wrong with the PackageRef in Qualified.
  let ress = HM.elems $ HM.filterWithKey (\name _ -> qualObject name == qualObject val && qualModule name == qualModule val) hmap
  in listToMaybe ress
  -- in if length ress > 1
  --    then trace ("Overlapping vals! : " ++ show val) $ listToMaybe ress
  --    else listToMaybe ress

-- TODO: Does this really need to be a seperate function?
lookupChoInHMap :: HM.HashMap (Qualified TypeConName, ChoiceName) (Expr -> Expr -> Expr -> UpdateSet)
  -> TypeConName -> ChoiceName -> Maybe (Expr -> Expr -> Expr -> UpdateSet)
-- TODO: This TypeConName should be qualified
-- TODO: The type con name really should be taken into account here
lookupChoInHMap hmap _tem cho = listToMaybe $ HM.elems
  $ HM.filterWithKey (\(_t, c) _ -> c == cho) hmap

solveValueUpdatesEnv :: Env -> Env
solveValueUpdatesEnv env =
  let hmap0 = _envvals env
      hmap1 = foldl solveValueUpdate hmap0 (HM.keys hmap0)
  in env{_envvals = hmap1}

solveValueUpdate :: HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)
  -> Qualified ExprValName -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet)
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
recTypConFields :: MonadEnv m => TypeConName -> m [(FieldName,Type)]
recTypConFields tc = lookupDataCon tc >>= \dat -> case dataCons dat of
  DataRecord fields -> return fields
  _ -> throwError ExpectRecord

recTypFields :: MonadEnv m => Type -> m [FieldName]
recTypFields (TCon tc) = do
  fields <- recTypConFields $ qualObject tc
  return $ map fst fields
recTypFields (TStruct fs) = return $ map fst fs
recTypFields _ = throwError ExpectRecord

recExpFields :: MonadEnv m => Expr -> m [(FieldName, Expr)]
recExpFields (EVar x) = do
  env <- get
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
-- recExpFields _ = throwError ExpectRecord
recExpFields (ERecProj _ f e) = do
  fields <- recExpFields e
  case lookup f fields of
    Just e' -> recExpFields e'
    Nothing -> throwError $ UnknownRecField f
recExpFields e = error ("Expected record: " ++ show e)

substituteTmUpd :: ExprSubst -> UpdateSet -> UpdateSet
substituteTmUpd s UpdateSet{..} = UpdateSet susCre susArc _usCho _usVal
  where
    susCre = map (substituteTmUpdCreate s) _usCre
    susArc = map (substituteTmUpdArchive s) _usArc

substituteTmUpdCreate :: ExprSubst -> UpdCreate -> UpdCreate
substituteTmUpdCreate s UpdCreate{..} = UpdCreate _creTemp
  (map (\(f,e) -> (f, substituteTmTm s e)) _creField)

substituteTmUpdArchive :: ExprSubst -> UpdArchive -> UpdArchive
substituteTmUpdArchive s UpdArchive{..} = UpdArchive _arcTemp
  (map (\(f,e) -> (f, substituteTmTm s e)) _arcField)

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

-- | For testing purposes: print the stored value definitions in the environment.
-- TODO: Remove
testPrint :: HM.HashMap (Qualified ExprValName) (Expr, UpdateSet) -> IO ()
testPrint hmap = putStrLn "Test print keys:"
  >> mapM_ print (HM.keys hmap)
