-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}

-- | Contexts for DAML LF static verification
module DA.Daml.LF.Verify.Context
  ( Phase(..)
  , IsPhase(..)
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
  , getEnv, putEnv
  , runEnv
  , genRenamedVar
  , createCond
  , addUpd
  , extVarEnv, extRecEnv, extRecEnvTCons, extValEnv, extChEnv, extDatsEnv
  , extCidEnv, extPrecond, extCtrRec, extCtr
  , lookupVar, lookupRec, lookupVal, lookupChoice, lookupDataCon, lookupCid
  , conditionalUpdateSet
  , fieldName2VarName
  , recTypConFields, recTypFields, recExpFields
  ) where

import Control.Monad.Error.Class (MonadError (..), throwError)
import Control.Monad.State.Lazy
import Data.Hashable
import GHC.Generics
import Data.Maybe (isJust)
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
  | BEq Expr Expr
  -- ^ Equality operator.
  | BGt Expr Expr
  -- ^ Greater than operator.
  | BGtE Expr Expr
  -- ^ Greater than or equal operator.
  | BLt Expr Expr
  -- ^ Less than operator.
  | BLtE Expr Expr
  -- ^ Less than or equal operator.
  deriving Show

-- | Convert an expression constraint into boolean expressions.
toBoolExpr :: Expr -> [BoolExpr]
toBoolExpr (EBuiltin (BEBool True)) = []
toBoolExpr (ETmApp (ETmApp op e1) e2) = case op of
  (EBuiltin (BEEqual _)) -> [BEq e1 e2]
  (ETyApp (EBuiltin BEGreaterNumeric) _) -> [BGt e1 e2]
  (ETyApp (EBuiltin BEGreaterEqNumeric) _) -> [BGtE e1 e2]
  (ETyApp (EBuiltin BELessNumeric) _) -> [BLt e1 e2]
  (ETyApp (EBuiltin BELessEqNumeric) _) -> [BLtE e1 e2]
  _ -> trace ("Unmatched Expr to BoolExpr Operator: " ++ show op) []
toBoolExpr exp = trace ("Unmatched Expr to BoolExpr: " ++ show exp) []

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

-- | Construct a single conditional, if the input is not empty.
-- Helper function for `introCond`.
buildCond :: IsPhase ph
  => BoolExpr
  -- ^ The condition.
  -> [Cond (UpdateSet ph)]
  -- ^ The input for the true case.
  -> [Cond (UpdateSet ph)]
  -- ^ The input for the false case.
  -> (UpdateSet ph -> [Cond a])
  -- ^ The fetch function.
  -> [Cond a]
buildCond e updx updy get =
  let xs = concatCond updx get
      ys = concatCond updy get
  in [Conditional e xs ys | not (null xs && null ys)]

-- | Fetch the conditionals from the conditional update set, and flatten the
-- two layers into one.
concatCond :: IsPhase ph
  => [Cond (UpdateSet ph)]
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

-- | Class containing the environment, and operations on it, for each generator phase.
class IsPhase (ph :: Phase) where
  -- | The collection of updates being performed.
  data UpdateSet ph
  -- TODO: Could we alternatively just declare the variables that occur in the updates and drop the skolems?
  -- | The environment for the DAML-LF verifier.
  data Env ph
  -- | Construct an empty update set.
  emptyUpdateSet :: UpdateSet ph
  -- | Combine two update sets.
  concatUpdateSet :: UpdateSet ph -> UpdateSet ph -> UpdateSet ph
  -- | Shift the conditional inside the update set.
  introCond :: Cond (UpdateSet ph) -> UpdateSet ph
  -- | Get the list of updates from an update set.
  updSetUpdates :: UpdateSet ph -> [Cond Upd]
  -- | Update the list of updates in an update set.
  setUpdSetUpdates :: [Cond Upd] -> UpdateSet ph -> UpdateSet ph
  -- | Get the list of exercised choices from an update set.
  updSetChoices :: UpdateSet ph -> [Cond UpdChoice]
  -- | Update the list of exercised choices in an update set.
  setUpdSetChoices :: [Cond UpdChoice] -> UpdateSet ph -> UpdateSet ph
  -- | Get the list of referenced values from an update set.
  updSetValues :: UpdateSet ph -> [Cond (Qualified ExprValName)]
  -- | Update the list of referenced values from an update set.
  setUpdSetValues :: [Cond (Qualified ExprValName)] -> UpdateSet ph -> UpdateSet ph
  -- | Construct an empty environment.
  emptyEnv :: Env ph
  -- | Combine two environments.
  concatEnv :: Env ph -> Env ph -> Env ph
  -- | Get the skolemised term variables and fields from the environment.
  envSkols :: Env ph -> [Skolem]
  -- | Update the skolemised term variables and fields in the environment.
  setEnvSkols :: [Skolem] -> Env ph -> Env ph
  -- | Get the bound values from the environment.
  envVals :: Env ph -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet ph)
  -- | Update the bound values in the environment.
  setEnvVals :: HM.HashMap (Qualified ExprValName) (Expr, UpdateSet ph) -> Env ph -> Env ph
  -- | Get the data constructors from the environment.
  envDats :: Env ph -> HM.HashMap TypeConName DefDataType
  -- | Update the data constructors in the environment.
  setEnvDats :: HM.HashMap TypeConName DefDataType -> Env ph -> Env ph
  -- | Get the fetched cid's mapped to their current variable name, along with
  -- a list of any potential old variable names, from the environment.
  envCids :: Env ph -> HM.HashMap Cid (ExprVarName, [ExprVarName])
  -- | Update the fetched cid's in the environment.
  setEnvCids :: HM.HashMap Cid (ExprVarName, [ExprVarName]) -> Env ph -> Env ph
  -- | Get the set of preconditions from the environment.
  envPreconds :: Env ph -> HM.HashMap (Qualified TypeConName) (Expr -> Expr)
  -- | Update the set of preconditions in the environment.
  setEnvPreconds :: HM.HashMap (Qualified TypeConName) (Expr -> Expr) -> Env ph -> Env ph
  -- | Get the additional constraints from the environment.
  envCtrs :: Env ph -> [BoolExpr]
  -- | Update the additional constraints in the environment.
  setEnvCtrs :: [BoolExpr] -> Env ph -> Env ph
  -- | Get the set of relevant choices from the environment.
  envChoices :: Env ph -> HM.HashMap UpdChoice (ChoiceData ph)
  -- | Update the set of relevant choices in the environment.
  setEnvChoices :: HM.HashMap UpdChoice (ChoiceData ph) -> Env ph -> Env ph

instance IsPhase 'ValueGathering where
  data UpdateSet 'ValueGathering = UpdateSetVG
    ![Cond Upd]
    -- ^ The list of updates.
    ![Cond UpdChoice]
    -- ^ The list of exercised choices.
    ![Cond (Qualified ExprValName)]
    -- ^ The list of referenced values.
  data Env 'ValueGathering = EnvVG
    ![Skolem]
    -- ^ The skolemised term variables and fields.
    !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering))
    -- ^ The bound values.
    !(HM.HashMap TypeConName DefDataType)
    -- ^ The set of data constructors.
    !(HM.HashMap Cid (ExprVarName, [ExprVarName]))
    -- ^ The set of fetched cid's mapped to their current variable name, along
    -- with a list of any potential old variable names.
    ![BoolExpr]
    -- ^ Additional constraints.
  emptyUpdateSet = UpdateSetVG [] [] []
  concatUpdateSet (UpdateSetVG upd1 cho1 val1) (UpdateSetVG upd2 cho2 val2) =
    UpdateSetVG (upd1 ++ upd2) (cho1 ++ cho2) (val1 ++ val2)
  introCond (Determined upds) = upds
  introCond (Conditional e updx updy) = UpdateSetVG
    (buildCond e updx updy updSetUpdates)
    (buildCond e updx updy updSetChoices)
    (buildCond e updx updy updSetValues)
  updSetUpdates (UpdateSetVG upd _ _) = upd
  setUpdSetUpdates upd (UpdateSetVG _ cho val) = UpdateSetVG upd cho val
  updSetChoices (UpdateSetVG _ cho _) = cho
  setUpdSetChoices cho (UpdateSetVG upd _ val) = UpdateSetVG upd cho val
  updSetValues (UpdateSetVG _ _ val) = val
  setUpdSetValues val (UpdateSetVG upd cho _) = UpdateSetVG upd cho val
  emptyEnv = EnvVG [] HM.empty HM.empty HM.empty []
  concatEnv (EnvVG vars1 vals1 dats1 cids1 ctrs1) (EnvVG vars2 vals2 dats2 cids2 ctrs2) =
    EnvVG (vars1 ++ vars2) (vals1 `HM.union` vals2) (dats1 `HM.union` dats2)
      (cids1 `HM.union` cids2) (ctrs1 ++ ctrs2)
  -- TODO: union makes me slightly nervous, as it allows overlapping keys
  -- (and just uses the first). `unionWith concatUpdateSet` would indeed be better,
  -- but this still makes me nervous as the expr and exprvarnames wouldn't be merged.
  envSkols (EnvVG sko _ _ _ _) = sko
  setEnvSkols sko (EnvVG _ val dat cid ctr) = EnvVG sko val dat cid ctr
  envVals (EnvVG _ val _ _ _) = val
  setEnvVals val (EnvVG sko _ dat cid ctr) = EnvVG sko val dat cid ctr
  envDats (EnvVG _ _ dat _ _) = dat
  setEnvDats dat (EnvVG sko val _ cid ctr) = EnvVG sko val dat cid ctr
  envCids (EnvVG _ _ _ cid _) = cid
  setEnvCids cid (EnvVG sko val dat _ ctr) = EnvVG sko val dat cid ctr
  envPreconds _ = error "A value gathering phase environment does not contain preconditions."
  setEnvPreconds _ _ = error "A value gathering phase environment does not contain preconditions."
  envCtrs (EnvVG _ _ _ _ ctr) = ctr
  setEnvCtrs ctr (EnvVG sko val dat cid _) = EnvVG sko val dat cid ctr
  envChoices _ = error "A value gathering phase environment does not contain choices."
  setEnvChoices _ _ = error "A value gathering phase environment does not contain choices."

instance IsPhase 'ChoiceGathering where
  data UpdateSet 'ChoiceGathering = UpdateSetCG
    ![Cond Upd]
    -- ^ The list of updates.
    ![Cond UpdChoice]
    -- ^ The list of exercised choices.
  data Env 'ChoiceGathering = EnvCG
    ![Skolem]
    -- ^ The skolemised term variables and fields.
    !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ChoiceGathering))
    -- ^ The bound values.
    !(HM.HashMap TypeConName DefDataType)
    -- ^ The set of data constructors.
    !(HM.HashMap Cid (ExprVarName, [ExprVarName]))
    -- ^ The set of fetched cid's mapped to their current variable name, along
    -- with a list of any potential old variable names.
    !(HM.HashMap (Qualified TypeConName) (Expr -> Expr))
    -- ^ The set of preconditions per template. The precondition is represented
    -- using a function from the `this` variable to the constraint expression.
    ![BoolExpr]
    -- ^ Additional constraints.
    !(HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering))
    -- ^ The set of relevant choices.
  emptyUpdateSet = UpdateSetCG [] []
  concatUpdateSet (UpdateSetCG upd1 cho1) (UpdateSetCG upd2 cho2) =
    UpdateSetCG (upd1 ++ upd2) (cho1 ++ cho2)
  introCond (Determined upds) = upds
  introCond (Conditional e updx updy) = UpdateSetCG
    (buildCond e updx updy updSetUpdates)
    (buildCond e updx updy updSetChoices)
  updSetUpdates (UpdateSetCG upd _) = upd
  setUpdSetUpdates upd (UpdateSetCG _ cho) = UpdateSetCG upd cho
  updSetChoices (UpdateSetCG _ cho) = cho
  setUpdSetChoices cho (UpdateSetCG upd _) = UpdateSetCG upd cho
  updSetValues _ = error "A choice gathering update set does not contain value references."
  setUpdSetValues _ _ = error "A choice gathering update set does not contain value references."
  emptyEnv = EnvCG [] HM.empty HM.empty HM.empty HM.empty [] HM.empty
  concatEnv (EnvCG vars1 vals1 dats1 cids1 pres1 ctrs1 chos1) (EnvCG vars2 vals2 dats2 cids2 pres2 ctrs2 chos2) =
    EnvCG (vars1 ++ vars2) (vals1 `HM.union` vals2) (dats1 `HM.union` dats2)
      (cids1 `HM.union` cids2) (pres1 `HM.union` pres2) (ctrs1 ++ ctrs2)
      (chos1 `HM.union` chos2)
  envSkols (EnvCG sko _ _ _ _ _ _) = sko
  setEnvSkols sko (EnvCG _ val dat cid pre ctr cho) = EnvCG sko val dat cid pre ctr cho
  envVals (EnvCG _ val _ _ _ _ _) = val
  setEnvVals val (EnvCG sko _ dat cid pre ctr cho) = EnvCG sko val dat cid pre ctr cho
  envDats (EnvCG _ _ dat _ _ _ _) = dat
  setEnvDats dat (EnvCG sko val _ cid pre ctr cho) = EnvCG sko val dat cid pre ctr cho
  envCids (EnvCG _ _ _ cid _ _ _) = cid
  setEnvCids cid (EnvCG sko val dat _ pre ctr cho) = EnvCG sko val dat cid pre ctr cho
  envPreconds (EnvCG _ _ _ _ pre _ _) = pre
  setEnvPreconds pre (EnvCG sko val dat cid _ ctr cho) = EnvCG sko val dat cid pre ctr cho
  envCtrs (EnvCG _ _ _ _ _ ctr _) = ctr
  setEnvCtrs ctr (EnvCG sko val dat cid pre _ cho) = EnvCG sko val dat cid pre ctr cho
  envChoices (EnvCG _ _ _ _ _ _ cho) = cho
  setEnvChoices cho (EnvCG sko val dat cid pre ctr _) = EnvCG sko val dat cid pre ctr cho

instance IsPhase 'Solving where
  data UpdateSet 'Solving = UpdateSetS
    ![Cond Upd]
    -- ^ The list of updates.
  data Env 'Solving = EnvS
    ![Skolem]
    -- ^ The skolemised term variables and fields.
    !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'Solving))
    -- ^ The bound values.
    !(HM.HashMap TypeConName DefDataType)
    -- ^ The set of data constructors.
    !(HM.HashMap Cid (ExprVarName, [ExprVarName]))
    -- ^ The set of fetched cid's mapped to their current variable name, along
    -- with a list of any potential old variable names.
    !(HM.HashMap (Qualified TypeConName) (Expr -> Expr))
    -- ^ The set of preconditions per template. The precondition is represented
    -- using a function from the `this` variable to the constraint expression.
    ![BoolExpr]
    -- ^ Additional constraints.
    !(HM.HashMap UpdChoice (ChoiceData 'Solving))
    -- ^ The set of relevant choices.
  emptyUpdateSet = UpdateSetS []
  concatUpdateSet (UpdateSetS upd1) (UpdateSetS upd2) =
    UpdateSetS (upd1 ++ upd2)
  introCond (Determined upds) = upds
  introCond (Conditional e updx updy) = UpdateSetS
    (buildCond e updx updy updSetUpdates)
  updSetUpdates (UpdateSetS upd) = upd
  setUpdSetUpdates upd (UpdateSetS _) = UpdateSetS upd
  updSetChoices _ = error "A solving update set does not contain choice references."
  setUpdSetChoices _ _ = error "A solving update set does not contain choice references."
  updSetValues _ = error "A solving update set does not contain value references."
  setUpdSetValues _ _ = error "A solving update set does not contain value references."
  emptyEnv = EnvS [] HM.empty HM.empty HM.empty HM.empty [] HM.empty
  concatEnv (EnvS vars1 vals1 dats1 cids1 pres1 ctrs1 chos1) (EnvS vars2 vals2 dats2 cids2 pres2 ctrs2 chos2) =
    EnvS (vars1 ++ vars2) (vals1 `HM.union` vals2) (dats1 `HM.union` dats2)
      (cids1 `HM.union` cids2) (pres1 `HM.union` pres2) (ctrs1 ++ ctrs2)
      (chos1 `HM.union` chos2)
  envSkols (EnvS sko _ _ _ _ _ _) = sko
  setEnvSkols sko (EnvS _ val dat cid pre ctr cho) = EnvS sko val dat cid pre ctr cho
  envVals (EnvS _ val _ _ _ _ _) = val
  setEnvVals val (EnvS sko _ dat cid pre ctr cho) = EnvS sko val dat cid pre ctr cho
  envDats (EnvS _ _ dat _ _ _ _) = dat
  setEnvDats dat (EnvS sko val _ cid pre ctr cho) = EnvS sko val dat cid pre ctr cho
  envCids (EnvS _ _ _ cid _ _ _) = cid
  setEnvCids cid (EnvS sko val dat _ pre ctr cho) = EnvS sko val dat cid pre ctr cho
  envPreconds (EnvS _ _ _ _ pre _ _) = pre
  setEnvPreconds pre (EnvS sko val dat cid _ ctr cho) = EnvS sko val dat cid pre ctr cho
  envCtrs (EnvS _ _ _ _ _ ctr _) = ctr
  setEnvCtrs ctr (EnvS sko val dat cid pre _ cho) = EnvS sko val dat cid pre ctr cho
  envChoices (EnvS _ _ _ _ _ _ cho) = cho
  setEnvChoices cho (EnvS sko val dat cid pre ctr _) = EnvS sko val dat cid pre ctr cho

-- | Add a single Upd to an UpdateSet
addUpd :: IsPhase ph
  => UpdateSet ph
  -- ^ The update set to extend.
  -> Upd
  -- ^ The update to add.
  -> UpdateSet ph
addUpd upds upd = setUpdSetUpdates (Determined upd : updSetUpdates upds) upds

-- | Make an update set conditional. A second update set can also be introduced
-- for the case where the condition does not hold.
conditionalUpdateSet :: IsPhase ph
  => Expr
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
-- The function can optionally refresh the contract id, and returns both the
-- converted contract id, and the substitution originating from the refresh.
expr2cid :: (IsPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Whether or not the refresh the contract id.
  -> Expr
  -- ^ The expression to be converted.
  -> m (Cid, ExprSubst)
expr2cid b (EVar x) = do
  (y, subst) <- refresh_cid b x
  return (CidVar y, subst)
expr2cid b (ERecProj _ f (EVar x)) = do
  (y, subst) <- refresh_cid b x
  extRecEnv y [f]
  return (CidRec y f, subst)
expr2cid b (EStructProj f (EVar x)) = do
  (y, subst) <- refresh_cid b x
  extRecEnv y [f]
  return (CidRec y f, subst)
expr2cid _ _ = throwError ExpectCid

-- | Internal function, for refreshing a given contract id.
refresh_cid :: MonadEnv m ph
  => Bool
  -- ^ Whether or not the refresh the contract id.
  -> ExprVarName
  -- ^ The variable name to refresh.
  -> m (ExprVarName, ExprSubst)
refresh_cid False x = return (x, emptyExprSubst)
refresh_cid True x = do
  y <- genRenamedVar x
  return (y, singleExprSubst x (EVar y))

-- | Data type containing the data stored for a choice definition.
data ChoiceData (ph :: Phase) = ChoiceData
  { _cdSelf :: ExprVarName
    -- ^ The variable denoting `self`.
  , _cdThis :: ExprVarName
    -- ^ The variable denoting `this`.
  , _cdArgs :: ExprVarName
    -- ^ The variable denoting `args`.
  , _cdUpds :: UpdateSet ph
    -- ^ The updates performed by this choice. Note that this update set depends
    -- on the above variables.
  , _cdType :: Type
    -- ^ The return type of this choice.
  }

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
  return $ "_" ++ show cur

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
extVarEnv :: (IsPhase ph, MonadEnv m ph)
  => ExprVarName
  -- ^ The expression variable to be skolemised.
  -> m ()
extVarEnv x = extSkolEnv (SkolVar x)

-- | Skolemise a list of record projection and extend the environment.
extRecEnv :: (IsPhase ph, MonadEnv m ph)
  => ExprVarName
  -- ^ The variable on which is being projected.
  -> [FieldName]
  -- ^ The fields which should be skolemised.
  -> m ()
extRecEnv x fs = do
  env <- getEnv
  let skols = envSkols env
      -- TODO: avoid duplicates
      curFs = [fs' | SkolRec x' fs' <- skols, x == x']
      newFs = if null curFs
        then fs
        else fs ++ head curFs
  extSkolEnv (SkolRec x newFs)

-- | Extend the environment with the fields of any given record or type
-- constructor type.
extRecEnvTCons :: (IsPhase ph, MonadEnv m ph)
  => [(FieldName, Type)]
  -- ^ The given fields and their corresponding types to analyse.
  -> m ()
extRecEnvTCons = mapM_ step
  where
    step :: (IsPhase ph, MonadEnv m ph) => (FieldName, Type) -> m ()
    step (f,typ) =
      recTypFields typ >>= \case
        Nothing -> return ()
        Just fsRec -> do
          extRecEnv (fieldName2VarName f) $ map fst fsRec

-- | Extend the environment with a new skolem variable.
-- TODO: Avoid duplicates.
extSkolEnv :: (IsPhase ph, MonadEnv m ph)
  => Skolem
  -- ^ The skolem variable to add.
  -> m ()
extSkolEnv skol = do
  env <- getEnv
  putEnv $ setEnvSkols (skol : envSkols env) env

-- | Extend the environment with a new value definition.
extValEnv :: (IsPhase ph, MonadEnv m ph)
  => Qualified ExprValName
  -- ^ The name of the value being defined.
  -> Expr
  -- ^ The (partially) evaluated value definition.
  -> UpdateSet ph
  -- ^ The updates performed by this value.
  -> m ()
extValEnv val expr upd = do
  env <- getEnv
  putEnv $ setEnvVals (HM.insert val (expr, upd) $ envVals env) env

-- | Extends the environment with a new choice.
extChEnv :: (IsPhase ph, MonadEnv m ph)
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
extChEnv tc ch self this arg upd typ = do
  env <- getEnv
  putEnv $ setEnvChoices (HM.insert (UpdChoice tc ch) (ChoiceData self this arg upd typ) $ envChoices env) env

-- | Extend the environment with a list of new data type definitions.
extDatsEnv :: (IsPhase ph, MonadEnv m ph)
  => HM.HashMap TypeConName DefDataType
  -- ^ A hashmap of the data constructor names, with their corresponding definitions.
  -> m ()
extDatsEnv hmap = do
  env <- getEnv
  putEnv $ setEnvDats (hmap `HM.union` envDats env) env

-- | Extend the environment with a refreshed contract id, and the variable to
-- which the fetched contract is bound. Returns a substitution, mapping the
-- given contract id, to the refreshed one.
-- While it might seem counter intuitive, the function only refreshes the
-- contract id on its first encounter. The reason is that it needs to be able to
-- keep track of old bindings.
-- Note that instead of overwriting old bindings, the function creates a new
-- synonym between the old and new binding.
extCidEnv :: (IsPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Flag denoting whether the contract id should be refreshed.
  -- Note that even with the flag on, contract id are only refreshed on their
  -- first encounter.
  -> Expr
  -- ^ The contract id expression.
  -> ExprVarName
  -- ^ The variable name to which the fetched contract is bound.
  -> m ExprSubst
extCidEnv b exp var = do
  case exp of
    -- Filter out any bindings to `_`.
    EVar (ExprVarName "ds2") -> return emptyExprSubst
    _ -> do
      prev <- do
        { (cur, old) <- lookupCid exp
        ; return $ cur : old }
        `catchError` (\_ -> return [])
      proj_def <- check_proj_cid exp
      (cid, subst) <- expr2cid (b && null prev && proj_def) exp
      env <- getEnv
      putEnv $ setEnvCids (HM.insert cid (var, prev) $ envCids env) env
      return subst
  where
    -- | Internal function to check whether the given cid has not yet been
    -- defined in a different projection.
    check_proj_cid :: (IsPhase ph, MonadEnv m ph)
      => Expr
      -- ^ The cid expression to verify.
      -> m Bool
    check_proj_cid (ERecProj _ _ (EVar x)) = do
      skols <- envSkols <$> getEnv
      return $ null [fs' | SkolRec x' fs' <- skols, x == x']
    check_proj_cid (EStructProj _ (EVar x)) = do
      skols <- envSkols <$> getEnv
      return $ null [fs' | SkolRec x' fs' <- skols, x == x']
    check_proj_cid _ = return True

-- | Extend the environment with an additional precondition, assigned to the
-- corresponding template.
extPrecond :: (IsPhase ph, MonadEnv m ph)
  => Qualified TypeConName
  -- ^ The template to assign the precondition to.
  -> (Expr -> Expr)
  -- ^ The precondition function, taking the `this` variable.
  -> m ()
extPrecond tem precond = do
  env <- getEnv
  putEnv (setEnvPreconds (HM.insert tem precond (envPreconds env)) env)

-- | Extend the environment with additional equality constraints, between a
-- variable and its field values.
extCtrRec :: (IsPhase ph, MonadEnv m ph)
  => ExprVarName
  -- ^ The variable to be asserted.
  -> [(FieldName, Expr)]
  -- ^ The fields with their values.
  -> m ()
extCtrRec var fields = do
  let ctrs = map (\(f, e) -> BEq e (EStructProj f (EVar var))) fields
  env <- getEnv
  putEnv $ setEnvCtrs (ctrs ++ envCtrs env) env

-- | Extend the environment with the given constraint.
extCtr :: (IsPhase ph, MonadEnv m ph)
  => Expr
  -- ^ The constraint to add.
  -> m ()
extCtr exp = do
  let ctrs = toBoolExpr exp
  env <- getEnv
  putEnv $ setEnvCtrs (ctrs ++ envCtrs env) env

-- | Lookup an expression variable in the environment. Returns `True` if this variable
-- has been skolemised, or `False` otherwise.
lookupVar :: (IsPhase ph, MonadEnv m ph)
  => ExprVarName
  -- ^ The expression variable to look up.
  -> m Bool
lookupVar x = do
  skols <- envSkols <$> getEnv
  return $ elem (SkolVar x) skols

-- | Lookup a record project in the environment. Returns a boolean denoting
-- whether or not the record projection has been skolemised.
lookupRec :: (IsPhase ph, MonadEnv m ph)
  => ExprVarName
  -- ^ The expression variable on which is being projected.
  -> FieldName
  -- ^ The field name which is being projected.
  -> m Bool
lookupRec x f = do
  skols <- envSkols <$> getEnv
  let fields = [ fs | SkolRec y fs <- skols, x == y ]
  if not (null fields)
    then return (elem f $ head fields)
    else return False

-- | Lookup a value name in the environment. Returns its (partially) evaluated
-- definition, together with the updates it performs.
lookupVal :: (IsPhase ph, MonadEnv m ph)
  => Qualified ExprValName
  -- ^ The value name to lookup.
  -> m (Maybe (Expr, UpdateSet ph))
lookupVal val = do
  vals <- envVals <$> getEnv
  return $ HM.lookup val vals

-- | Lookup a choice name in the environment. Returns a function which, once
-- self, this and args have been instantiated, returns the set of updates it
-- performs. Also returns the return type of the choice.
lookupChoice :: (IsPhase ph, MonadEnv m ph)
  => Qualified TypeConName
  -- ^ The template name in which this choice is defined.
  -> ChoiceName
  -- ^ The choice name to lookup.
  -> m (Expr -> Expr -> Expr -> UpdateSet ph, Type)
lookupChoice tem ch = do
  chs <- envChoices <$> getEnv
  case HM.lookup (UpdChoice tem ch) chs of
    Nothing -> throwError (UnknownChoice ch)
    Just ChoiceData{..} -> do
      let updFunc (self :: Expr) (this :: Expr) (args :: Expr) =
            let subst = createExprSubst [(_cdSelf,self),(_cdThis,this),(_cdArgs,args)]
            in substituteTm subst _cdUpds
      return (updFunc, _cdType)

-- | Lookup a data type definition in the environment.
lookupDataCon :: (IsPhase ph, MonadEnv m ph)
  => TypeConName
  -- ^ The data constructor to lookup.
  -> m DefDataType
lookupDataCon tc = do
  dats <- envDats <$> getEnv
  case HM.lookup tc dats of
    Nothing -> throwError (UnknownDataCons tc)
    Just def -> return def

-- | Lookup a contract id in the environment. Returns the variable its fetched
-- contract is bound to, along with a list of any previous bindings.
lookupCid :: (IsPhase ph, MonadEnv m ph)
  => Expr
  -- ^ The contract id to lookup.
  -> m (ExprVarName, [ExprVarName])
lookupCid exp = do
  (cid, _) <- expr2cid False exp
  cids <- envCids <$> getEnv
  case HM.lookup cid cids of
    Nothing -> throwError $ UnknownCid cid
    Just var -> return var

-- TODO: This should work recursively
-- | Lookup the field names and corresponding types, for a given record type
-- constructor name, if possible.
recTypConFields :: (IsPhase ph, MonadEnv m ph)
  => TypeConName
  -- ^ The record type constructor name to lookup.
  -> m (Maybe [(FieldName,Type)])
recTypConFields tc = lookupDataCon tc >>= \dat -> case dataCons dat of
  DataRecord fields -> return $ Just fields
  _ -> return Nothing

-- | Lookup the fields for a given record type, if possible.
recTypFields :: (IsPhase ph, MonadEnv m ph)
  => Type
  -- ^ The type to lookup.
  -> m (Maybe [(FieldName,Type)])
recTypFields (TCon tc) = do
  recTypConFields $ qualObject tc
recTypFields (TStruct fs) = return $ Just fs
recTypFields (TApp (TBuiltin BTContractId) t) = recTypFields t
recTypFields _ = return Nothing

-- | Lookup the record fields and corresponding values from a given expression.
recExpFields :: (IsPhase ph, MonadEnv m ph)
  => Expr
  -- ^ The expression to lookup.
  -> m (Maybe [(FieldName, Expr)])
recExpFields (EVar x) = do
  skols <- envSkols <$> getEnv
  let fss = [ fs | SkolRec y fs <- skols, x == y ]
  if not (null fss)
    -- TODO: I would prefer `this.amount` here
    then return $ Just $ zip (head fss) (map (EVar . fieldName2VarName) $ head fss)
    else throwError $ UnboundVar x
recExpFields (ERecCon _ fs) = return $ Just fs
recExpFields (EStructCon fs) = return $ Just fs
recExpFields (ERecUpd _ f recExp fExp) = do
  recExpFields recExp >>= \case
    Just fs -> do
      unless (isJust $ find (\(n, _) -> n == f) fs) (throwError $ UnknownRecField f)
      return $ Just $ (f, fExp) : [(n, e) | (n, e) <- fs, n /= f]
    Nothing -> return Nothing
recExpFields (ERecProj _ f e) = do
  recExpFields e >>= \case
    Just fields -> case lookup f fields of
      Just e' -> recExpFields e'
      Nothing -> throwError $ UnknownRecField f
    Nothing -> return Nothing
recExpFields (EStructProj f e) = trace "Exp F" $ do
  recExpFields e >>= \case
    Just fields -> case lookup f fields of
      Just e' -> recExpFields e'
      Nothing -> throwError $ UnknownRecField f
    Nothing -> return Nothing
recExpFields _ = return Nothing

instance SubstTm BoolExpr where
  substituteTm s (BExpr e) = BExpr (substituteTm s e)
  substituteTm s (BAnd e1 e2) = BAnd (substituteTm s e1) (substituteTm s e2)
  substituteTm s (BNot e) = BNot (substituteTm s e)
  substituteTm s (BEq e1 e2) = BEq (substituteTm s e1) (substituteTm s e2)
  substituteTm s (BGt e1 e2) = BGt (substituteTm s e1) (substituteTm s e2)
  substituteTm s (BGtE e1 e2) = BGtE (substituteTm s e1) (substituteTm s e2)
  substituteTm s (BLt e1 e2) = BLt (substituteTm s e1) (substituteTm s e2)
  substituteTm s (BLtE e1 e2) = BLtE (substituteTm s e1) (substituteTm s e2)

instance SubstTm a => SubstTm (Cond a) where
  substituteTm s (Determined x) = Determined $ substituteTm s x
  substituteTm s (Conditional e x y) =
    Conditional (substituteTm s e) (map (substituteTm s) x) (map (substituteTm s) y)

instance IsPhase ph => SubstTm (UpdateSet ph) where
  substituteTm s upds = setUpdSetUpdates substUpd upds
    where
      substUpd = map (substituteTm s) (updSetUpdates upds)

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
