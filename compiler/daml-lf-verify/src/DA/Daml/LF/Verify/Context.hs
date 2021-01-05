-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  , Rec(..)
  , Env(..)
  , Error(..)
  , MonadEnv
  , UpdateSet
  , BaseUpd(..)
  , Upd(..)
  , ChoiceData(..)
  , UpdChoice(..)
  , Skolem(..)
  , getEnv, putEnv
  , runEnv
  , genRenamedVar
  , createCond
  , introCond
  , makeRec, makeMutRec
  , addBaseUpd, addChoice
  , emptyUpdateSet, extendUpdateSet, concatUpdateSet
  , extVarEnv, extRecEnv, extRecEnvTCons, extValEnv, extChEnv, extDatsEnv
  , extCidEnv, extPrecond, extCtrRec, extCtr
  , lookupVar, lookupRec, lookupVal, lookupChoice, lookupDataCon, lookupCid
  , conditionalUpdateSet
  , computeCycles
  , fieldName2VarName
  , recTypConFields, recTypFields, recExpFields
  , applySubstInUpd
  ) where

import Control.Monad.Error.Class (MonadError (..), throwError)
import Control.Monad.Extra (whenJust)
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
import DA.Daml.LF.Ast.Subst

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
  deriving (Eq, Show)

-- | Convert an expression constraint into boolean expressions.
-- This function covers the supported operations in `ensure` statements.
-- Operations that are not currently supported trigger a warning, and are then
-- ignored for the remainder of the verification process. No harm comes from an
-- unsupported operation.
toBoolExpr :: Expr -> [BoolExpr]
toBoolExpr (EBuiltin (BEBool True)) = []
toBoolExpr (ETmApp (ETmApp op e1) e2) = case op of
  (EBuiltin (BEEqual _)) -> [BEq e1 e2]
  (ETyApp (EBuiltin BEGreaterNumeric) _) -> [BGt e1 e2]
  (ETyApp (EBuiltin BEGreaterEqNumeric) _) -> [BGtE e1 e2]
  (ETyApp (EBuiltin BELessNumeric) _) -> [BLt e1 e2]
  (ETyApp (EBuiltin BELessEqNumeric) _) -> [BLtE e1 e2]
  _ -> trace ("Warning: Unmatched Expr to BoolExpr Operator: " ++ show op) []
toBoolExpr exp = trace ("Warning: Unmatched Expr to BoolExpr: " ++ show exp) []

-- | Data type denoting a potentially conditional value.
data Cond a
  = Determined a
  -- ^ Non-conditional value.
  | Conditional BoolExpr [Cond a] [Cond a]
  -- ^ Conditional value, with a (Boolean) condition, values in case
  -- the condition holds, and values in case it doesn't.
  deriving (Eq, Show, Functor)

-- | Construct a simple conditional.
createCond :: BoolExpr
  -- ^ The condition to depend on.
  -> a
  -- ^ The value in case the condition holds.
  -> a
  -- ^ The value in case the condition does not hold.
  -> Cond a
createCond cond x y = Conditional cond [Determined x] [Determined y]

extCond :: Eq a => BoolExpr -> Cond a -> [Cond a]
extCond bexp cond =
  let cond' = case cond of
        (Determined x) -> Conditional bexp [Determined x] []
        (Conditional bexp' xs ys) -> Conditional (bexp `BAnd` bexp') xs ys
  in simplifyCond cond'

-- | Perform common simplifications on Conditionals.
-- This simplification should never alter the meaning of the constraints in any
-- way. It's only purpose is to simplify the output shown to the user.
-- TODO: This can be extended with additional cases in the future.
simplifyCond :: Eq a => Cond a -> [Cond a]
simplifyCond (Conditional (BAnd b1 b2) xs ys)
  | b1 == b2 = simplifyCond (Conditional b1 xs ys)
  | b1 == BNot b2 = ys
  | b2 == BNot b1 = ys
simplifyCond (Conditional b [Conditional b1 xs1 _] [Conditional b2 _ ys2])
  | b1 == b2 = simplifyCond (Conditional b xs1 ys2)
simplifyCond (Conditional _ xs ys)
  | xs == ys = concatMap simplifyCond xs
simplifyCond c = [c]

-- | Shift the conditional inside of the update set, by extending each update
-- with the condition.
introCond :: IsPhase ph => Cond (UpdateSet ph) -> UpdateSet ph
introCond (Determined upds) = upds
introCond (Conditional e updx updy) = buildCond e updx updy extCondUpd
  where
    -- | Construct a single conditional update set, combining the two input lists
    -- and the boolean expression, if the input is non-empty.
    buildCond :: IsPhase ph
      => BoolExpr
      -> [Cond (UpdateSet ph)]
      -> [Cond (UpdateSet ph)]
      -> (BoolExpr -> Upd ph -> UpdateSet ph)
      -> UpdateSet ph
    buildCond bexp cxs cys ext =
      let xs = concatMap introCond cxs
          ys = concatMap introCond cys
      in concatMap (ext bexp) xs ++ concatMap (ext $ BNot bexp) ys

-- | Data type denoting a potential recursion cycle.
data Rec a
  = Simple a
  -- ^ Basic, non-recursive value.
  | Rec [a]
  -- ^ (Possibly multiple) recursion cycles.
  -- Note that future optimisations could possible introduce an empty list here,
  -- so no assumptions are made that this list is non-empty.
  | MutRec [(String,a)]
  -- ^ (Possibly multiple) mutual recursion cycles.
  -- Note that this behaves idential to regular recursion, with the addition of
  -- an information field for debugging purposes.
  -- In the current version, this String stores the names of all values or
  -- choices in the cycle.
  deriving Functor

-- | Split a list of Rec values by constructor.
splitRecs :: [Rec [a]] -> ([a], [Rec [a]], [Rec [a]])
splitRecs inp = (simples, recs, mutrecs)
  where
    simples = concat [x | Simple x <- inp]
    recs = [Rec xs | Rec xs <- inp]
    mutrecs = [MutRec xs | MutRec xs <- inp]

-- | Introduce a Rec constructor.
-- Note that nested recursion is flattened. This is fine as each cycles has to
-- preserve the field, meaning that the total result should be a nop for field.
makeRec :: [Rec [Cond a]] -> [Rec [Cond a]]
makeRec inp = Rec [simples] : recs ++ mutrecs
  where
    (simples, recs, mutrecs) = splitRecs inp

-- | Introduce a MutRec constructor.
makeMutRec :: [Rec [Cond a]] -> String -> [Rec [Cond a]]
makeMutRec inp str = MutRec [(str,simples)] : recs ++ mutrecs
  where
    (simples, recs, mutrecs) = splitRecs inp

-- | Take a conditional Rec value apart, by splitting into the simple values,
-- and constructing a list of all possible cycles (along with some debugging
-- information).
computeCycles :: [Rec [Cond a]] -> [(String, [Cond a])]
computeCycles inp = ("Main flow: ", simples) : recs
  where
    simples = concat [x | Simple x <- inp]
    recs = [("Recursion cycle: ", x) | Rec xs <- inp, x <- xs]
        ++ [("Mutual recursion cycle: " ++ info, x) | MutRec xs <- inp, (info, x) <- xs]

-- | Data type denoting a simple update.
data BaseUpd
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
  deriving (Eq, Show)

-- | The collection of updates being performed.
type UpdateSet ph = [Upd ph]

-- | Construct an empty update set.
emptyUpdateSet :: UpdateSet ph
emptyUpdateSet = []

-- | Extend an update set.
extendUpdateSet :: Upd ph -> UpdateSet ph -> UpdateSet ph
extendUpdateSet = (:)

-- | Combine two update sets.
concatUpdateSet :: UpdateSet ph -> UpdateSet ph -> UpdateSet ph
concatUpdateSet = (++)

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
  -- | The updates which can be performed.
  data Upd ph
  -- | The environment for the DAML-LF verifier.
  data Env ph
  -- | Construct a base update.
  baseUpd :: Rec [Cond BaseUpd] -> Upd ph
  -- | Construct a choice exercise update.
  choiceUpd :: Cond UpdChoice -> Upd ph
  -- | Construct a value update.
  valueUpd :: Cond (Qualified ExprValName) -> Upd ph
  -- | Map over a single base update.
  mapBaseUpd :: (Rec [Cond BaseUpd] -> Upd ph) -> Upd ph -> Upd ph
  -- | Check whether the update set contains any choice references.
  containsChoiceRefs :: UpdateSet ph -> Bool
  -- | Extend the conditional of an update.
  extCondUpd :: BoolExpr -> Upd ph -> UpdateSet ph
  -- | Construct an empty environment.
  emptyEnv :: Env ph
  -- | Get the skolemised term variables and fields from the environment.
  envSkols :: Env ph -> [Skolem]
  -- | Update the skolemised term variables and fields in the environment.
  setEnvSkols :: [Skolem] -> Env ph -> Env ph
  -- | Get the bound values from the environment.
  envVals :: Env ph -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet ph)
  -- | Get the data constructors from the environment.
  envDats :: Env ph -> HM.HashMap (Qualified TypeConName) DefDataType
  -- | Get the fetched cid's mapped to their current variable name, along with
  -- a list of any potential old variable names, from the environment.
  envCids :: Env ph -> HM.HashMap Cid (ExprVarName, [ExprVarName])
  -- | Update the fetched cid's in the environment.
  setEnvCids :: HM.HashMap Cid (ExprVarName, [ExprVarName]) -> Env ph -> Env ph
  -- | Get the set of preconditions from the environment.
  envPreconds :: Env ph -> HM.HashMap (Qualified TypeConName) (Expr -> Expr)
  -- | Get the additional constraints from the environment.
  envCtrs :: Env ph -> [BoolExpr]
  -- | Update the additional constraints in the environment.
  setEnvCtrs :: [BoolExpr] -> Env ph -> Env ph
  -- | Get the set of relevant choices from the environment.
  envChoices :: Env ph -> HM.HashMap UpdChoice (ChoiceData ph)

instance IsPhase 'ValueGathering where
  data Upd 'ValueGathering
    = UpdVGBase ![Cond BaseUpd]
    -- ^ A base update.
    | UpdVGChoice !(Cond UpdChoice)
    -- ^ An exercised choice.
    | UpdVGVal !(Cond (Qualified ExprValName))
    -- ^ A referenced value.
  data Env 'ValueGathering = EnvVG
    ![Skolem]
    -- ^ The skolemised term variables and fields.
    !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering))
    -- ^ The bound values.
    !(HM.HashMap (Qualified TypeConName) DefDataType)
    -- ^ The set of data constructors.
    !(HM.HashMap Cid (ExprVarName, [ExprVarName]))
    -- ^ The set of fetched cid's mapped to their current variable name, along
    -- with a list of any potential old variable names.
    ![BoolExpr]
    -- ^ Additional constraints.
  baseUpd = \case
    Simple upd -> UpdVGBase upd
    _ -> error "The value gathering phase can't contain recursive updates."
  choiceUpd = UpdVGChoice
  valueUpd = UpdVGVal
  mapBaseUpd f = \case
    UpdVGBase b -> f (Simple b)
    upd -> upd
  containsChoiceRefs upds = not $ null [x | UpdVGChoice x <- upds]
  extCondUpd bexp = \case
    (UpdVGBase base) -> map (UpdVGBase . extCond bexp) base
    (UpdVGChoice cho) -> map UpdVGChoice $ extCond bexp cho
    (UpdVGVal val) -> map UpdVGVal $ extCond bexp val
  emptyEnv = EnvVG [] HM.empty HM.empty HM.empty []
  envSkols (EnvVG sko _ _ _ _) = sko
  setEnvSkols sko (EnvVG _ val dat cid ctr) = EnvVG sko val dat cid ctr
  envVals (EnvVG _ val _ _ _) = val
  envDats (EnvVG _ _ dat _ _) = dat
  envCids (EnvVG _ _ _ cid _) = cid
  setEnvCids cid (EnvVG sko val dat _ ctr) = EnvVG sko val dat cid ctr
  envPreconds _ = HM.empty
  envCtrs (EnvVG _ _ _ _ ctr) = ctr
  setEnvCtrs ctr (EnvVG sko val dat cid _) = EnvVG sko val dat cid ctr
  envChoices _ = HM.empty

instance IsPhase 'ChoiceGathering where
  data Upd 'ChoiceGathering
    = UpdCGBase !(Rec [Cond BaseUpd])
    -- ^ A single recursion cycle, containing a list of updates.
    | UpdCGChoice !(Cond UpdChoice)
    -- ^ An exercised choice.
  data Env 'ChoiceGathering = EnvCG
    ![Skolem]
    -- ^ The skolemised term variables and fields.
    !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ChoiceGathering))
    -- ^ The bound values.
    !(HM.HashMap (Qualified TypeConName) DefDataType)
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
  baseUpd = UpdCGBase
  choiceUpd = UpdCGChoice
  valueUpd = error "The choice gathering phase no longer contains value references."
  mapBaseUpd f = \case
    UpdCGBase b -> f b
    upd -> upd
  containsChoiceRefs upds = not $ null [x | UpdCGChoice x <- upds]
  extCondUpd bexp = \case
    (UpdCGBase base) ->
      let base' = case base of
            Simple upd -> map (Simple . extCond bexp) upd
            Rec cycles -> [Rec $ map (concatMap (extCond bexp)) cycles]
            MutRec cycles -> [MutRec $ map (second (concatMap (extCond bexp))) cycles]
      in map UpdCGBase base'
    (UpdCGChoice cho) -> map UpdCGChoice (extCond bexp cho)
  emptyEnv = EnvCG [] HM.empty HM.empty HM.empty HM.empty [] HM.empty
  envSkols (EnvCG sko _ _ _ _ _ _) = sko
  setEnvSkols sko (EnvCG _ val dat cid pre ctr cho) = EnvCG sko val dat cid pre ctr cho
  envVals (EnvCG _ val _ _ _ _ _) = val
  envDats (EnvCG _ _ dat _ _ _ _) = dat
  envCids (EnvCG _ _ _ cid _ _ _) = cid
  setEnvCids cid (EnvCG sko val dat _ pre ctr cho) = EnvCG sko val dat cid pre ctr cho
  envPreconds (EnvCG _ _ _ _ pre _ _) = pre
  envCtrs (EnvCG _ _ _ _ _ ctr _) = ctr
  setEnvCtrs ctr (EnvCG sko val dat cid pre _ cho) = EnvCG sko val dat cid pre ctr cho
  envChoices (EnvCG _ _ _ _ _ _ cho) = cho

instance IsPhase 'Solving where
  data Upd 'Solving
    = UpdSBase !(Rec [Cond BaseUpd])
    -- ^ A single recursion cycle, containing a list of updates.
  data Env 'Solving = EnvS
    ![Skolem]
    -- ^ The skolemised term variables and fields.
    !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'Solving))
    -- ^ The bound values.
    !(HM.HashMap (Qualified TypeConName) DefDataType)
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
  baseUpd = UpdSBase
  choiceUpd = error "The solving phase no longer contains choice references."
  valueUpd = error "The solving phase no longer contains value references."
  mapBaseUpd f (UpdSBase b) = f b
  containsChoiceRefs _ = False
  extCondUpd bexp (UpdSBase base) =
    let base' = case base of
          Simple upd -> map (Simple . extCond bexp) upd
          Rec cycles -> [Rec $ map (concatMap (extCond bexp)) cycles]
          MutRec cycles -> [MutRec $ map (second (concatMap (extCond bexp))) cycles]
    in map UpdSBase base'
  emptyEnv = EnvS [] HM.empty HM.empty HM.empty HM.empty [] HM.empty
  envSkols (EnvS sko _ _ _ _ _ _) = sko
  setEnvSkols sko (EnvS _ val dat cid pre ctr cho) = EnvS sko val dat cid pre ctr cho
  envVals (EnvS _ val _ _ _ _ _) = val
  envDats (EnvS _ _ dat _ _ _ _) = dat
  envCids (EnvS _ _ _ cid _ _ _) = cid
  setEnvCids cid (EnvS sko val dat _ pre ctr cho) = EnvS sko val dat cid pre ctr cho
  envPreconds (EnvS _ _ _ _ pre _ _) = pre
  envCtrs (EnvS _ _ _ _ _ ctr _) = ctr
  setEnvCtrs ctr (EnvS sko val dat cid pre _ cho) = EnvS sko val dat cid pre ctr cho
  envChoices (EnvS _ _ _ _ _ _ cho) = cho

-- | Update the bound values in the environment.
setEnvVals :: HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering)
  -> Env 'ValueGathering -> Env 'ValueGathering
setEnvVals val (EnvVG sko _ dat cid ctr) = EnvVG sko val dat cid ctr

-- | Update the set of preconditions in the environment.
setEnvPreconds :: HM.HashMap (Qualified TypeConName) (Expr -> Expr)
  -> Env 'ChoiceGathering -> Env 'ChoiceGathering
setEnvPreconds pre (EnvCG sko val dat cid _ ctr cho) =
  EnvCG sko val dat cid pre ctr cho

-- | Update the data constructors in the environment.
setEnvDats :: HM.HashMap (Qualified TypeConName) DefDataType
  -> Env 'ValueGathering -> Env 'ValueGathering
setEnvDats dat (EnvVG sko val _ cid ctr) = EnvVG sko val dat cid ctr

-- | Update the set of relevant choices in the environment.
setEnvChoices :: HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering)
  -> Env 'ChoiceGathering -> Env 'ChoiceGathering
setEnvChoices cho (EnvCG sko val dat cid pre ctr _) =
  EnvCG sko val dat cid pre ctr cho

-- | Add a single BaseUpd to an UpdateSet.
addBaseUpd :: IsPhase ph
  => UpdateSet ph
  -- ^ The update set to extend.
  -> BaseUpd
  -- ^ The update to add.
  -> UpdateSet ph
addBaseUpd upds upd = baseUpd (Simple [Determined upd]) : upds

-- | Add a single choice reference to an UpdateSet.
addChoice :: IsPhase ph
  => UpdateSet ph
  -- ^ The update set to extend.
  -> Qualified TypeConName
  -- ^ The template name in which this choice is defined.
  -> ChoiceName
  -- ^ The choice name to add.
  -> UpdateSet ph
addChoice upds tem cho = choiceUpd (Determined $ UpdChoice tem cho) : upds

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
  -> m (Cid, Subst)
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
  -> m (ExprVarName, Subst)
refresh_cid False x = return (x, mempty)
refresh_cid True x = do
  y <- genRenamedVar x
  return (y, exprSubst x (EVar y))

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
      -- Note that x might already be in the skolem list. We thus lookup the
      -- current (latest) entry, and overwrite it with a new entry containing
      -- both the current and new fields.
      newFs = case [fs' | SkolRec x' fs' <- skols, x == x'] of
        [] -> fs
        (curFs:_) -> fs ++ curFs
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
    step (f,typ) = do
      mbFields <- recTypFields typ
      whenJust mbFields $ \fsRec ->
        extRecEnv (fieldName2VarName f) $ map fst fsRec

-- | Extend the environment with a new skolem variable.
extSkolEnv :: (IsPhase ph, MonadEnv m ph)
  => Skolem
  -- ^ The skolem variable to add.
  -> m ()
extSkolEnv skol = do
  env <- getEnv
  when (notElem skol $ envSkols env)
       (putEnv $ setEnvSkols (skol : envSkols env) env)

-- | Extend the environment with a new value definition.
extValEnv :: MonadEnv m 'ValueGathering
  => Qualified ExprValName
  -- ^ The name of the value being defined.
  -> Expr
  -- ^ The (partially) evaluated value definition.
  -> UpdateSet 'ValueGathering
  -- ^ The updates performed by this value.
  -> m ()
extValEnv val expr upd = do
  env <- getEnv
  putEnv $ setEnvVals (HM.insert val (expr, upd) $ envVals env) env

-- | Extends the environment with a new choice.
extChEnv :: MonadEnv m 'ChoiceGathering
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
  -> UpdateSet 'ChoiceGathering
  -- ^ The updates performed by the new choice.
  -> Type
  -- ^ The result type of the new choice.
  -> m ()
extChEnv tc ch self this arg upd typ = do
  env <- getEnv
  putEnv $ setEnvChoices (HM.insert (UpdChoice tc ch) (ChoiceData self this arg upd typ) $ envChoices env) env

-- | Extend the environment with a list of new data type definitions.
extDatsEnv :: MonadEnv m 'ValueGathering
  => HM.HashMap (Qualified TypeConName) DefDataType
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
  -> m Subst
extCidEnv b exp var = do
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
extPrecond :: MonadEnv m 'ChoiceGathering
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
  case [ fs | SkolRec y fs <- skols, x == y ] of
    [] -> return False
    (fields:_) -> return (f `elem` fields)

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
  -> m (Maybe (Expr -> Expr -> Expr -> UpdateSet ph, Type))
lookupChoice tem ch = do
  chs <- envChoices <$> getEnv
  case HM.lookup (UpdChoice tem ch) chs of
    Nothing -> return Nothing
    Just ChoiceData{..} -> do
      let updFunc (self :: Expr) (this :: Expr) (args :: Expr) =
            let subst = foldMap (uncurry exprSubst) [(_cdSelf,self),(_cdThis,this),(_cdArgs,args)]
            in map (applySubstInUpd subst) _cdUpds
      return $ Just (updFunc, _cdType)

-- | Lookup a data type definition in the environment.
lookupDataCon :: (IsPhase ph, MonadEnv m ph)
  => Qualified TypeConName
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

-- | Lookup the field names and corresponding types, for a given record type
-- constructor name, if possible.
-- TODO: At the moment, this does not work recursively for nested type
-- constructors. This might be a useful extension later on.
recTypConFields :: (IsPhase ph, MonadEnv m ph)
  => Qualified TypeConName
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
  recTypConFields tc
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
  case [ fs | SkolRec y fs <- skols, x == y ] of
    [] -> throwError $ UnboundVar x
    (fss:_) -> return $ Just $ zip fss (map (\f -> EStructProj f (EVar x)) fss)
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
recExpFields (EStructProj f (EVar _)) = recExpFields (EVar $ fieldName2VarName f)
recExpFields (EStructProj f e) = do
  recExpFields e >>= \case
    Just fields -> case lookup f fields of
      Just e' -> recExpFields e'
      Nothing -> throwError $ UnknownRecField f
    Nothing -> return Nothing
recExpFields (ELocation _ e) = recExpFields e
recExpFields _ = return Nothing

applySubstInBoolExpr :: Subst -> BoolExpr -> BoolExpr
applySubstInBoolExpr subst = \case
    BExpr e -> BExpr (applySubstInExpr subst e)
    BAnd e1 e2 -> BAnd (applySubstInBoolExpr subst e1) (applySubstInBoolExpr subst e2)
    BNot e -> BNot (applySubstInBoolExpr subst e)
    BEq e1 e2 -> BEq (applySubstInExpr subst e1) (applySubstInExpr subst e2)
    BGt e1 e2 -> BGt (applySubstInExpr subst e1) (applySubstInExpr subst e2)
    BGtE e1 e2 -> BGtE (applySubstInExpr subst e1) (applySubstInExpr subst e2)
    BLt e1 e2 -> BLt (applySubstInExpr subst e1) (applySubstInExpr subst e2)
    BLtE e1 e2 -> BLtE (applySubstInExpr subst e1) (applySubstInExpr subst e2)

applySubstInCond :: (Subst -> a -> a) -> Subst -> Cond a -> Cond a
applySubstInCond f subst = \case
    Determined a -> Determined (f subst a)
    Conditional c xs ys ->
        Conditional
            (applySubstInBoolExpr subst c)
            (map (applySubstInCond f subst) xs)
            (map (applySubstInCond f subst) ys)

applySubstInUpd :: IsPhase ph => Subst -> Upd ph -> Upd ph
applySubstInUpd subst =
    mapBaseUpd (baseUpd . fmap (map (applySubstInCond applySubstInBaseUpd subst)))

applySubstInBaseUpd :: Subst -> BaseUpd -> BaseUpd
applySubstInBaseUpd subst UpdCreate{..} =
    UpdCreate _creTemp (map (second $ applySubstInExpr subst) _creField)
applySubstInBaseUpd subst UpdArchive{..} =
    UpdArchive _arcTemp (map (second $ applySubstInExpr subst) _arcField)

-- | Data type representing an error.
data Error
  = UnknownValue (Qualified ExprValName)
  | UnknownDataCons (Qualified TypeConName)
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
