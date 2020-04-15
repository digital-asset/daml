-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

-- | Contexts for DAML LF static verification
module DA.Daml.LF.Verify.Context
  ( Delta, Error(..)
  , MonadDelta, dchs, devars, _devals, devals
  , UpdateSet(..)
  , UpdCreate(..), usCre, usArc, usCho, usVal
  , UpdArchive(..)
  , UpdChoice(..)
  , runDelta
  , emptyDelta, setDelta
  , introDelta, extVarDelta
  , lookupDExprVar, lookupDVal, lookupDChoice
  , concatDelta
  , emptyUpdateSet
  , concatUpdateSet
  , solveValueUpdatesDelta
  , testPrint
  ) where

import Control.Lens hiding (Context)
import Control.Monad.Error.Class (MonadError (..), throwError)
import Control.Monad.Reader
import Data.Maybe (listToMaybe)
import qualified Data.HashMap.Strict as HM

import DA.Daml.LF.Ast

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

makeLenses ''UpdateSet

emptyUpdateSet :: UpdateSet
emptyUpdateSet = UpdateSet [] [] [] []

concatUpdateSet :: UpdateSet -> UpdateSet -> UpdateSet
concatUpdateSet (UpdateSet cres1 arcs1 chos1 vals1) (UpdateSet cres2 arcs2 chos2 vals2) =
  UpdateSet (cres1 ++ cres2) (arcs1 ++ arcs2) (chos1 ++ chos2) (vals1 ++ vals2)

-- | The environment for the DAML-LF verifier
data Delta = Delta
  { _devars :: ![ExprVarName]
    -- ^ The skolemised term variables.
  , _devals :: !(HM.HashMap (Qualified ExprValName) (Expr, UpdateSet))
    -- ^ The bound values.
  , _dchs :: !(HM.HashMap (Qualified TypeConName, ChoiceName) UpdateSet)
    -- ^ The set of relevant choices.
  -- TODO: split this off into data types for readability?
  }
  deriving Show

makeLenses ''Delta

emptyDelta :: Delta
emptyDelta = Delta [] HM.empty HM.empty

concatDelta :: Delta -> Delta -> Delta
concatDelta (Delta vars1 vals1 chs1) (Delta vars2 vals2 chs2) =
  Delta (vars1 ++ vars2) (vals1 `HM.union` vals2) (chs1 `HM.union` chs2)
  -- TODO: union makes me slightly nervous, as it allows overlapping keys
  -- (and just uses the first)

-- | Type class constraint with the required monadic effects for functions
-- manipulating the verification environment.
-- TODO: the more I look at this, the more convinced I am that this should be a
-- state monad, not a reader.
type MonadDelta m = (MonadError Error m, MonadReader Delta m)

runDelta :: ReaderT Delta (Either Error) a -> Delta -> Either Error a
runDelta = runReaderT

-- | Run a computation in the current environment, with an additional
-- environment extension.
introDelta :: MonadDelta m => Delta -> m a -> m a
introDelta delta = local (concatDelta delta)

-- TODO: This is a bit strange in a reader monad.
-- Figure out a way to extend, instead of overwrite every time.
setDelta :: MonadDelta m => Delta -> m a -> m a
setDelta delta = local (const delta)

extVarDelta :: MonadDelta m => ExprVarName -> m a -> m a
extVarDelta x = local (over devars ((:) x))

lookupDExprVar :: MonadDelta m => ExprVarName -> m ()
lookupDExprVar x = ask >>= \ del -> unless (elem x $ _devars del)
  (throwError $ UnboundVar x)

lookupDVal :: MonadDelta m => Qualified ExprValName -> m (Expr, UpdateSet)
-- lookupDVal w = view (devals . at w) >>= match _Just (UnknownValue w)
lookupDVal val = do
  del <- ask
  case lookupValInHMap (_devals del) val of
    Just res -> return res
    Nothing -> throwError (UnknownValue val)

-- TODO: There seems to be something wrong with the qualifiers. This is a
-- temporary solution.
lookupValInHMap :: (HM.HashMap (Qualified ExprValName) (Expr, UpdateSet))
  -> Qualified ExprValName -> Maybe (Expr, UpdateSet)
lookupValInHMap hmap val = listToMaybe $ HM.elems
  $ HM.filterWithKey (\name _ -> (qualObject name) == (qualObject val)) hmap

lookupDChoice :: MonadDelta m => Qualified TypeConName -> ChoiceName
             -> m UpdateSet
lookupDChoice tem ch = view (dchs . at (tem, ch)) >>= match _Just
  (UnknownChoice ch)

-- | Helper functions mirrored from Env.
match :: MonadDelta m => Prism' a b -> Error -> a -> m b
match p err x = either (const (throwError err)) pure (matching p x)
-- TODO: no context, like in Gamma

solveValueUpdatesDelta :: Delta -> Delta
solveValueUpdatesDelta del =
  let hmap0 = _devals del
      hmap1 = foldl solveValueUpdate hmap0 (HM.keys hmap0)
  in del{_devals = hmap1}

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
