-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Daml.LF.Evaluator.Value
  ( Value(..), Func(..), Tag(..), B0(..), B2(..), B3(..), FieldName,
    trueTag,falseTag,consTag,nilTag,mkTag,noneTag,someTag,
    bool, num, deNum,
    apply,
    projectRec,
    Effect, run, Counts(..),
  ) where

import Control.Monad (ap,liftM)
import Data.Int (Int64)
import qualified DA.Daml.LF.Ast as LF
import qualified Data.Text as Text

data Value
  = Function Func
  | Record [(FieldName, Value)]
  | Constructed Tag [Value]
  | B0 B0
  | B2 B2
  | B2_1 B2 Value
  | B3 B3
  | B3_1 B3 Value
  | B3_2 B3 Value Value
  deriving (Show)

newtype Func = Func (Value -> Effect Value)

instance Show Func where show _ = "<func>"

type FieldName = LF.FieldName

newtype Tag = Tag { unTag :: String }
  deriving (Eq,Show)

data B0
  = Unit
  | Num Int64
  deriving (Show)

data B2
  = ADDI | SUBI | MULI | MODI | EXPI
  | LESSI | LESSEQI | EQUALI | GREATERI | GREATEREQI
  deriving (Show)

data B3
  = FOLDL | FOLDR
  deriving (Show)


bool :: Bool -> Value
bool = \case
  True -> Constructed trueTag []
  False -> Constructed falseTag []

apply :: (Value, Value) -> Effect Value
apply = \case
  (Function (Func f), v) -> do CountApp; f v
  (Record _,_) -> error "Value.apply, Record"
  (Constructed _ _,_) -> error "Value.apply, Constructed"
  (B0 _,_) -> error "Value.apply, B0"
  (B2 b,v1) -> return $ B2_1 b v1
  (B2_1 b v1,v2) -> do CountPrim; return $ applyB2 b v1 v2
  (B3 b,v1) -> return $ B3_1 b v1
  (B3_1 b v1,v2) -> return $ B3_2 b v1 v2
  (B3_2 b v1 v2,v3) -> do CountPrim; applyB3 b v1 v2 v3

applyB2 :: B2 -> Value -> Value -> Value
applyB2 = \case
  ADDI -> liftNN_N (+)
  SUBI -> liftNN_N (-)
  MULI -> liftNN_N (*)
  MODI -> liftNN_N mod
  EXPI -> liftNN_N (^)
  LESSI -> liftNN_B (<)
  LESSEQI -> liftNN_B (<=)
  EQUALI -> liftNN_B (==)
  GREATERI -> liftNN_B (>)
  GREATEREQI -> liftNN_B (>=)

liftNN_N :: (Int64 -> Int64 -> Int64) -> Value -> Value -> Value
liftNN_N f v1 v2 = do
  let n1 = deNum v1
  let n2 = deNum v2
  B0 $ Num $ f n1 n2

liftNN_B :: (Int64 -> Int64 -> Bool) -> Value -> Value -> Value
liftNN_B f v1 v2 = do
  let n1 = deNum v1
  let n2 = deNum v2
  bool $ f n1 n2

deNum :: Value -> Int64
deNum = \case
  B0 (Num n) -> n
  v -> error $ "deNum, " <> show v

applyB3 :: B3 -> Value -> Value -> Value -> Effect Value
applyB3 = \case
  FOLDL -> applyFoldl
  FOLDR -> applyFoldr

applyFoldl :: Value -> Value -> Value -> Effect Value
applyFoldl f b xs = do
  case deListValue xs of
    Nothing -> return b
    Just (x,xs') -> do
      fb <- apply (f,b)
      b' <- apply (fb,x)
      applyFoldl f b' xs'

applyFoldr :: Value -> Value -> Value -> Effect Value
applyFoldr f b xs = do
  case deListValue xs of
    Nothing -> return b
    Just (x,xs') -> do
      fx <- apply (f,x)
      b' <- applyFoldr f b xs'
      apply (fx,b')

projectRec :: FieldName -> Value -> Effect Value
projectRec fieldName = \case
  Record xs ->
    case lookup fieldName xs of
      Nothing -> error $ "Value.projectRec, " <> show fieldName
      Just v -> do
        CountProjection
        return v
  _ ->
    error "Value.projectRec, not a record"

num :: Int64 -> Value
num = B0 . Num

trueTag, falseTag, consTag, nilTag, noneTag, someTag  :: Tag

trueTag = Tag "True"
falseTag = Tag "False"

consTag = Tag "Cons"
nilTag = Tag "Nil"

noneTag = Tag "None"
someTag = Tag "Some"

mkTag :: LF.VariantConName -> Tag
mkTag = Tag . Text.unpack . LF.unVariantConName

deListValue :: Value -> Maybe (Value,Value)
deListValue = \case
  Constructed (Tag "Nil") [] -> Nothing
  Constructed (Tag "Cons") [x,xs] -> Just (x,xs)
  v -> error $ "deListValue, " <> show v


data Effect a where
  Ret :: a -> Effect a
  Bind :: Effect a -> (a -> Effect b) -> Effect b
  CountApp :: Effect ()
  CountPrim :: Effect ()
  CountProjection :: Effect ()

instance Functor Effect where fmap = liftM
instance Applicative Effect where pure = return; (<*>) = ap
instance Monad Effect where return = Ret; (>>=) = Bind

run :: Effect a -> (a,Counts)
run = loop counts0 where
  loop :: Counts -> Effect a -> (a,Counts)
  loop counts = \case
    Ret x -> (x,counts)
    Bind e f -> let (v,counts') = loop counts e in loop counts' (f v)
    CountApp -> return $ incApps counts
    CountPrim -> return $ incPrim counts
    CountProjection -> return $ incProjection counts


data Counts = Counts
  { apps :: Int -- beta-applications (function-calls)
  , prims :: Int -- applications of primitive/builtin functions
  , projections :: Int -- applications of primitive/builtin functions
  }
  deriving (Show)

counts0 :: Counts
counts0 = Counts { apps = 0, prims = 0, projections = 0 }

incApps :: Counts -> Counts
incApps x@Counts{apps} = x { apps = apps + 1 }

incPrim :: Counts -> Counts
incPrim x@Counts{prims} = x { prims = prims + 1 }

incProjection :: Counts -> Counts
incProjection x@Counts{projections} = x { projections = projections + 1 }

