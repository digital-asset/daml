-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Daml.LF.Evaluator.Value
  ( Throw(..), Value(..), Func(..), Tag(..), B0(..), B1(..), B2(..), B3(..), FieldName,
    trueTag,falseTag,consTag,nilTag,mkTag,noneTag,someTag,
    bool, num, deNum,
    apply,
    TFunc(..), tapply, -- allows types not to be erased at runtime
    projectRec,
    Effect, run, Counts(..),
  ) where

import Control.Monad (ap,liftM)
import Data.Int (Int64)
import qualified Data.Text as T

import qualified DA.Daml.LF.Ast as LF

newtype Throw = Throw String deriving (Eq,Show)

data Value
  = Function Func
  | TFunction TFunc
  | Record [(FieldName, Value)]
  | Constructed Tag [Value]
  | B0 B0
  | B1 B1
  | B2 B2
  | B2_1 B2 Value
  | B3 B3
  | B3_1 B3 Value
  | B3_2 B3 Value Value
  | UnknownBuiltin LF.BuiltinExpr
  deriving (Show)

newtype Func = Func (Value -> Effect Value)

instance Show Func where show _ = "<func>"

newtype TFunc = TFunc (Effect Value)

instance Show TFunc where show _ = "<tfunc>"

type FieldName = LF.FieldName

newtype Tag = Tag { unTag :: String }
  deriving (Eq,Show)

data B0
  = Unit
  | Num Int64
  | Text T.Text
  deriving (Show)

data B1
  = ERROR
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


tapply :: Value -> Effect Value
tapply = \case
  x@(B1 _) -> return x
  x@(B2 _) -> return x
  x@(B3 _) -> return x
  TFunction (TFunc f) -> do f
  v -> error $ "Value.tapply, " <> show v


apply :: (Value, Value) -> Effect Value
apply = \case
  (Function (Func f), v) -> do CountApp; f v
  (TFunction _,_) -> error "Value.apply, Type-Function"
  (Record _,_) -> error "Value.apply, Record"
  (Constructed _ _,_) -> error "Value.apply, Constructed"
  (B0 _,_) -> error "Value.apply, B0"
  (B1 b,v) -> applyB1 b v
  (B2 b,v1) -> return $ B2_1 b v1
  (B2_1 b v1,v2) -> do CountPrim; return $ applyB2 b v1 v2
  (B3 b,v1) -> return $ B3_1 b v1
  (B3_1 b v1,v2) -> return $ B3_2 b v1 v2
  (B3_2 b v1 v2,v3) -> do CountPrim; applyB3 b v1 v2 v3
  (UnknownBuiltin b,_) -> error $ "Value.apply, UnknownBuiltin, " <> show b

applyB1 :: B1 -> Value -> Effect Value
applyB1 = \case
  ERROR -> \v -> Fail $ T.unpack $ deText v

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

deText :: Value -> T.Text
deText = \case
  B0 (Text t) -> t
  v -> error $ "deText, " <> show v

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
mkTag = Tag . T.unpack . LF.unVariantConName

deListValue :: Value -> Maybe (Value,Value)
deListValue = \case
  Constructed (Tag "Nil") [] -> Nothing
  Constructed (Tag "Cons") [x,xs] -> Just (x,xs)
  v -> error $ "deListValue, " <> show v


data Effect a where
  Ret :: a -> Effect a
  Bind :: Effect a -> (a -> Effect b) -> Effect b
  Fail :: String -> Effect a
  CountApp :: Effect ()
  CountPrim :: Effect ()
  CountProjection :: Effect ()

instance Functor Effect where fmap = liftM
instance Applicative Effect where pure = return; (<*>) = ap
instance Monad Effect where return = Ret; (>>=) = Bind

run :: Effect a -> (Either Throw a,Counts)
run = loop counts0 where
  loop :: Counts -> Effect a -> (Either Throw a,Counts)
  loop counts = \case
    Ret x -> (Right x,counts)
    Fail s -> (Left (Throw s),counts)
    Bind e f -> do
      let (either,counts') = loop counts e
      case either of
        Left throw -> (Left throw,counts')
        Right v -> loop counts' (f v)
    CountApp -> (Right (), incApps counts)
    CountPrim -> (Right (), incPrim counts)
    CountProjection -> (Right (), incProjection counts)


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

