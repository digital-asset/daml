-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Daml.LF.Evaluator.Eval (runIntProgArg) where

import Control.Monad (forM,liftM,ap)
import DA.Daml.LF.Evaluator.Exp (Exp,Alt,Prog)
import DA.Daml.LF.Evaluator.Value (Value,Throw,Counts)
import Data.Map.Strict (Map)
import Data.Int (Int64)
import qualified DA.Daml.LF.Evaluator.Exp as Exp
import qualified DA.Daml.LF.Evaluator.Value as Value
import qualified Data.Map.Strict as Map


runIntProgArg :: Prog -> Int64 -> (Either Throw Int64,Counts)
runIntProgArg Exp.Prog{defs,start} arg = do
  run defs $ do
    fv <- eval start
    let av = Value.num arg
    v <- ValueEffect $ Value.apply (fv,av)
    return $ Value.deNum v


eval :: Exp -> Effect Value
eval = \case
  Exp.Lit v -> return v

  Exp.Var x -> do
    env <- GetEnv
    case Map.lookup x env of
      Nothing -> error $ "eval, " <> show x
      Just v -> return v

  Exp.App e1 e2 -> do
    v1 <- eval e1
    v2 <- eval e2
    ValueEffect $ Value.apply (v1,v2)

  Exp.Lam x _ body -> do
    env <- GetEnv
    run <- RunContext
    return $ Value.Function $ Value.Func $ \arg ->
      run $ ModEnv (\_ -> Map.insert x arg env) $ eval body

  Exp.Let x rhs body -> do
    v <- eval rhs
    ModEnv (Map.insert x v) $ eval body

  Exp.Rec elems -> do
    xs <- forM elems $ \(name,e) -> do
      v <- eval e
      return (name,v)
    return $ Value.Record xs

  Exp.Dot exp name -> do
    v <- eval exp
    ValueEffect $ Value.projectRec name v

  Exp.Con tag elems -> do
    vs <- mapM eval elems
    return $ Value.Constructed tag vs

  Exp.Match{scrut,alts} -> do
    v <- eval scrut
    evalAlts v alts

  Exp.Ref i -> do
    exp <- GetDef i
    eval exp

  -- This has behaviour of all types being erased at runtime:
  --   Exp.TypeLam _ exp -> eval exp
  --   Exp.TypeApp exp _ -> eval exp

  -- In this version we don't erase types at runtime, and so an unapplied type-abstraction is
  -- treated as a value, and wont have an effect (i.e. error) until and unless it is applied.

  Exp.TypeLam _ body -> do
    env <- GetEnv
    run <- RunContext
    return $ Value.TFunction $ Value.TFunc $
      run $ ModEnv (\_ -> env) $ eval body

  Exp.TypeApp exp _ -> do
    v <- eval exp
    ValueEffect $ Value.tapply v


evalAlts :: Value -> [Alt] -> Effect Value
evalAlts v = \case
  [] -> error "no alternatives match"
  alt:alts ->
    case evalAlt v alt of
      Nothing -> evalAlts v alts
      Just effect -> effect

evalAlt :: Value -> Alt -> Maybe (Effect Value)
evalAlt v = \case
  Exp.Alt{tag,bound=xs,rhs} -> case v of
    Value.Constructed tag' vs
      | tag /= tag' -> Nothing
      | length xs /= length vs -> error $ "evalAlt, mismatch length, " <> Value.unTag tag
      | otherwise -> do
          let f env = foldr (\(x,v) -> Map.insert x v) env (zip xs vs)
          Just $ ModEnv f $ eval rhs
    _ ->
      error $ "evalAlt, unexpected value, " <> show v


data Effect a where
  Ret :: a -> Effect a
  Bind :: Effect a -> (a -> Effect b) -> Effect b
  GetEnv :: Effect Env
  ModEnv :: (Env -> Env) -> Effect a -> Effect a
  GetDef :: Int -> Effect Exp
  ValueEffect :: Value.Effect a -> Effect a
  RunContext :: Effect (Effect b -> Value.Effect b)

instance Functor Effect where fmap = liftM
instance Applicative Effect where pure = return; (<*>) = ap
instance Monad Effect where return = Ret; (>>=) = Bind


run :: Exp.Defs -> Effect a -> (Either Throw a, Counts)
run defs e = Value.run $ run env0 e
  where
    run :: Env -> Effect a -> Value.Effect a
    run env = \case
      Ret x -> return x
      Bind e f -> do v <- run env e; run env (f v)
      GetEnv -> return env
      ModEnv f e -> run (f env) e
      GetDef i -> return $ getDef i
      ValueEffect x -> x
      RunContext -> return $ run env

    getDef :: Int -> Exp
    getDef i =
      case Map.lookup i defs of
        Nothing -> error $ "getDef, " <> show i
        Just (_,exp) -> exp


type Env = Map Exp.Var Value

env0 :: Env
env0 = Map.empty

