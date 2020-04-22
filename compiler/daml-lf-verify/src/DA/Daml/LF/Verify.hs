-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Static verification of DAML packages.
module DA.Daml.LF.Verify ( main ) where

import Options.Applicative
import Data.Maybe (fromJust)

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Verify.Generate
import DA.Daml.LF.Verify.Solve
import DA.Daml.LF.Verify.Read
import DA.Daml.LF.Verify.Context
import DA.Pretty
-- import qualified Data.NameMap as NM

-- TODO: temporarily hardcoded
templName :: TypeConName
templName = TypeConName ["Iou"]

choiceName :: ChoiceName
choiceName = ChoiceName "Iou_Merge"

fieldName :: FieldName
fieldName = FieldName "amount"

main :: IO ()
main = do
  Options{..} <- execParser optionsParserInfo
  pkgs <- readPackages optInputDars
  -- mapM_ (putStrLn . renderPretty) (concat (map (NM.toList . packageModules . fst . snd) pkgs))
  putStrLn "Start value phase" >> case runEnv (genPackages ValuePhase pkgs) emptyEnv of
    Left err-> putStrLn "Value phase finished with error: " >> print err
    Right env1 -> putStrLn "Start value solving" >>
      let env2 = solveValueUpdatesEnv env1
      in putStrLn "Start template phase" >> case runEnv (genPackages TemplatePhase pkgs) env2 of
        Left err -> putStrLn "Template phase finished with error: " >> print err
        Right env3 -> do
          putStrLn "Success!"
          let upds = fromJust $ lookupChoInHMap (_envchs env3) choiceName
          mapM_ (\cre -> putStrLn "Create: " >> print (qualObject $ _creTemp cre) >> printFExpr (_creField cre) >> putStrLn "") (_usCre upds)
          mapM_ (\arc -> putStrLn "Archive: " >> print (qualObject $ _arcTemp arc) >> printFExpr (_arcField arc) >> putStrLn "") (_usArc upds)
          mapM_ (\cho -> putStrLn "Choice: " >> print (_choName cho) >> putStrLn "") (_usCho upds)
          putStrLn "Start constraint solving phase"
          let cset = constructConstr env3 templName choiceName fieldName
          putStr "Create: " >> print (_cCres cset)
          putStr "Archive: " >> print (_cArcs cset)

printFExpr :: [(FieldName, Expr)] -> IO ()
printFExpr fields = mapM_ (\(f,e) -> putStrLn (show f ++ " : " ++ renderPretty e)) fields
