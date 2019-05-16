module Development.IDE.Tests (main) where

import DAML.Project.Consts
import Test.Tasty as Tasty
import Test.Tasty.HUnit as Tasty
import Test.Tasty.QuickCheck as Tasty

main :: IO ()
main =
    Tasty.defaultMain $ Tasty.testGroup "Development.IDE.Tests" [fileTests]

-- * File equivalence tests
makeRelative :: FilePath -> IO FilePath
makeRelative fp = withProjectRoot ($ fp)

fileTests :: Tasty.TestTree
fileTests = Tasty.testGroup "File tests"
    [ Test.case "Paths are always equal when they are URIs" $

    ]
