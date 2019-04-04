-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- CI script to build the DA GHC fork and make a ghc-lib out of it

import Control.Monad
import System.Directory
import System.Process.Extra
import System.IO.Extra
import System.Info.Extra
import System.Exit
import System.Time.Extra
import Data.List.Extra

main :: IO ()
main = do

    let cmd x = do
        putStrLn $ "\n\n# Running: " ++ x
        hFlush stdout
        (t, _) <- duration $ system_ x
        putStrLn $ "# Completed in " ++ showDuration t ++ ": " ++ x ++ "\n"
        hFlush stdout
    when isWindows $
        cmd "stack exec -- pacman -S autoconf automake-wrapper make patch python tar --noconfirm"

    let cmdInPythonVenv x = do
        if isWindows
            then cmd $ "py -3 -m venv venv && venv\\Scripts\\activate && " ++ x
            else cmd $ "python3.6 -m venv venv && source venv/bin/activate && " ++ x

    withTempDir $ \tmpDir -> do

        withCurrentDirectory tmpDir $ do
            putStrLn $ "[Info] Entered " ++ tmpDir

            cmd "git clone https://github.com/digital-asset/ghc-lib.git"

            withCurrentDirectory "ghc-lib" $ do
                ghcLibDir <- getCurrentDirectory
                putStrLn $ "[Info] Entered " ++ ghcLibDir
                cmd "git clone https://gitlab.haskell.org/ghc/ghc.git"

                withCurrentDirectory "ghc" $ do
                    ghcDir <- getCurrentDirectory
                    putStrLn $ "[Info] Entered " ++ ghcDir
                    branch <- trim <$> systemOutput_ "git rev-parse --abbrev-ref HEAD"

                    when (branch /= "master") $ do
                        _ <- putStrLn "[Error] Expected origin/master"
                        exitWith $ ExitFailure 1

                    cmd "git remote add upstream https://github.com/digital-asset/ghc.git"
                    cmd "git fetch upstream"
                    base0 <- systemOutput_ "git merge-base upstream/da-master origin/master"
                    base1 <- systemOutput_ "git merge-base upstream/da-unit-ids origin/master"
                    when (base0 /= base1) $ do
                        _ <- putStrLn "[Error] Expected common ancestor"
                        exitWith $ ExitFailure 1

                    cmd $ "git checkout " ++ base0
                    cmd "git merge --no-edit upstream/da-master"
                    cmd "git submodule update --init --recursive"

                    cmd "stack build --stack-yaml=hadrian/stack.yaml --only-dependencies --no-terminal --interleaved-output"
                    if isWindows
                        then cmdInPythonVenv "hadrian/build.stack.bat --configure --flavour=quickest -j"
                        else cmdInPythonVenv "hadrian/build.stack.sh --configure --flavour=quickest -j"

                    cmd "stack exec --no-terminal -- _build/stage1/bin/ghc --version"
                    cmd "git merge --no-edit upstream/da-unit-ids"
                    putStrLn $ "[Info] Leaving " ++ ghcDir

                cmd "stack setup > /dev/null 2>&1"
                cmd "stack build --no-terminal --interleaved-output"
                cmdInPythonVenv "stack exec --no-terminal -- ghc-lib-gen ghc --ghc-lib-parser"
                stackYaml <- readFile' "stack.yaml"
                writeFile "stack.yaml" $ stackYaml ++ unlines ["- ghc"]
                cmd "stack sdist ghc --tar-dir=."

                cmd "cd ghc && git clean -xf && git checkout ."
                cmdInPythonVenv "stack exec --no-terminal -- ghc-lib-gen ghc --ghc-lib"
                cmd "stack sdist ghc --tar-dir=."

                cmd "tar -xf ghc-lib-parser-0.1.0.tar.gz"
                cmd "tar -xf ghc-lib-0.1.0.tar.gz"
                cmd "mv ghc-lib-parser-0.1.0 ghc-lib-parser"
                cmd "mv ghc-lib-0.1.0 ghc-lib"
                removeFile "ghc/ghc-lib.cabal"

                writeFile "stack.yaml" $
                   stackYaml ++
                    unlines [ "- ghc-lib-parser"
                            , "- ghc-lib"
                            , "- examples/mini-hlint"
                            , "- examples/mini-compile"
                            ]

                -- Replace `ghc-prim` with `daml-prim`; this avoids an error importing `GHC.Prim` in MiniCompileTest.hs.
                cmd "sed -i.bak s/\"ghc-prim\"/\"daml-prim\"/g examples/mini-compile/src/Main.hs"

                cmd "stack build --no-terminal --interleaved-output"
                cmd "stack exec --no-terminal -- ghc-lib --version"
                cmd "stack exec --no-terminal -- mini-hlint examples/mini-hlint/test/MiniHlintTest.hs"
                cmd "stack exec --no-terminal -- mini-hlint examples/mini-hlint/test/MiniHlintTest_error_handling.hs"
                cmd "stack exec --no-terminal -- mini-compile examples/mini-compile/test/MiniCompileTest.hs"
                putStrLn $ "[Info] Leaving " ++ ghcLibDir
