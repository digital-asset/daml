## Building GHC on Windows

These notes detail how to build GHC on a Windows 10 VM.

Make sure your VM has at least 4 CPUS and 4096Mb or more of RAM. This
is critical for performance (if these conditions aren't met, the build
build times are unreasonable).

Start by downloading and installing `git` and `slack` which you can
get from https://git-scm.com/download/win and
https://www.stackage.org/stack/windows-x86_64-installer. You can
happily accept the defaults for both installers.

If at some point you think you might be going to check out the DA git
repository, first set the registry key
`HKLM\SYSTEM\CurrentControlSet\Control\FileSystem\LongPathsEnabled`
and update `C:\git\mingw64\etc\gitconfig` to contain the following:
 ```
   [core]
     longpaths = true
 ```

It is important for the next step that the git
carriage-return/new-line handling policy is set to the UNIX
convention. At a command prompt execute:
```
  git config --global core.autocrlf false
```

Open a command prompt in your `C:\Users\<your-name>` directory and run:
```
  stack setup
  stack install happy alex
  stack exec -- pacman -S gcc binutils git automake-wrapper tar make patch autoconf python3 --noconfirm
  stack exec -- git clone --recursive https://gitlab.haskell.org/ghc/ghc.git
  cd ghc
  stack build --stack-yaml=hadrian/stack.yaml --only-dependencies
  hadrian/build.stack.bat -j --configure --flavour=quickest
```

*[Info : The MSYS2 home directory that `slack` ends up creating for you is in
your filesystem at this path:
`C:\Users\<your-name>\AppData\Local\Programs\stack\x86_64-windows\msys2-20180531\home\<your-name>`.]*
