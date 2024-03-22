set nocompatible              " be improved, required
filetype off                  " required

" set the runtime path to include Vundle and initialize
set rtp+=~/.vim/bundle/Vundle.vim
call vundle#begin()
" alternatively, pass a path where Vundle should install plugins
"call vundle#begin('~/some/path/here')

" let Vundle manage Vundle, required
Plugin 'gmarik/Vundle.vim'

" The following are examples of different formats supported.
" Keep Plugin commands between vundle#begin/end.
" plugin on GitHub repo
Plugin 'tpope/vim-fugitive'
Plugin 'ervandew/supertab'
Plugin 'scrooloose/nerdtree'
Plugin 'ctrlpvim/ctrlp.vim.git'
Plugin 'godlygeek/tabular'
Plugin 'ntpeters/vim-better-whitespace'
Plugin 'altercation/vim-colors-solarized'
Plugin 'bling/vim-airline'
Plugin 'spwhitt/vim-nix'
Plugin 'ahf/twelf-syntax'
Plugin 'tpope/vim-repeat'
Plugin 'easymotion/vim-easymotion'


" All of your Plugins must be added before the following line
call vundle#end()            " required
filetype plugin indent on    " required
" To ignore plugin indent changes, instead use:
"filetype plugin on
"
" Brief help
" :PluginList       - lists configured plugins
" :PluginInstall    - installs plugins; append `!` to update or just :PluginUpdate
" :PluginSearch foo - searches for foo; append `!` to refresh local cache
" :PluginClean      - confirms removal of unused plugins; append `!` to auto-approve removal
"
" see :h vundle for more details or wiki for FAQ
" Put your non-Plugin stuff after this line

syntax on


" basic options
set t_vb=
set t_Co=16
set novisualbell
set noerrorbells
set autoindent
set mouse=a
set incsearch
set shiftwidth=2
set sts=2
set expandtab
set nowrap
set gdefault
set tw=78                     " default textwidth is a max of 78
set list                      " enable custom list chars
set listchars=tab:▸\ ,extends:❯,precedes:❮    " replace tabs, eol
set showbreak=↪               " show breaks
set colorcolumn=+1
set number
syntax enable
let g:solarized_termcolors=256
let g:solarized_contrast="high"    "default value is normal
" let g:solarized_hitrail=1
set background=dark
colorscheme solarized
filetype plugin on

" Temporary files
set directory=~/tmp/vim/
set undofile
set undodir=~/tmp/vim/
set backup
set backupcopy=yes
set backupdir=~/tmp/vim/
set backupskip=~/tmp/vim/
set writebackup

let mapleader=","

" ensure that buffer position is restored
function! ResCur()
  if line("'\"") <= line("$")
    normal! g`"
    return 1
  endif
endfunction

augroup resCur
  autocmd!
  autocmd BufWinEnter * call ResCur()
augroup END

" Ctrl-P configuration
let g:ctrlp_user_command = ['.git', 'cd %s && git ls-files --exclude-standard']

" tags configuration
set tags+=./tags,tags;/,

" EasyMotion configuration
let g:EasyMotion_do_mapping = 0 " Disable default mappings

" Bi-directional find motion
" Jump to anywhere you want with minimal keystrokes, with just one key binding.
" `s{char}{label}`
nmap s <Plug>(easymotion-s)
vmap s <Plug>(easymotion-s)
" or
" `s{char}{char}{label}`
" Need one more keystroke, but on average, it may be more comfortable.
" nmap s <Plug>(easymotion-s2)
" vmap s <Plug>(easymotion-s2)

" Turn on case insensitive feature
let g:EasyMotion_smartcase = 1

" JK motions: Line motions
map <Leader>j <Plug>(easymotion-j)
map <Leader>k <Plug>(easymotion-k)

" Easy centering of window in normal and visual modes
nmap <space> zz
vmap <space> zz

" Easy window navigation
map <C-h> <C-w>h
map <C-j> <C-w>j
map <C-k> <C-w>k
map <C-l> <C-w>l

" Quick commands using leader sequences
map <Leader>a :Tabularize //<Left>
map <Leader>p :CtrlPBuffer<CR>
map <Leader>t :NERDTreeToggle<CR>
map <Leader>w :StripWhitespace<CR>:w<CR>
map <Leader>gs :GStatus<CR>
map <F3> :w<CR>:make!<CR>

" Grep options
autocmd QuickFixCmdPost *grep* cwindow " Always open the quickfix window when grepping

" Git grep. Example usage:
" :GG -i 'foobar' -- lib/*.hs
" Grep case-insensitively for the string 'foobar', searching all haskell
" source files located (recursively) under the 'lib' directory.
if !exists(':GG')
  " Ggrep! = git grep without automatically jumping to the first match
  command -nargs=+ GG silent Ggrep! <args>
endif

" Fast searching for word under cursor
map <Leader>gg :GG <cword><CR>
map <Leader>f :cnext<CR>
map <Leader>b :cprevious<CR>

" *.md is Markdown, I don't write Modula2 files ;-)
autocmd BufNewFile,BufRead *.md set filetype=markdown

autocmd BufNewFile,BufRead *.alp set filetype=alp
autocmd BufNewFile,BufRead *.daml set filetype=daml

" Update (or create) tags files when working on haskell
"
" If you do not want tags, you can add
"
" :au! tags
"
" to the end of your personal .vimrc file
" or run it in vim to disable it for the current session
"
" By default tag files are created for
"
" 1. files you've edited
"
" 2. files which are checked into git but are not in the scratch dir
augroup tags
au BufWritePost *.hs  silent !init-tags %
au BufWritePost *.hsc silent !init-tags %
augroup END

" If you use qualified tags, then you have to change iskeyword to include
" a dot.  Unfortunately, that affects a lot of other commands, such as
" w, and \< \> regexes used by * and #.  For me, this is confusing because
" it's inconsistent with vim keys everywhere else. This makes the change
" only affect <c-]>. It might be good to also change this setting for
" :tj and the like, but that exceeds my vim-fu.
nnoremap <silent> <c-]> :setl iskeyword=@,_,.,48-57,39<cr><c-]>
  \:setl iskeyword=@,48-57,_,192-255<cr>
