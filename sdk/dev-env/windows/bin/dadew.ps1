param (
    [Parameter(Position=0)][String]$action = "help",
    [Parameter(Position=1)][String]$param1
)

$old_erroractionpreference = $erroractionpreference
$erroractionpreference = 'stop'

. "$PSScriptRoot\..\libexec\dadew.ps1"


switch ( $action ) {
    install {
        da_install
    }
    enable {
        da_enable_win_dev_env
    }
    sync {
        da_sync_scoop -Directory $(If ($param1) { da_resolve_fullpath $param1 } Else { $pwd.Path })
    }
    disable {
        da_reset_path
    }
    uninstall {
        da_uninstall
    }
    version {
        da_version
    }
    where {
        da_where
    }
    which {
        If (!$param1) { da_error("<command> missing") } Else { da_which $param1 }
    }
    required-version {
        da_required_version -Directory $(If ($param1) { da_resolve_fullpath $param1 } Else { $pwd.Path })
    }
    default {
        da_info "Usage: install | enable | sync [directory] | disable | uninstall | version | required-version | where | which <command> | help"
    }
}

$erroractionpreference = $old_erroractionpreference
