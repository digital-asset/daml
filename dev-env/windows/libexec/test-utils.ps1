function da_random_dadew_home {
    $path = $PSScriptRoot + "../../../../target/dadew/" + ([guid]::NewGuid())
    mkdir -p $path
    $env:DADEW = Resolve-Path -LiteralPath $path
}

function da_clear_random_dadew_home {
    $env:DADEW = $null
}