exec python3 -c "import pty,sys; pty.spawn(sys.argv[1:])" "$@"
