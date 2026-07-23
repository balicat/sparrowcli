// completion — static shell completion scripts (bash, zsh, fish).
//
// The command/flag tables below are maintained BY HAND next to the flag
// definitions they mirror. That drift risk is accepted: the flag surface is
// small and changes rarely, and static tables keep the scripts dependency-
// free and instant. When adding a command or flag, update this file.
package main

import (
	"fmt"
	"sort"
	"strings"
)

// connection flags shared by every server-facing command (addConnFlags)
var connFlagNames = []string{
	"s", "basic", "bearer", "header", "tls-skip-verify", "tls-cert", "tls-key", "tls-ca",
}

var cmdDesc = map[string]string{
	"connect":    "probe a server and save a connection profile",
	"orient":     "one-shot markdown map: vendor, tables, schemas",
	"ls":         "list tables",
	"info":       "table schema + row count",
	"sql":        "run a Flight SQL statement",
	"query":      "build and run a simple SELECT",
	"head":       "preview the first n rows of a table",
	"pull":       "Direct Pull (1-RTT): a ready ticket straight to the server",
	"profile":    "per-column null/distinct/min/max profile",
	"doctor":     "layered connection diagnosis",
	"check":      "data-quality checks on a table",
	"expect":     "assert something about a query result (exit 1 on violation)",
	"verify":     "re-run a receipt's query and confirm the fingerprint matches",
	"replay":     "re-run a recorded session and confirm every step reproduces",
	"diff":       "compare a table across two servers",
	"audit":      "security surface probe of a server you operate",
	"ping":       "latency percentiles: bare TCP vs warm RPC",
	"feedback":   "send feedback to the sparrow maintainers",
	"profiles":   "list / use / rm saved profiles",
	"completion": "print a shell completion script",
	"agent":      "print a complete agent-ready manual (markdown)",
	"mcp":        "serve the core tools over MCP stdio — for chat agents without a shell",
	"ticket":     "emit a reusable pull ticket (JSON) to save and replay",
	"version":    "print version",
	"help":       "help for a command",
}

// per-command flags beyond the shared connection set (nil = conn flags only)
var cmdOwnFlags = map[string][]string{
	"connect":    {"name"},
	"orient":     nil,
	"ls":         {"o"},
	"info":       {"no-count"},
	"sql":        {"f", "substrait", "o", "max-rows", "encrypt-key", "stats", "ipc", "schema", "bigint-as-string", "cost", "budget", "receipt"},
	"query":      {"cols", "where", "order", "desc", "limit", "o", "max-rows", "encrypt-key", "stats", "ipc", "bigint-as-string", "cost", "budget", "receipt"},
	"head":       {"o"},
	"pull":       {"o", "encrypt-key", "max-rows", "stats", "ipc", "bigint-as-string", "accept-compression", "dry-run", "budget"},
	"profile":    {"o"},
	"doctor":     {"o", "server"},
	"check":      {"key", "time", "value", "max-age", "strict", "fail-on", "show-violations", "approx", "explain", "baseline", "o"},
	"expect":     {"f", "eq", "ne", "gt", "lt", "ge", "le", "rows", "rows-min", "rows-max", "empty", "nonempty", "cols", "o"},
	"verify":     {"o"},
	"replay":     {"o"},
	"diff":       {"against", "time", "o"},
	"audit":      {"o"},
	"ping":       {"n", "o"},
	"feedback":   {"category", "from"},
	"profiles":   {},
	"completion": {},
	"agent":      {"json"},
	"mcp":        {"max-rows"},
	"ticket":     {"series", "start", "end", "pretty"},
	"version":    {},
	"help":       {},
}

// serverCmds get the shared connection flags in addition to their own
var serverCmds = map[string]bool{
	"connect": true, "orient": true, "ls": true, "info": true, "sql": true,
	"query": true, "head": true, "pull": true, "profile": true, "doctor": true, "check": true,
	"expect": true, "verify": true, "replay": true, "diff": true, "audit": true, "ping": true,
	"mcp": true,
}

func completionCommands() []string {
	out := make([]string, 0, len(cmdOwnFlags))
	for c := range cmdOwnFlags {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func flagsFor(cmd string) []string {
	var names []string
	if serverCmds[cmd] {
		names = append(names, connFlagNames...)
	}
	names = append(names, cmdOwnFlags[cmd]...)
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = "--" + n
	}
	return out
}

func cmdCompletion(args []string) error {
	fs := newFlagSet("completion", `usage: sparrow completion bash|zsh|fish
print a completion script for your shell; load it with:
  bash:  source <(sparrow completion bash)      (or drop into bash_completion.d)
  zsh:   source <(sparrow completion zsh)       (or a file in your $fpath)
  fish:  sparrow completion fish > ~/.config/fish/completions/sparrow.fish`)
	pos := parseFlags(fs, args)
	if len(pos) != 1 {
		return usagef("usage: sparrow completion bash|zsh|fish")
	}
	cmds := completionCommands()
	switch pos[0] {
	case "bash":
		fmt.Println(`# bash completion for sparrow — load with: source <(sparrow completion bash)
_sparrow() {
    local cur cmd
    cur="${COMP_WORDS[COMP_CWORD]}"
    cmd="${COMP_WORDS[1]}"
    if [ "$COMP_CWORD" -eq 1 ]; then
        COMPREPLY=($(compgen -W "` + strings.Join(cmds, " ") + `" -- "$cur"))
        return
    fi
    case "$cmd" in`)
		for _, c := range cmds {
			if f := flagsFor(c); len(f) > 0 {
				fmt.Printf("        %s) COMPREPLY=($(compgen -W \"%s\" -- \"$cur\"));;\n", c, strings.Join(f, " "))
			}
		}
		fmt.Println(`    esac
}
complete -F _sparrow sparrow`)
	case "zsh":
		fmt.Println(`#compdef sparrow
# zsh completion for sparrow — load with: source <(sparrow completion zsh)
# (bash-style alternative: autoload bashcompinit && bashcompinit,
#  then source the bash script instead)
_sparrow() {
    if (( CURRENT == 2 )); then
        compadd ` + strings.Join(cmds, " ") + `
        return
    fi
    case "${words[2]}" in`)
		for _, c := range cmds {
			if f := flagsFor(c); len(f) > 0 {
				fmt.Printf("        %s) compadd -- %s;;\n", c, strings.Join(f, " "))
			}
		}
		fmt.Println(`    esac
}
_sparrow "$@"`)
	case "fish":
		fmt.Println(`# fish completion for sparrow
# install: sparrow completion fish > ~/.config/fish/completions/sparrow.fish
complete -c sparrow -f`)
		for _, c := range cmds {
			fmt.Printf("complete -c sparrow -n __fish_use_subcommand -a %s -d %q\n", c, cmdDesc[c])
		}
		for _, c := range cmds {
			for _, f := range flagsFor(c) {
				fmt.Printf("complete -c sparrow -n \"__fish_seen_subcommand_from %s\" -o %s\n",
					c, strings.TrimPrefix(f, "--"))
			}
		}
	default:
		return usagef("unknown shell %q (bash, zsh, fish)", pos[0])
	}
	return nil
}
