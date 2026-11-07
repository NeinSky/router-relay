#!/usr/bin/env bash
#
# Repository policy checks that complement gitleaks.
#
# gitleaks answers "does anything in this repository look like a secret?".
# This script answers the project-specific questions a generic scanner cannot:
# are the local-only files still untracked, are the .gitignore rules still
# doing their job, and did a credential ever slip into a tracked file?
#
# It is deliberately self-contained (git + grep only) so the same checks run in
# CI and on a developer machine:
#
#     bash scripts/check-repo-policy.sh
#
# Exit code 0 = clean, 1 = policy violation.

set -euo pipefail

fail=0

err() {
	printf '::error::%s\n' "$1"
	fail=1
}

# --- 1. Local-only paths must never be tracked ------------------------------
# These files exist on a developer machine but carry the live api_key or
# generated runtime state. .gitignore keeps them out; this confirms it actually
# happened, and catches the case of a file added with `git add -f`.
untracked_only() {
	local pattern="$1" why="$2" hits
	hits=$(git ls-files -- "$pattern")
	if [ -n "$hits" ]; then
		err "$pattern is tracked but must stay local ($why)"
		printf '%s\n' "$hits" | sed 's/^/  - /'
	fi
}

printf '%s\n' '==> local-only paths'
untracked_only 'conf.txt'       'it holds the live api_key'
untracked_only '.env'           'it holds credentials'
untracked_only '*.env'          'it holds credentials'
untracked_only 'cline-router/*' 'it holds Cline sessions, SQLite databases and settings/providers.json with the key'
untracked_only '*.log'          'runtime logs are generated, not source'
untracked_only '*.bak*'         'editor backups are not source'
untracked_only 'node_modules/*' 'dependencies are never vendored'

# --- 2. The .gitignore rules must still be active ---------------------------
# A behavioural probe is used instead of grepping .gitignore, so reformatting or
# reordering the file cannot break this check while leaving the rules dead.
printf '%s\n' '==> .gitignore rules'
while IFS= read -r probe; do
	[ -z "$probe" ] && continue
	if ! git check-ignore -q -- "$probe"; then
		err "$probe is no longer ignored - a rule in .gitignore was lost"
	fi
done <<'PROBES'
conf.txt
.env
cline-router/data/settings/providers.json
cline-router/data/db/sessions.db
router-relay.log
PROBES

# --- 3. Credential shapes must not appear in tracked content ----------------
# gitleaks carries the deep rule set and scans all of history; this is a fast
# second net for the token formats that matter here, over the current tree.
printf '%s\n' '==> credential patterns in tracked files'
pattern='(sk-[A-Za-z0-9_-]{20,}'
pattern+='|gh[pousr]_[A-Za-z0-9]{20,}'
pattern+='|github_pat_[A-Za-z0-9_]{20,}'
pattern+='|AKIA[0-9A-Z]{16}'
pattern+='|xox[baprs]-[A-Za-z0-9-]{10,}'
pattern+='|-----BEGIN [A-Z ]*PRIVATE KEY-----)'
if git grep -n -I -E "$pattern" -- .; then
	err 'a credential-like string is present in tracked files'
fi

# --- summary ---------------------------------------------------------------
if [ "$fail" -ne 0 ]; then
	printf '\n%s\n' 'FAILED: repository policy violations found (see the errors above).'
	exit 1
fi
printf '\n%s\n' 'OK: no policy violations.'
