"""diarios.secrets — the one resolver for the shared cross-cutting secrets file.

INTENT
    Cross-repo API keys (TWOCAPTCHA_API_KEY, DECODO_*, OPENAI_API_KEY, ...) live in ONE
    canonical store outside every project git repo: ~/secrets/secrets.env, an age-managed
    secrets repo (2026-08-28). Consumers used to hardcode the path; this module is the single
    place that knows where the file is — import it rather than re-deriving, the same reason
    research-kit/tools/workspace_root.py exists.

REASONING (the candidate order is not arbitrary)
    ~/secrets/secrets.env is preferred: it is the full, actively-maintained store and resolves
    at the SAME home-relative path under both sandbox runtimes (docker binds host ~/secrets into
    /home/henrik/secrets; apptainer keeps host $HOME), so it needs no env var. The in-tree
    <workspace-root>/.secrets/research.env candidates stay BELOW it as a fallback for a checkout
    that still carries that file, and they cover two of the three places a scraper runs:
      - host session, cwd inside the tree        -> module-relative root, or the CWD walk-up
      - sandbox jailed at the workspace root     -> /workspace/.secrets/research.env
      - sandbox jailed at a project dir          -> NOTHING in-tree resolves, which is exactly
        why HOME_STORE is ranked first. Apptainer auto-mounts two things, $HOME (at its host
        path) and the cwd; it does NOT bind /projects. The parent dirs it synthesises to carry
        the cwd mount are empty stubs — with cwd=projects/procure the container's
        /projects/ec113/henrik/research/ holds only projects/, while packages/, .secrets/ and
        research-kit/ are absent. So both in-tree candidates miss, and diarios cannot even be
        imported from the tree unless it is bound in (or PYTHONPATH'd to a mounted copy).
        Verified 2026-08-30 against claude-sandbox.sif; also in research/rules/secrets.md.
    $RESEARCH_SECRETS wins outright so a test or a one-off can point elsewhere; the legacy
    ~/.config/research/secrets.env stays last so a machine still holding the old file works.
    NOTE ~/research is NOT a candidate: on educloud that path exists but is an unrelated
    stub (same trap workspace_root.py documents) — the real root is /projects/ec113/henrik/research.

ASSUMES nothing: both functions degrade to None when the file is absent, so importing this
    module is safe on a machine with no secrets at all. Callers keep their own missing-creds
    fallback (direct egress, "--proxy given but creds missing", etc.).
"""
from __future__ import annotations

import os
from pathlib import Path

REL = ".secrets/research.env"
HOME_STORE = Path.home() / "secrets/secrets.env"        # canonical age-managed store (2026-08-28+)
LEGACY = Path.home() / ".config/research/secrets.env"   # pre-2026-08-25 location


def secret_files() -> list[Path]:
    """Candidate shared-secret files, most-preferred first (first existing one wins)."""
    cands: list[Path] = []
    if env := os.environ.get("RESEARCH_SECRETS"):
        cands.append(Path(env).expanduser())
    # ~/secrets/secrets.env is the canonical store: a superset of the in-tree file and reachable
    # at the same home-relative path under both sandbox runtimes (docker binds host ~/secrets into
    # /home/henrik/secrets; apptainer keeps host $HOME). Path.home()-relative, so CWD-independent.
    cands.append(HOME_STORE)
    if ws := os.environ.get("RESEARCH_WORKSPACE"):
        cands.append(Path(ws).expanduser() / REL)
    # <root>/packages/diarios/diarios/secrets.py -> <root>  (holds when imported from the tree)
    if len(Path(__file__).resolve().parents) > 3:
        cands.append(Path(__file__).resolve().parents[3] / REL)
    cwd = Path.cwd()
    for d in (cwd, *cwd.parents):   # launch from a subdir, or a non-editable install
        cands.append(d / REL)
    cands.append(LEGACY)
    seen: set[Path] = set()
    return [p for p in cands if not (p in seen or seen.add(p))]


def secrets_path() -> Path | None:
    """First existing shared-secrets file, or None when the machine has none."""
    return next((p for p in secret_files() if p.is_file()), None)


def load_secrets() -> Path | None:
    """Load the shared secrets into os.environ; return the file used (None if nothing loaded).

    Existing environment variables win, so an exported key — or a repo-specific .env loaded
    earlier — always overrides the shared file. Returns None when the file is absent or
    python-dotenv (an optional diarios extra) isn't installed and the vars are ambient already.
    """
    path = secrets_path()
    if path is None:
        return None
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:   # optional extra; vars may already be exported
        return None
    load_dotenv(path)
    return path


if __name__ == "__main__":
    print(secrets_path() or "no shared secrets file found")
