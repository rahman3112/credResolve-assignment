"""Keeps schema.sql, states.py and docs/state-machines.md from drifting apart.
Pure text checks: no database needed."""

import re
from pathlib import Path

from smartdialer import db
from smartdialer.repo.models import BorrowerStatus
from smartdialer.states import (
    AGENT_ALLOWED_TRANSITIONS,
    CALL_ALLOWED_TRANSITIONS,
    TERMINAL_CALL_STATES,
    AgentState,
    CallState,
    mermaid_edges,
)

SQL = db.schema_sql()
DOC = (Path(__file__).parents[2] / "docs" / "state-machines.md").read_text(encoding="utf-8")


def check_values(constraint: str) -> set[str]:
    m = re.search(rf"CONSTRAINT {constraint}\s+CHECK \(\w+ IN\s*\(([^)]*)\)", SQL)
    assert m, f"constraint {constraint} not found in schema.sql"
    return set(re.findall(r"'([A-Z_]+)'", m.group(1)))


def test_agent_states_match_schema():
    assert check_values("agents_state_chk") == {s.value for s in AgentState}


def test_call_states_match_schema():
    assert check_values("calls_state_chk") == {s.value for s in CallState}


def test_borrower_statuses_match_schema():
    assert check_values("borrowers_status_chk") == {s.value for s in BorrowerStatus}


def test_terminal_states_in_partial_indexes_match_python():
    terminal = {s.value for s in TERMINAL_CALL_STATES}
    lists = re.findall(r"state NOT IN \(([^)]*)\)", SQL)
    assert len(lists) >= 4  # the unique indexes + the reconciler/pacing indexes
    for lst in lists:
        assert set(re.findall(r"'([A-Z_]+)'", lst)) == terminal, lst


README = (Path(__file__).parents[2] / "README.md").read_text(encoding="utf-8")


def _doc_edges(title_marker: str, doc: str = DOC) -> set[str]:
    section = doc.split(title_marker, 1)[1].split("```", 2)[1]
    return {ln.strip() for ln in section.splitlines() if "-->" in ln and "[*]" not in ln}


def test_agent_diagram_in_docs_matches_code():
    assert _doc_edges("<!-- agent-diagram -->") == {e.strip() for e in mermaid_edges(AGENT_ALLOWED_TRANSITIONS)}


def test_call_diagram_in_docs_matches_code():
    assert _doc_edges("<!-- call-diagram -->") == {e.strip() for e in mermaid_edges(CALL_ALLOWED_TRANSITIONS)}


def test_readme_diagrams_match_code_too():
    assert _doc_edges("<!-- agent-diagram -->", README) == {e.strip() for e in mermaid_edges(AGENT_ALLOWED_TRANSITIONS)}
    assert _doc_edges("<!-- call-diagram -->", README) == {e.strip() for e in mermaid_edges(CALL_ALLOWED_TRANSITIONS)}
