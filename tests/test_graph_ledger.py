"""Tests for InMemoryGraphLedger ID consistency and mapping behavior."""

from typing import Any

import pytest

from ttasks.ledger import InMemoryGraphLedger
from ttasks.workflow import TaskGraph


def test_graph_ledger_rejects_non_graph_values() -> None:
    """Only TaskGraph instances can be stored in the graph ledger."""
    ledger = InMemoryGraphLedger()
    not_a_graph: Any = "not a graph"

    with pytest.raises(TypeError, match="Expected TaskGraph, got str"):
        ledger["id"] = not_a_graph

    assert "id" not in ledger


def test_graph_ledger_rejects_graph_id_mismatch() -> None:
    """A graph cannot be stored under an ID that differs from graph.id."""
    ledger = InMemoryGraphLedger()
    graph = TaskGraph(title="Build")

    with pytest.raises(ValueError, match="graph_id must match graph.id"):
        ledger["wrong-id"] = graph

    assert "wrong-id" not in ledger
    assert graph.id not in ledger


def test_graph_ledger_accepts_graph_under_its_own_id() -> None:
    """A graph stored under graph.id is retrievable by that same ID."""
    ledger = InMemoryGraphLedger()
    graph = TaskGraph(title="Build")

    ledger[graph.id] = graph

    assert graph.id in ledger
    assert ledger[graph.id] is graph


def test_graph_ledger_iterates_in_insertion_order() -> None:
    """Iterating a graph ledger yields graphs in insertion order."""
    ledger = InMemoryGraphLedger()
    first = TaskGraph(title="First")
    second = TaskGraph(title="Second")

    ledger[first.id] = first
    ledger[second.id] = second

    assert list(ledger) == [first, second]


def test_graph_ledger_repr_includes_graph_count() -> None:
    """The graph ledger repr summarizes the number of stored graphs."""
    ledger = InMemoryGraphLedger()

    assert repr(ledger) == "InMemoryGraphLedger(0 graphs)"


def test_graph_ledger_get_missing_graph_raises_key_error() -> None:
    """Reading a missing graph preserves dictionary KeyError behavior."""
    ledger = InMemoryGraphLedger()

    with pytest.raises(KeyError):
        ledger["missing"]


def test_graph_ledger_del_removes_graph() -> None:
    """Deleting from the graph ledger removes the graph entirely."""
    ledger = InMemoryGraphLedger()
    graph = TaskGraph(title="Build")
    ledger[graph.id] = graph

    del ledger[graph.id]

    assert graph.id not in ledger
    assert len(ledger) == 0


def test_graph_ledger_delete_missing_graph_raises_key_error() -> None:
    """Deleting a missing graph preserves dictionary KeyError behavior."""
    ledger = InMemoryGraphLedger()

    with pytest.raises(KeyError):
        del ledger["missing"]


def test_graph_id_cannot_change_after_storing_in_graph_ledger() -> None:
    """Read-only graph IDs prevent ledger/graph identity drift."""
    ledger = InMemoryGraphLedger()
    graph = TaskGraph(title="Build")
    graph_id = graph.id
    ledger[graph.id] = graph

    attr = "id"
    with pytest.raises(AttributeError):
        setattr(graph, attr, "new-id")

    assert graph.id == graph_id
    assert ledger[graph_id] is graph
