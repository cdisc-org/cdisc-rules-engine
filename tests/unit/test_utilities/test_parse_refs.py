"""Tests for the $utils.parse_refs JSONata function (resources/jsonata/parse_refs.jsonata).

The function is loaded the way the engine loads it (JSONataProcessor.get_custom_functions), so a
file the engine could not load fails here too.
"""

import copy
import json
import os
import random

import pytest
from jsonata import Jsonata
from yaml import safe_load

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.utilities.jsonata_processor import JSONataProcessor

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_MARKER = "circularReference"


@pytest.fixture(scope="module")
def utils():
    return JSONataProcessor.get_custom_functions(
        "utils", os.path.join(_REPO_ROOT, DefaultFilePaths.JSONATA_UTILS.value)
    )


def _parse_refs(utils, within, ref="ref", to="resolved"):
    # The input goes through a variable, as in the rules: in $utils.parse_refs(..., within) a bare
    # path argument would be evaluated against $utils, not the document.
    expr = f"$w := within; $utils.parse_refs({json.dumps(ref)}, {json.dumps(to)}, $w)"
    return Jsonata(f"(\n{utils}{expr}\n)").evaluate({"within": within})


def _links(pairs):
    """[("A", "B"), ...] -> elements A, ... whose single ref points at the second id."""
    return [{"id": a, "ref": b} for a, b in pairs]


def _walk(node, to="resolved"):
    """Follow single references: ["A", "B", "loop:A"] for A -> B -> marker for A."""
    out = []
    while node is not None:
        if _MARKER in node:
            out.append(f"loop:{node[_MARKER]}")
            break
        out.append(node["id"])
        node = node.get(to)
    return out


def _by_id(result):
    return {element["id"]: element for element in result}


def test_functions_load_like_the_engine_loads_them(utils):
    # The engine strips the first "{" and the last "}" of each file and joins them into one
    # object, so a comment must sit inside the braces and contain neither.
    assert Jsonata(f"(\n{utils}$keys($utils)\n)").evaluate({}) == [
        "parse_refs",
        "sift_tree",
    ]


class TestWithoutLoops:
    def test_chain_nests_resolved_copies_and_replaces_the_key(self, utils):
        within = [
            {"id": "A", "ref": "B", "z": 1},
            {"id": "B", "ref": "C"},
            {"id": "C", "ref": None},
        ]

        result = _parse_refs(utils, within)

        assert result == [
            {"id": "A", "z": 1, "resolved": {"id": "B", "resolved": {"id": "C"}}},
            {"id": "B", "resolved": {"id": "C"}},
            {"id": "C"},
        ]
        assert list(result[0]) == ["id", "z", "resolved"], "the new key goes last"
        assert within[0] == {"id": "A", "ref": "B", "z": 1}, "the input is not modified"

    def test_documented_example(self, utils):
        # resources/schema/rule/jsonata_functions.md
        within = [
            {"id": "Object_1", "name": "OBJ1", "nextId": "Object_2"},
            {"id": "Object_2", "name": "OBJ2", "nextId": "Object_3"},
            {"id": "Object_3", "name": "OBJ3", "nextId": None},
        ]

        result = _parse_refs(utils, within, "nextId", "next")

        obj3 = {"id": "Object_3", "name": "OBJ3"}
        obj2 = {"id": "Object_2", "name": "OBJ2", "next": obj3}
        assert result == [{"id": "Object_1", "name": "OBJ1", "next": obj2}, obj2, obj3]

    @pytest.mark.parametrize(
        ("ref", "want"),
        [
            ("B", {"id": "B"}),
            (["B"], {"id": "B"}),
            (["B", "C"], [{"id": "B"}, {"id": "C"}]),
            (["missing", "C"], {"id": "C"}),
            ("missing", None),
            ([], None),
            (None, None),
        ],
        ids=[
            "string",
            "one-item list",
            "list",
            "missing in list",
            "missing",
            "empty",
            "null",
        ],
    )
    def test_reference_shapes(self, utils, ref, want):
        within = [{"id": "A", "ref": ref}, {"id": "B"}, {"id": "C"}]

        got = _parse_refs(utils, within)[0]

        assert got == ({"id": "A"} if want is None else {"id": "A", "resolved": want})

    def test_nulls_and_nested_containers_are_kept(self, utils):
        # jsonata-python's $each and $map drop nulls, so an implementation that rebuilds objects
        # by hand would lose e.g. previousId: null, which rules test for.
        target = {
            "id": "B",
            "previousId": None,
            "one": [{"k": None}],
            "nulls": [None],
            "empty": {},
        }
        within = [{"id": "A", "previousId": None, "ref": "B"}, target]

        result = _parse_refs(utils, within)

        assert result == [{"id": "A", "previousId": None, "resolved": target}, target]

    def test_shared_target_is_copied_under_each_holder(self, utils):
        within = [
            {"id": "A", "ref": ["B", "C"]},
            {"id": "B", "ref": "D"},
            {"id": "C", "ref": "D"},
            {"id": "D"},
        ]

        result = _parse_refs(utils, within)

        d = {"id": "D"}
        assert result[0] == {
            "id": "A",
            "resolved": [{"id": "B", "resolved": d}, {"id": "C", "resolved": d}],
        }

    def test_repeated_id_resolves_to_every_element_with_it(self, utils):
        within = [{"id": "A", "ref": "B"}, {"id": "B", "n": 1}, {"id": "B", "n": 2}]

        result = _parse_refs(utils, within)

        assert result[0]["resolved"] == [{"id": "B", "n": 1}, {"id": "B", "n": 2}]

    def test_non_string_ids_still_resolve(self, utils):
        within = [{"id": "A", "ref": [1, "B"]}, {"id": 1}, {"id": "B"}]

        result = _parse_refs(utils, within)

        assert result[0]["resolved"] == [{"id": 1}, {"id": "B"}]

    @pytest.mark.parametrize(
        "within",
        [[{"id": "A"}], [{"id": "A", "x": [1, {"y": None}]}, "text", None]],
        ids=["no refs", "mixed"],
    )
    def test_input_without_refs_is_unchanged(self, utils, within):
        assert _parse_refs(utils, within) == within

    def test_long_chain(self, utils):
        n = 120
        within = _links((f"n{i}", f"n{i + 1}") for i in range(n - 1))
        within.append({"id": f"n{n - 1}"})

        result = _parse_refs(utils, within)

        assert _walk(result[0]) == [f"n{i}" for i in range(n)]


class TestLoops:
    @pytest.mark.parametrize("length", range(1, 8))
    def test_loop_ends_at_its_first_repeat_from_every_element(self, utils, length):
        # Loops of length 3, 5, 6 and 7 used to recurse until RecursionError.
        ids = [f"n{i}" for i in range(length)]
        within = _links((ids[i], ids[(i + 1) % length]) for i in range(length))

        result = _by_id(_parse_refs(utils, within))

        for i, start in enumerate(ids):
            cycle = [ids[(i + k) % length] for k in range(length)]
            assert _walk(result[start]) == cycle + [f"loop:{start}"]

    def test_repeating_cycle_on_a_timeline(self, utils):
        # Screening, then a treatment cycle whose decision instance defaults back to its first
        # visit: a loop of three, as used by DDF00087 and DDF00088.
        pairs = [
            ("S1", "S2"),
            ("S2", "D1"),
            ("D1", "T1"),
            ("T1", "T2"),
            ("T2", "D2"),
            ("D2", "T1"),
        ]
        within = [{"id": a, "defaultConditionId": b} for a, b in pairs]

        result = _by_id(
            _parse_refs(utils, within, "defaultConditionId", "defaultCondition")
        )

        def walk(start):
            return _walk(result[start], "defaultCondition")

        assert walk("S1") == ["S1", "S2", "D1", "T1", "T2", "D2", "loop:T1"]
        assert walk("T2") == ["T2", "D2", "T1", "loop:T2"]
        assert walk("D2") == ["D2", "T1", "T2", "loop:D2"]
        assert "defaultConditionId" not in json.dumps(result)

    def test_path_into_a_loop_keeps_every_element(self, utils):
        # The previous implementation ended here but marked E's own reference, dropping A.
        within = _links([("E", "A"), ("A", "A")])

        result = _by_id(_parse_refs(utils, within))

        assert _walk(result["E"]) == ["E", "A", "loop:A"]
        assert _walk(result["A"]) == ["A", "loop:A"]

    def test_only_the_looping_branch_is_cut(self, utils):
        within = [
            {"id": "D", "ref": ["T1", "F1"]},
            {"id": "T1", "ref": "T2"},
            {"id": "T2", "ref": "D"},
            {"id": "F1", "ref": "F2"},
            {"id": "F2"},
        ]

        result = _by_id(_parse_refs(utils, within))

        assert result["D"] == {
            "id": "D",
            "resolved": [
                {"id": "T1", "resolved": {"id": "T2", "resolved": {_MARKER: "D"}}},
                {"id": "F1", "resolved": {"id": "F2"}},
            ],
        }

    def test_a_loop_reached_twice_is_cut_on_each_path(self, utils):
        within = [
            {"id": "A", "ref": ["L1", "L2"]},
            {"id": "L1", "ref": "L2"},
            {"id": "L2", "ref": "L1"},
        ]

        result = _by_id(_parse_refs(utils, within))

        assert result["A"]["resolved"] == [
            {"id": "L1", "resolved": {"id": "L2", "resolved": {_MARKER: "L1"}}},
            {"id": "L2", "resolved": {"id": "L1", "resolved": {_MARKER: "L2"}}},
        ]

    def test_holder_nested_inside_the_element_it_refers_to(self, utils):
        # T holds no reference itself; its child does, back to T. The loop is caught because the
        # path holds T's id once T is resolved, not only the ids of reference holders. At the top
        # level T's own copy is not on the path, so its child resolves T once before the marker.
        within = [
            {"id": "A", "ref": "T"},
            {"id": "T", "child": {"id": "c", "ref": "T"}},
        ]

        result = _by_id(_parse_refs(utils, within))

        looped = {"id": "T", "child": {"id": "c", "resolved": {_MARKER: "T"}}}
        assert result["A"] == {"id": "A", "resolved": looped}
        assert result["T"] == {"id": "T", "child": {"id": "c", "resolved": looped}}

    def test_single_object_within(self, utils):
        within = {"id": "A", "ref": "A", "x": None}

        result = _parse_refs(utils, within)

        assert result == {"id": "A", "x": None, "resolved": {_MARKER: "A"}}

    def test_long_loop(self, utils):
        n = 80
        within = _links((f"n{i}", f"n{(i + 1) % n}") for i in range(n))

        result = _parse_refs(utils, within)

        assert _walk(result[0]) == [f"n{i}" for i in range(n)] + ["loop:n0"]


@pytest.mark.parametrize("seed", range(4))
def test_random_graphs_match_the_reference_model(utils, seed):
    rng = random.Random(seed)
    for _ in range(80):
        within = _random_graph(rng, acyclic=rng.random() < 0.3)

        assert _parse_refs(utils, within) == _reference_parse_refs(within), within


@pytest.fixture
def engine_functions(monkeypatch):
    """Let the engine load the real functions: they load relative to cwd, and other tests
    cache a mocked set in get_all_custom_functions."""
    monkeypatch.chdir(_REPO_ROOT)
    JSONataProcessor.get_all_custom_functions.cache_clear()
    yield
    JSONataProcessor.get_all_custom_functions.cache_clear()


def test_rule_calling_parse_refs_runs_on_a_looping_timeline(engine_functions):
    # Before the fix this raised RuleExecutionError (RecursionError).
    rule = Rule.from_cdisc_metadata(safe_load("""
        Check: |
            (
                $instances := study.instances;
                $parsed := $utils.parse_refs("defaultConditionId", "defaultCondition", $instances);
                $parsed[id = "S1"].{
                    "instanceType": instanceType,
                    "id": id,
                    "order": $join(**.id, " > "),
                    "loop": **.circularReference
                }
            )
        Core:
            Id: parse_refs loop test
        Status: Draft
        Outcome:
            Message: "Walk from the first instance"
            Output Variables:
                - order
                - loop
        Rule Type: JSONata
        Scope:
            Entities:
                Include:
                    - ALL
        Sensitivity: Record
        """))
    pairs = [("S1", "T1"), ("T1", "T2"), ("T2", "D"), ("D", "T1")]
    instances = [
        {"id": a, "instanceType": "ScheduledActivityInstance", "defaultConditionId": b}
        for a, b in pairs
    ]

    result = JSONataProcessor.execute_jsonata_rule(
        rule=rule,
        dataset={"study": {"instances": instances}},
        jsonata_custom_functions=(),
    )

    values = [error["value"] for container in result for error in container["errors"]]
    assert values == [
        {
            "instanceType": "ScheduledActivityInstance",
            "id": "S1",
            "order": "S1 > T1 > T2 > D",
            "loop": "T1",
        }
    ]


def _reference_parse_refs(within):
    return _ReferenceModel(within).resolve(within, [])


class _ReferenceModel:
    """Python model of parse_refs, for "ref" -> "resolved".

    Follows references depth first. The path holds the id of each reference holder and of each
    resolved element on the way down; a reference to an id on it becomes the marker. It
    reproduces JSONata's sequence rules: null references and missing ids are dropped, a single
    result is a value and not a list, and a repeated id resolves to the list of its elements.
    """

    def __init__(self, within):
        self.elements = within if isinstance(within, list) else [within]

    def resolve(self, node, path):
        node = copy.deepcopy(node)
        self.visit(node, path)
        return node

    def visit(self, value, path):
        if isinstance(value, list):
            for item in value:
                self.visit(item, path)
        elif isinstance(value, dict):
            for key, item in list(value.items()):
                if key != "ref":
                    self.visit(item, path)
            if "ref" in value:
                self.replace(value, path + ([value["id"]] if "id" in value else []))

    def replace(self, holder, here):
        refs = holder.pop("ref")
        out = [
            target
            for ref in (refs if isinstance(refs, list) else [refs])
            if ref is not None
            for target in [self.lookup(ref, here)]
            if target is not None
        ]
        if out:
            holder["resolved"] = out[0] if len(out) == 1 else out

    def lookup(self, ref, here):
        if ref in here:
            return {_MARKER: ref}
        targets = [
            e for e in self.elements if isinstance(e, dict) and e.get("id") == ref
        ]
        if not targets:
            return None
        return self.resolve(targets[0] if len(targets) == 1 else targets, here + [ref])


def _random_value(rng, depth=0):
    roll = rng.random()
    if roll < 0.2:
        return None
    if roll < 0.4:
        return rng.choice(["x", "", 0, 1.5, True, False])
    if roll < 0.55 and depth < 2:
        return [_random_value(rng, depth + 1) for _ in range(rng.randint(0, 2))]
    if roll < 0.7 and depth < 2:
        return {
            f"k{i}": _random_value(rng, depth + 1) for i in range(rng.randint(0, 2))
        }
    return "v"


def _random_ref(rng, pool):
    roll = rng.random()
    if roll < 0.06:
        return None
    if roll < 0.12:
        return "missing"
    if roll < 0.55 and pool:
        return rng.choice(pool)
    return [rng.choice(pool) for _ in range(rng.randint(0, 3))] if pool else []


def _random_graph(rng, acyclic):
    """Up to six elements with string or list refs, nulls, missing ids and a repeated id."""
    n = rng.randint(1, 6)
    ids = [f"n{i}" for i in range(n)]
    repeated = n > 2 and rng.random() < 0.2
    if repeated:
        ids[-1] = ids[
            -2
        ]  # the last two share an id and hold no ref, so a DAG stays a DAG
    elements = []
    for i, element_id in enumerate(ids):
        element = {"id": element_id}
        for j in range(rng.randint(0, 2)):
            element[f"x{j}"] = _random_value(rng)
        if not (repeated and i >= n - 2) and rng.random() >= 0.15:
            element["ref"] = _random_ref(rng, ids[i + 1 :] if acyclic else ids)
        if rng.random() < 0.3:
            element["z"] = _random_value(rng)
        elements.append(element)
    rng.shuffle(elements)
    return elements
