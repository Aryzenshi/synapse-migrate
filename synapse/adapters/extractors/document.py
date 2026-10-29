"""PyMongo schema probe using `$sample`, validator merge, Wilson intervals, and bounded stats."""
from __future__ import annotations

import datetime as dt
import math
import random
import re
from collections import Counter
from decimal import Decimal
from typing import Any, Iterable

from synapse.adapters.extractors.base import Extractor
from synapse.domain.heuristics.reverse import wilson_interval
from synapse.domain.ir.document import ArrayProfile, Collection, DeclaredConstraints, DocumentIR, FieldKind, FieldNode, IdProfile, InferenceSource, MongoIndex, NumericStats, RefCandidate, SampleInfo, StringStats, TypeObservation, ValidatorInfo

_MAX_RESERVOIR = 10_000
_MAX_IDS_PER_PATH = 200


def _database_handle(connection: Any) -> Any:
    """Resolve a PyMongo Database without triggering Database.__getattr__.

    PyMongo Database objects dynamically expose unknown attributes as collections,
    so ``getattr(database, "database")`` is not a reliable way to identify the
    database handle: it can return a Collection named ``database``.
    """
    if callable(getattr(connection, "list_collection_names", None)):
        return connection
    database = getattr(connection, "database", None)
    if callable(getattr(database, "list_collection_names", None)):
        return database
    raise TypeError("DocumentExtractor expects a PyMongo Database or Collection-like database handle")


def _bson_type(value: Any) -> str:
    if value is None: return "null"
    if isinstance(value, bool): return "bool"
    if isinstance(value, int): return "int" if -(2**31) <= value < 2**31 else "long"
    if isinstance(value, float): return "double"
    if isinstance(value, Decimal): return "decimal"
    name = type(value).__name__.lower()
    if name == "decimal128": return "decimal"
    if name == "objectid": return "objectId"
    if name in {"binary", "bytes", "bytearray", "memoryview"}: return "binData"
    if name == "datetime" or isinstance(value, dt.datetime): return "date"
    if name == "regex" or hasattr(value, "pattern") and hasattr(value, "flags"): return "regex"
    if isinstance(value, dict): return "object"
    if isinstance(value, (list, tuple)): return "array"
    if isinstance(value, str): return "string"
    if name == "timestamp": return "timestamp"
    if name in {"minKey", "maxKey", "dbref", "dbpointer"}: return name
    return "unknown"


def _kind_for_types(types: Iterable[str]) -> FieldKind:
    types = set(types) - {"null", "undefined"}
    if not types:
        return FieldKind.SCALAR
    kinds = set()
    for bson_type in types:
        if bson_type == "object": kinds.add(FieldKind.OBJECT)
        elif bson_type == "array": kinds.add(FieldKind.ARRAY)
        else: kinds.add(FieldKind.SCALAR)
    return next(iter(kinds)) if len(kinds) == 1 else FieldKind.MIXED


def _numeric(value: Any) -> int | float | Decimal | None:
    if isinstance(value, bool): return None
    if isinstance(value, (int, float, Decimal)): return value
    if type(value).__name__.lower() == "decimal128":
        try: return value.to_decimal()
        except Exception: return None
    return None


def _bounded_add(reservoir: list[Any], value: Any, seen: int, max_size: int = _MAX_RESERVOIR) -> None:
    if len(reservoir) < max_size:
        reservoir.append(value)
        return
    slot = random.Random(seen * 2654435761).randrange(seen + 1)
    if slot < max_size:
        reservoir[slot] = value


def _quantile(values: list[int], q: float) -> int:
    if not values: return 0
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1))]


class _Scope:
    def __init__(self) -> None:
        self.n = 0
        self.fields: dict[str, _FieldStats] = {}

    def add_doc(self, doc: dict[str, Any]) -> None:
        self.n += 1
        for name, value in doc.items():
            stats = self.fields.setdefault(str(name), _FieldStats(str(name)))
            stats.observe(value)


class _FieldStats:
    def __init__(self, name: str) -> None:
        self.name = name
        self.present = 0
        self.nulls = 0
        self.types: Counter[str] = Counter()
        self.strings: list[int] = []
        self.string_max = 0
        self.string_seen = 0
        self.numbers: list[int | float | Decimal] = []
        self.numeric_min: int | float | Decimal | None = None
        self.numeric_max: int | float | Decimal | None = None
        self.all_integer = True
        self.decimal_precision = 0
        self.decimal_scale = 0
        self.decimal_integer_digits = 0
        self.object_scope = _Scope()
        self.array_element_scope = _Scope()
        self.array_lengths: list[int] = []
        self.array_min: int | None = None
        self.array_max: int = 0
        self.array_count = 0
        self.array_empty = 0
        self.element_types: Counter[str] = Counter()
        self.element_objectids: list[Any] = []
        self.scalar_objectids: list[Any] = []
        self.value_count = 0
        self.max_depth = 0

    def observe(self, value: Any, depth: int = 0) -> None:
        self.present += 1
        self.value_count += 1
        self.max_depth = max(self.max_depth, depth)
        kind = _bson_type(value)
        self.types[kind] += 1
        if kind == "null":
            self.nulls += 1
        elif kind == "string":
            length = len(value)
            self.string_max = max(self.string_max, length)
            self.string_seen += 1
            _bounded_add(self.strings, length, self.string_seen)
        elif kind in {"int", "long", "double", "decimal"}:
            number = _numeric(value)
            if number is not None:
                self.numeric_min = number if self.numeric_min is None else min(self.numeric_min, number)
                self.numeric_max = number if self.numeric_max is None else max(self.numeric_max, number)
                self.all_integer = self.all_integer and (isinstance(number, int) or (isinstance(number, (float, Decimal)) and number == int(number)))
                if isinstance(number, Decimal):
                    decimal_value = number
                elif isinstance(number, int):
                    decimal_value = Decimal(number)
                elif isinstance(number, float) and math.isfinite(number):
                    decimal_value = Decimal(str(number)).normalize()
                else:
                    decimal_value = None
                if decimal_value is not None:
                    tup = decimal_value.as_tuple()
                    scale = max(0, -tup.exponent)
                    integer_digits = max(0, len(tup.digits) + tup.exponent)
                    self.decimal_scale = max(self.decimal_scale, scale)
                    self.decimal_integer_digits = max(self.decimal_integer_digits, integer_digits)
                    # Decimal precision is max integer digits + max observed scale,
                    # not merely the largest coefficient length (e.g. 998 and 49.5
                    # need NUMERIC(4,1), not NUMERIC(3,1)).
                    self.decimal_precision = max(self.decimal_precision,
                                                 self.decimal_integer_digits + self.decimal_scale,
                                                 self.decimal_scale)
        elif kind == "object":
            self.object_scope.add_doc(value)
            self.max_depth = max(self.max_depth, _object_depth(value))
        elif kind == "array":
            self.array_count += 1
            length = len(value)
            if length == 0: self.array_empty += 1
            self.array_min = length if self.array_min is None else min(self.array_min, length)
            self.array_max = max(self.array_max, length)
            _bounded_add(self.array_lengths, length, self.array_count)
            for item in value:
                item_type = _bson_type(item)
                self.element_types[item_type] += 1
                if item_type == "object":
                    self.array_element_scope.add_doc(item)
                elif item_type == "objectId":
                    _bounded_add(self.element_objectids, item, len(self.element_objectids) + 1, _MAX_IDS_PER_PATH)
            if len(self.element_objectids) > _MAX_IDS_PER_PATH:
                del self.element_objectids[_MAX_IDS_PER_PATH:]
        elif kind == "objectId":
            _bounded_add(self.scalar_objectids, value, len(self.scalar_objectids) + 1, _MAX_IDS_PER_PATH)


def _object_depth(value: Any) -> int:
    if not isinstance(value, dict):
        return 0
    return 1 + max((_object_depth(child) for child in value.values() if isinstance(child, dict)), default=0)


def _type_observations(counts: Counter[str], denominator: int) -> list[TypeObservation]:
    return [TypeObservation(bson_type=kind, count=count, rate=count / denominator if denominator else 0.0)
            for kind, count in sorted(counts.items())]


def _build_nodes(scope: _Scope, prefix: str, acc_map: dict[str, _FieldStats], source: InferenceSource) -> list[FieldNode]:
    nodes: list[FieldNode] = []
    for name, stats in sorted(scope.fields.items()):
        path = f"{prefix}.{name}" if prefix else name
        denominator = scope.n
        presence = stats.present / denominator if denominator else 0.0
        null_rate = stats.nulls / denominator if denominator else 0.0
        absent = max(0.0, 1.0 - presence)
        observed_non_null = set(stats.types) - {"null", "undefined"}
        kind = _kind_for_types(observed_non_null)
        numeric_types = {"int", "long", "double", "decimal"}
        is_polymorphic = kind == FieldKind.MIXED or (len(observed_non_null) > 1 and not observed_non_null <= numeric_types)
        types = _type_observations(stats.types, denominator)
        lo, hi = wilson_interval(stats.present, denominator)
        children: list[FieldNode] = []
        array_profile = None
        if kind == FieldKind.OBJECT:
            children = _build_nodes(stats.object_scope, path, acc_map, source)
        elif kind == FieldKind.ARRAY:
            element_count = sum(stats.element_types.values())
            element_kind = _kind_for_types(stats.element_types)
            array_profile = ArrayProfile(
                element_kind=element_kind,
                len_min=stats.array_min or 0,
                len_p50=_quantile(stats.array_lengths, 0.50),
                len_p95=_quantile(stats.array_lengths, 0.95),
                len_max_observed=stats.array_max,
                empty_rate=stats.array_empty / stats.array_count if stats.array_count else 0.0,
                element_types=_type_observations(stats.element_types, element_count),
            )
            if element_kind == FieldKind.OBJECT:
                children = _build_nodes(stats.array_element_scope, path + "[]", acc_map, source)
        string_stats = StringStats(max_len=stats.string_max, p99_len=_quantile(stats.strings, 0.99)) if stats.string_seen else None
        numeric_stats = None
        if stats.numeric_min is not None:
            numeric_stats = NumericStats(minimum=stats.numeric_min, maximum=stats.numeric_max,
                                         all_integer_valued=stats.all_integer,
                                         precision=stats.decimal_precision or None,
                                         scale=stats.decimal_scale if stats.decimal_precision else None)
        node = FieldNode(path=path, name=name, kind=kind, types=types, sample_size=denominator,
                         presence_rate=presence, null_rate=null_rate, absent_rate=absent,
                         presence_ci=(lo, hi), is_polymorphic=is_polymorphic, array=array_profile,
                         string_stats=string_stats, numeric_stats=numeric_stats, children=children,
                         source=source, max_depth_observed=stats.max_depth)
        acc_map[path] = stats
        nodes.append(node)
    return nodes


def _validator_constraints(json_schema: dict[str, Any]) -> dict[str, DeclaredConstraints]:
    result: dict[str, DeclaredConstraints] = {}
    def walk(schema: dict[str, Any], prefix: str = "", required: set[str] | None = None) -> None:
        required = required or set()
        for name, item in schema.get("properties", {}).items():
            path = f"{prefix}.{name}" if prefix else name
            result[path] = DeclaredConstraints(
                required=name in required,
                bson_types=[item["bsonType"]] if isinstance(item.get("bsonType"), str) else list(item.get("bsonType", [])),
                max_length=item.get("maxLength"), min_length=item.get("minLength"),
                minimum=item.get("minimum"), maximum=item.get("maximum"),
                enum=item.get("enum"), pattern=item.get("pattern"),
            )
            nested = item
            if item.get("bsonType") == "array" and isinstance(item.get("items"), dict):
                nested = item["items"]
                if nested.get("bsonType") == "object":
                    walk(nested, path + "[]", set(nested.get("required", [])))
            elif item.get("bsonType") == "object":
                walk(item, path, set(item.get("required", [])))
    walk(json_schema, "", set(json_schema.get("required", [])))
    return result


def _name_candidates(field_name: str, collections: set[str]) -> list[str]:
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", field_name).lower()
    bases = [snake]
    for suffix in ("_ids", "_id", "ids", "id", "_ref", "ref"):
        if snake.endswith(suffix) and len(snake) > len(suffix):
            bases.append(snake[:-len(suffix)].rstrip("_"))
    candidates: list[str] = []
    for base in bases:
        candidates.extend([base, base + "s", base + "es"])
    return [candidate for candidate in dict.fromkeys(candidates) if candidate in collections]


def _lookup_ref(database: Any, collection_names: set[str], field_name: str, ids: list[Any], min_match: float) -> RefCandidate | None:
    if not ids:
        return None
    candidates = _name_candidates(field_name, collection_names)
    # At most 200 distinct sampled identifiers are ever sent to MongoDB.
    unique: list[Any] = []
    seen: set[str] = set()
    for value in ids[:_MAX_IDS_PER_PATH]:
        key = str(value)
        if key not in seen:
            unique.append(value)
            seen.add(key)
    if not unique:
        return None
    best: tuple[float, str, int] | None = None
    for candidate in candidates:
        try:
            found = len(list(database[candidate].find({"_id": {"$in": unique}}, {"_id": 1}).limit(len(unique) + 1)))
        except Exception:
            continue
        rate = found / len(unique)
        if best is None or rate > best[0] or (rate == best[0] and candidate < best[1]):
            best = (rate, candidate, found)
    if best and best[0] >= min_match:
        return RefCandidate(collection=best[1], match_rate=best[0], method="lookup", sampled=len(unique), found=best[2])
    if candidates:
        rate, candidate, found = best or (0.0, candidates[0], 0)
        return RefCandidate(collection=candidate, match_rate=rate, method="name_hint", sampled=len(unique), found=found)
    return None


def _merge_declared_nodes(nodes: list[FieldNode], declared: dict[str, DeclaredConstraints], sample_size: int, additional_properties: bool | None) -> list[FieldNode]:
    """Merge validator-only fields without turning unobserved required fields into NOT NULL."""
    by_path: dict[str, FieldNode] = {}
    roots = list(nodes)
    def index(current: list[FieldNode]) -> None:
        for item in current:
            by_path[item.path] = item
            index(item.children)
    index(roots)
    for path in sorted(declared, key=lambda item: (item.count("."), len(item), item)):
        constraint = declared[path]
        existing = by_path.get(path)
        if existing is not None:
            existing.declared = constraint
            existing.source = InferenceSource.MERGED
            continue
        type_names = [name for name in constraint.bson_types if name != "null"]
        mapped_types = {"objectId": "objectId", "binData": "binData", "date": "date", "bool": "bool", "object": "object", "array": "array", "number": "double"}
        kind = _kind_for_types(mapped_types.get(name, name) for name in type_names)
        name = path.rsplit(".", 1)[-1]
        sample_ci = wilson_interval(0, sample_size)
        node = FieldNode(path=path, name=name, kind=kind,
                         types=[TypeObservation(bson_type=item, count=0, rate=0.0) for item in constraint.bson_types],
                         sample_size=sample_size, presence_rate=0.0, null_rate=0.0, absent_rate=1.0,
                         presence_ci=sample_ci, is_polymorphic=kind == FieldKind.MIXED,
                         declared=constraint, children=[], source=InferenceSource.MERGED)
        parent_path = path.rsplit(".", 1)[0] if "." in path else ""
        parent_key = parent_path[:-2] if parent_path.endswith("[]") else parent_path
        parent = by_path.get(parent_key)
        if parent is None:
            roots.append(node)
        else:
            parent.children.append(node)
        by_path[path] = node
    if additional_properties is False:
        declared_paths = set(declared)
        def mark(items: list[FieldNode]) -> None:
            for item in items:
                if item.path not in declared_paths:
                    item.source = InferenceSource.MERGED
                mark(item.children)
        mark(roots)
    return sorted(roots, key=lambda item: item.path)


def _attach_refs(nodes: list[FieldNode], acc_map: dict[str, _FieldStats], database: Any, collection_names: set[str], enabled: bool, min_match: float) -> list[FieldNode]:
    output: list[FieldNode] = []
    for node in nodes:
        acc = acc_map.get(node.path)
        children = _attach_refs(node.children, acc_map, database, collection_names, enabled, min_match)
        update: dict[str, Any] = {"children": children}
        if enabled and acc is not None:
            if node.kind == FieldKind.SCALAR and any(item.bson_type == "objectId" for item in node.types):
                update["ref_target"] = _lookup_ref(database, collection_names, node.name, acc.scalar_objectids, min_match)
            if node.kind == FieldKind.ARRAY and node.array and any(item.bson_type == "objectId" for item in node.array.element_types):
                candidate = _lookup_ref(database, collection_names, node.name, acc.element_objectids, min_match)
                update["array"] = node.array.model_copy(update={"element_ref_target": candidate})
        output.append(node.model_copy(update=update))
    return output


def _validator_info(options: dict[str, Any]) -> tuple[ValidatorInfo | None, dict[str, DeclaredConstraints]]:
    validator = options.get("validator") or {}
    json_schema = validator.get("$jsonSchema") if isinstance(validator, dict) else None
    if not isinstance(json_schema, dict):
        return None, {}
    info = ValidatorInfo(json_schema=json_schema,
                         validation_level=options.get("validationLevel"),
                         validation_action=options.get("validationAction"),
                         additional_properties=json_schema.get("additionalProperties"))
    return info, _validator_constraints(json_schema)


class DocumentExtractor(Extractor[DocumentIR]):
    def extract(self, conn: Any, options: dict[str, Any] | None = None) -> DocumentIR:
        opts = options or {}
        sample_opts = opts.get("sample", {})
        requested = int(sample_opts.get("size", 2000))
        if not 100 <= requested <= 50_000:
            raise ValueError("sample.size must be between 100 and 50000")
        recency_count = max(0, min(int(sample_opts.get("recency_probe", 200)), 10_000))
        max_time_ms = int(sample_opts.get("max_time_ms", 20_000))
        full_scan = bool(sample_opts.get("full_scan", False))
        names_opt = opts.get("collections")
        database = _database_handle(conn)
        collection_names = set(names_opt or database.list_collection_names())
        collection_names = {name for name in collection_names if not name.startswith("system.") and not name.startswith("_synapse_")}
        use_validators = bool(opts.get("use_validators", True))
        ref_opts = opts.get("reference_detection", {})
        ref_enabled = bool(ref_opts.get("enabled", True))
        min_match = float(ref_opts.get("min_match_rate", 0.9))
        collections: list[Collection] = []
        for name in sorted(collection_names):
            coll = database[name]
            try:
                doc_count = int(coll.estimated_document_count())
            except Exception:
                doc_count = int(coll.count_documents({}))
            collection_options = {}
            if use_validators:
                try:
                    info = database.command("listCollections", filter={"name": name}, nameOnly=False)
                    batch = info.get("cursor", {}).get("firstBatch", [])
                    collection_options = batch[0].get("options", {}) if batch else {}
                except Exception:
                    try:
                        collection_options = database.list_collections(filter={"name": name}).next().get("options", {})
                    except Exception:
                        collection_options = {}
            validator, declared = _validator_info(collection_options)
            source = InferenceSource.FULL_SCAN if full_scan else (InferenceSource.MERGED if validator else InferenceSource.SAMPLE)
            scope = _Scope()
            doc_size_total = 0
            sampled_docs = 0
            if full_scan:
                cursor = coll.find({}, no_cursor_timeout=True).batch_size(1000)
                try:
                    for doc in cursor:
                        scope.add_doc(doc)
                        sampled_docs += 1
                        doc_size_total += _bson_size(doc)
                finally:
                    cursor.close()
            else:
                try:
                    if doc_count <= requested:
                        docs = list(coll.find({}, no_cursor_timeout=True).limit(requested + 1))
                        strategy = "head_tail" if docs else "$sample"
                        docs = docs[:requested]
                    else:
                        docs = list(coll.aggregate([{"$sample": {"size": requested}}], allowDiskUse=False, maxTimeMS=max_time_ms))
                        strategy = "$sample"
                except Exception as exc:
                    raise RuntimeError(f"MongoDB sample probe failed for collection {name!r}: {type(exc).__name__}") from exc
                for doc in docs:
                    scope.add_doc(doc)
                    sampled_docs += 1
                    doc_size_total += _bson_size(doc)
                if recency_count:
                    try:
                        recent = list(coll.find({}, no_cursor_timeout=True).sort("_id", -1).limit(recency_count))
                    except Exception:
                        recent = []
                    # Include recent-only types conservatively. Duplicate sampled docs are allowed and disclosed by sample metadata.
                    for doc in recent:
                        scope.add_doc(doc)
                        doc_size_total += _bson_size(doc)
                    strategy = "head_tail" if strategy == "head_tail" else strategy
            acc_map: dict[str, _FieldStats] = {}
            fields = _build_nodes(scope, "", acc_map, source)
            additional = collection_options.get("validator", {}).get("$jsonSchema", {}).get("additionalProperties")
            fields = _merge_declared_nodes(fields, declared, scope.n, additional)
            fields = _attach_refs(fields, acc_map, database, collection_names, ref_enabled, min_match)
            id_stats = acc_map.get("_id")
            id_profile = IdProfile(types=_type_observations(id_stats.types, scope.n) if id_stats else [], sample_size=scope.n)
            indexes: list[MongoIndex] = []
            try:
                for index_name, spec in coll.index_information().items():
                    keys = [(str(field), direction) for field, direction in spec.get("key", [])]
                    if not keys:
                        continue
                    key_types = {str(direction) for _, direction in keys}
                    kind = "text" if "text" in key_types else ("2dsphere" if "2dsphere" in key_types else ("hashed" if "hashed" in key_types else "btree"))
                    if spec.get("wildcardProjection") is not None: kind = "wildcard"
                    indexes.append(MongoIndex(name=index_name, keys=keys, unique=bool(spec.get("unique", False)), sparse=bool(spec.get("sparse", False)),
                                              expire_after_seconds=spec.get("expireAfterSeconds"), index_type="ttl" if spec.get("expireAfterSeconds") is not None else kind,
                                              partial_filter=spec.get("partialFilterExpression")))
            except Exception:
                pass
            sample_info = SampleInfo(strategy="$sample" if full_scan else strategy, requested=requested,
                                     taken=sampled_docs, full_scan=full_scan, recency_probe=recency_count,
                                     random_cursor_fallback=None, max_time_ms=max_time_ms)
            collections.append(Collection(name=name, doc_count_estimate=doc_count,
                                          avg_doc_bytes=int(doc_size_total / max(scope.n, 1)), sample=sample_info,
                                          validator=validator, indexes=indexes, id_profile=id_profile, fields=fields,
                                          capped=False))
        return DocumentIR(collections=collections)


def _bson_size(document: dict[str, Any]) -> int:
    try:
        from bson import BSON
        return len(BSON.encode(document))
    except Exception:
        import json
        return len(json.dumps(document, default=str, separators=(",", ":")).encode("utf-8"))
