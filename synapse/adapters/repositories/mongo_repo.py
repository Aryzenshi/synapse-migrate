"""Verified MongoDB CRUD with independent API validation and optimistic versions."""
from __future__ import annotations

import copy
import re
import decimal
import datetime as dt
from typing import Any

from synapse.domain.typemap.constraints import validate_against_bson_schema


class MongoValidationError(ValueError):
    def __init__(self, errors: list[dict[str, Any]]):
        super().__init__("document failed API-level schema validation")
        self.errors = errors


class MongoVersionConflict(RuntimeError):
    pass


class MongoDocumentNotFound(LookupError):
    pass


def _plain(value: Any) -> Any:
    if isinstance(value, dict): return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, list): return [_plain(item) for item in value]
    return value


class VerifiedMongoRepository:
    def __init__(self, collection: Any, *, validator: dict[str, Any] | None = None,
                 indexed_fields: set[str] | None = None,
                 reference_checks: list[tuple[Any, str, str]] | None = None,
                 delete_references: list[tuple[Any, str, str]] | None = None,
                 derived_fields: dict[str, dict[str, Any]] | None = None):
        self.collection = collection
        self.validator = (validator or {}).get("$jsonSchema", validator or {})
        self.indexed_fields = indexed_fields or {"_id"}
        self.reference_checks = reference_checks or []
        self.delete_references = delete_references or []
        self.derived_fields = derived_fields or {}

    def validate(self, document: dict[str, Any]) -> None:
        normalized = self._coerce_document(document)
        payload = {key: value for key, value in normalized.items() if key not in {"_version", "_id"}}
        errors = validate_against_bson_schema(payload, self.validator)
        for reference_collection, field, display in self.reference_checks:
            value = self._get_path(payload, field)
            if value is not None and reference_collection.find_one({"_id": self._normalize_id(value)}, {"_id": 1}) is None:
                errors.append({"path": f"$.{field}", "code": "reference_not_found", "message": f"{display}/{value} does not exist", "source": "api"})
        if errors:
            raise MongoValidationError(errors)

    def create(self, document: dict[str, Any]) -> dict[str, Any]:
        payload = self._apply_derived_document(self._coerce_document(copy.deepcopy(document)))
        payload.pop("_id", None)
        payload["_version"] = 1
        self.validate(payload)
        result = self.collection.insert_one(payload)
        payload["_id"] = result.inserted_id
        return payload

    def get(self, document_id: Any) -> dict[str, Any]:
        value = self.collection.find_one({"_id": self._normalize_id(document_id)})
        if value is None:
            raise MongoDocumentNotFound(str(document_id))
        return value

    def replace(self, document_id: Any, document: dict[str, Any], *, if_match: int | None = None) -> dict[str, Any]:
        query = {"_id": self._normalize_id(document_id)}
        current = self.collection.find_one(query, {"_version": 1})
        if current is None:
            raise MongoDocumentNotFound(str(document_id))
        version = int(current.get("_version", 1))
        if if_match is not None and if_match != version:
            raise MongoVersionConflict(f"expected version {if_match}, current version is {version}")
        payload = self._apply_derived_document(self._coerce_document(copy.deepcopy(document)))
        payload.pop("_id", None)
        payload["_version"] = version + 1
        self.validate(payload)
        result = self.collection.replace_one({**query, "_version": version}, payload, upsert=False)
        if result.matched_count != 1:
            raise MongoVersionConflict("document changed during replacement")
        payload["_id"] = query["_id"]
        return payload

    def patch_merge(self, document_id: Any, patch: dict[str, Any], *, if_match: int | None = None) -> dict[str, Any]:
        current = self.get(document_id)
        version = int(current.get("_version", 1))
        if if_match is not None and if_match != version:
            raise MongoVersionConflict(f"expected version {if_match}, current version is {version}")
        updated = self._apply_derived_document(self._coerce_document(self._merge_patch(current, patch)))
        updated.pop("_id", None)
        updated["_version"] = version + 1
        self.validate(updated)
        result = self.collection.replace_one({"_id": self._normalize_id(document_id), "_version": version}, updated, upsert=False)
        if result.matched_count != 1:
            raise MongoVersionConflict("document changed during patch")
        updated["_id"] = self._normalize_id(document_id)
        return updated

    def patch_operations(self, document_id: Any, body: dict[str, Any], *, if_match: int | None = None) -> dict[str, Any]:
        current = self.get(document_id)
        version = int(current.get("_version", 1))
        if if_match is not None and if_match != version:
            raise MongoVersionConflict(f"expected version {if_match}, current version is {version}")
        pipeline_set: dict[str, Any] = {"_version": {"$add": ["$_version", 1]}}
        for operation in body.get("ops", []):
            op = operation.get("op")
            path = str(operation.get("path", ""))
            if op == "set":
                match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\[([A-Za-z_][A-Za-z0-9_]*)=([^]]+)\]\.(.+)", path)
                if match:
                    array_name, match_key, match_value, child_path = match.groups()
                    array_ref = f"${array_name}"
                    filter_value: Any = match_value
                    if filter_value.isdigit(): filter_value = int(filter_value)
                    pipeline_set[array_name] = {"$map": {"input": {"$ifNull": [array_ref, []]}, "as": "item",
                                                            "in": {"$cond": [{"$eq": [f"$$item.{match_key}", filter_value]},
                                                                              {"$mergeObjects": ["$$item", self._nested_set(child_path, operation.get("value"))]},
                                                                              "$$item"]}}}
                else:
                    pipeline_set[path.replace("[", ".").replace("]", "")] = operation.get("value")
            elif op == "push":
                pipeline_set[path] = {"$concatArrays": [{"$ifNull": [f"${path}", []]}, [operation.get("value")]]}
            elif op == "pull":
                match_value = operation.get("match", {})
                condition = {"$and": [{"$eq": [f"$$item.{key}", value]} for key, value in match_value.items()]} if match_value else {"$ne": ["$$item", operation.get("value")]}
                pipeline_set[path] = {"$filter": {"input": {"$ifNull": [f"${path}", []]}, "as": "item", "cond": {"$not": [condition]}}}
            else:
                raise ValueError(f"unsupported array operation {op!r}")
        for name in body.get("recompute", []):
            definition = self.derived_fields.get(name)
            if definition:
                pipeline_set[name] = self._derived_expression(definition)
        self.validate(self._apply_ops_for_validation(current, body))
        result = self.collection.update_one({"_id": self._normalize_id(document_id), "_version": version}, [{"$set": pipeline_set}])
        if result.matched_count != 1:
            raise MongoVersionConflict("document changed during patch")
        return self.get(document_id)

    def delete(self, document_id: Any, *, if_match: int | None = None, restrict: bool = True) -> bool:
        query = {"_id": self._normalize_id(document_id)}
        current = self.collection.find_one(query, {"_version": 1})
        if current is None:
            raise MongoDocumentNotFound(str(document_id))
        version = int(current.get("_version", 1))
        if if_match is not None and if_match != version:
            raise MongoVersionConflict(f"expected version {if_match}, current version is {version}")
        if restrict:
            for other_collection, path, display in self.delete_references:
                if other_collection.find_one({path: query["_id"]}, {"_id": 1}) is not None:
                    raise RuntimeError(f"referenced: document is still referenced by {display}")
        result = self.collection.delete_one({**query, "_version": version})
        if result.deleted_count != 1:
            raise MongoVersionConflict("document changed during delete")
        return True

    def list(self, *, limit: int = 50, after: Any | None = None, filters: dict[str, Any] | None = None,
             projection: dict[str, int] | None = None, allow_scan: bool = False, sort: str = "_id") -> tuple[list[dict[str, Any]], Any | None, bool]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be 1..500")
        query: dict[str, Any] = {}
        if filters:
            for field, value in filters.items():
                if field not in self.indexed_fields and not allow_scan:
                    raise ValueError(f"unindexed filter {field!r} requires allow_scan=true")
                query[field] = value
        direction = -1 if sort.startswith("-") else 1
        sort_field = sort[1:] if sort.startswith("-") else sort
        if sort_field not in self.indexed_fields and not allow_scan:
            raise ValueError(f"unindexed sort {sort_field!r} requires allow_scan=true")
        if after is not None:
            operator = "$gt" if direction > 0 else "$lt"
            if sort_field == "_id":
                key = after[0] if isinstance(after, (list, tuple)) else after
                query["_id"] = {operator: self._normalize_id(key)}
            else:
                if not isinstance(after, (list, tuple)) or len(after) != 2:
                    raise ValueError("non-_id sort cursor must contain [sort_value, _id]")
                value, object_id = after
                query["$or"] = [{sort_field: {operator: value}}, {sort_field: value, "_id": {operator: self._normalize_id(object_id)}}]
        sort_spec = [(sort_field, direction)] if sort_field == "_id" else [(sort_field, direction), ("_id", direction)]
        docs = list(self.collection.find(query, projection).sort(sort_spec).limit(limit + 1))
        has_more = len(docs) > limit
        docs = docs[:limit]
        next_key = None
        if has_more and docs:
            next_key = [docs[-1].get("_id")] if sort_field == "_id" else [docs[-1].get(sort_field), docs[-1].get("_id")]
        return docs, next_key, has_more

    def _coerce_document(self, document: dict[str, Any]) -> dict[str, Any]:
        schema = self.validator
        def coerce(value: Any, rule: dict[str, Any]) -> Any:
            kinds = rule.get("bsonType")
            kinds = kinds if isinstance(kinds, list) else [kinds]
            if "objectId" in kinds and isinstance(value, str):
                try:
                    from bson import ObjectId
                    return ObjectId(value) if ObjectId.is_valid(value) else value
                except ImportError:
                    return value
            if "decimal" in kinds and isinstance(value, str):
                try:
                    from bson.decimal128 import Decimal128
                    return Decimal128(value)
                except Exception:
                    return value
            if "date" in kinds and isinstance(value, str):
                try:
                    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
                    return parsed
                except ValueError:
                    return value
            if isinstance(value, dict):
                rules = rule.get("properties", {})
                return {key: coerce(item, rules.get(key, {})) for key, item in value.items()}
            if isinstance(value, list) and "items" in rule:
                return [coerce(item, rule["items"]) for item in value]
            return value
        normalized = coerce(copy.deepcopy(document), schema)
        return normalized if isinstance(normalized, dict) else copy.deepcopy(document)

    def _apply_derived_document(self, document: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(document)
        for path, definition in self.derived_fields.items():
            if definition.get("op") != "sum_product":
                continue
            items = self._get_path(result, definition["array"]) or []
            total = decimal.Decimal(0)
            for item in items:
                if isinstance(item, dict):
                    try:
                        total += decimal.Decimal(str(item.get(definition["fields"][0], 0))) * decimal.Decimal(str(item.get(definition["fields"][1], 0)))
                    except decimal.InvalidOperation as exc:
                        raise ValueError(f"derived field {path} has a non-numeric input") from exc
            try:
                from bson.decimal128 import Decimal128
                total_value: Any = Decimal128(total)
            except ImportError:
                total_value = total
            self._set_path(result, path, total_value)
        return result

    @staticmethod
    def _set_path(document: dict[str, Any], path: str, value: Any) -> None:
        parts = path.split(".")
        target = document
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = value

    @staticmethod
    def _normalize_id(value: Any) -> Any:
        if isinstance(value, str):
            try:
                from bson import ObjectId
                if ObjectId.is_valid(value): return ObjectId(value)
            except ImportError:
                pass
        return value

    @staticmethod
    def _merge_patch(target: Any, patch: Any) -> Any:
        if not isinstance(patch, dict):
            return copy.deepcopy(patch)
        if not isinstance(target, dict):
            target = {}
        result = copy.deepcopy(target)
        for key, value in patch.items():
            if value is None:
                result.pop(key, None)
            else:
                result[key] = VerifiedMongoRepository._merge_patch(result.get(key), value) if isinstance(value, dict) else copy.deepcopy(value)
        return result

    @staticmethod
    def _get_path(value: dict[str, Any], path: str) -> Any:
        current: Any = value
        for part in path.split("."):
            if not isinstance(current, dict): return None
            current = current.get(part)
        return current

    @staticmethod
    def _nested_set(path: str, value: Any) -> dict[str, Any]:
        parts = path.split(".")
        result: dict[str, Any] = {}
        current = result
        for part in parts[:-1]:
            current[part] = {}
            current = current[part]
        current[parts[-1]] = value
        return result

    @staticmethod
    def _derived_expression(definition: dict[str, Any]) -> Any:
        if definition.get("op") == "sum_product":
            alias = "syn_item"
            return {"$sum": {"$map": {"input": {"$ifNull": [f"${definition['array']}", []]}, "as": alias,
                                         "in": {"$multiply": [f"$${alias}.{definition['fields'][0]}", f"$${alias}.{definition['fields'][1]}"]}}}}
        raise ValueError(f"unsupported derived-field expression {definition!r}")

    @staticmethod
    def _apply_ops_for_validation(current: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(current)
        for operation in body.get("ops", []):
            path = operation.get("path", "")
            op = operation.get("op")
            if "[" in path:
                continue
            if op == "set":
                parts = path.split(".")
                target = result
                for part in parts[:-1]: target = target.setdefault(part, {})
                target[parts[-1]] = operation.get("value")
            elif op == "push":
                result.setdefault(path, []).append(operation.get("value"))
            elif op == "pull":
                match = operation.get("match", {})
                result[path] = [item for item in result.get(path, []) if not isinstance(item, dict) or any(item.get(key) != value for key, value in match.items())]
        result.pop("_id", None)
        result.pop("_version", None)
        return result


MongoRepository = VerifiedMongoRepository
