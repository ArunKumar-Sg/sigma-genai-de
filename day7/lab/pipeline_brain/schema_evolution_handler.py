from typing import Dict, List, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, any]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    drift_severity = 'NONE'
    if new_columns:
        if any("null" not in v for v in new_columns.values()):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'
    return {
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes,
        "drift_severity": drift_severity
    }

def decide_action(drift_report: Dict[str, any]) -> Dict[str, Dict[str, str]]:
    decisions = {}
    for column_name, data_type in drift_report["new_columns"].items():
        if data_type.endswith("[string]"):
            decisions[column_name] = {"action": "ADD_TO_SCHEMA", "reason": "New nullable string column", "risk_level": "LOW"}
        elif data_type.endswith("[float]"):
            decisions[column_name] = {"action": "FLAG_ANOMALY", "reason": "New float column affecting revenue", "risk_level": "HIGH"}
    for column_name in drift_report["removed_columns"]:
        decisions[column_name] = {"action": "HALT", "reason": "Removed column will break downstream queries", "risk_level": "BREAKING"}
    for column_name, (old_type, new_type) in drift_report["type_changes"].items():
        if old_type.endswith("[int]") and new_type.endswith("[float]"):
            decisions[column_name] = {"action": "ADD_TO_SCHEMA", "reason": "Type widening", "risk_level": "LOW"}
        elif old_type.endswith("[float]") and new_type.endswith("[int]"):
            decisions[column_name] = {"action": "FLAG_ANOMALY", "reason": "Type narrowing", "risk_level": "HIGH"}
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, str]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for column_name, decision in decisions.items():
        action = decision["action"]
        if action == "DROP_SILENTLY":
            spark_df = spark_df.drop(column_name)
        elif action == "ADD_TO_SCHEMA":
            migration_notes.append(f"Added column: {column_name} with type: {updated_schema[column_name]}")
        elif action == "FLAG_ANOMALY":
            spark_df = spark_df.withColumn(f"{column_name}_anomaly", F.lit(True).alias(f"{column_name}_anomaly"))
            migration_notes.append(f"Flagged anomaly for column: {column_name}")
        elif action == "HALT":
            raise ValueError(f"Cannot proceed: {decision['reason']}")
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, any]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    print(f"Drift Report: {drift_report}")
    print(f"Action Decisions: {decisions}")
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        return {"drift_report": drift_report, "decisions": decisions, "migration_notes": migration_notes, "evolved_df": evolved_df}
    return {"drift_report": drift_report, "decisions": decisions}
