"""
ETL Data Quality Validator
==========================
Author  : Sanjyot Patil
Purpose : Source-to-target data quality validation for ETL pipelines.
          Covers row count reconciliation, null checks, duplicate detection,
          referential integrity, and value-range assertions.

Usage:
    python etl_data_quality_validator.py

Dependencies:
    pip install pandas sqlalchemy great-expectations  (optional)
"""

import pandas as pd
import sqlite3
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─── Result model ─────────────────────────────────────────────────────────────
@dataclass
class CheckResult:
    check_name: str
    status: str          # PASS / FAIL / WARN
    expected: str
    actual: str
    details: str = ""
    run_ts: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


# ─── Core validator class ──────────────────────────────────────────────────────
class ETLDataQualityValidator:
    """
    Validates data quality between a source DataFrame and a target DataFrame.
    Mirrors checks performed during SIT/UAT in Informatica / Ab Initio pipelines.
    """

    def __init__(self, source: pd.DataFrame, target: pd.DataFrame, pipeline_name: str = "ETL_PIPELINE"):
        self.source = source
        self.target = target
        self.pipeline_name = pipeline_name
        self.results: list[CheckResult] = []

    # ── 1. Row Count Reconciliation ────────────────────────────────────────────
    def check_row_count(self) -> CheckResult:
        """Source and target must have the same number of rows."""
        src_count = len(self.source)
        tgt_count = len(self.target)
        status = "PASS" if src_count == tgt_count else "FAIL"
        result = CheckResult(
            check_name="Row Count Reconciliation",
            status=status,
            expected=str(src_count),
            actual=str(tgt_count),
            details=f"Delta: {abs(src_count - tgt_count)} rows",
        )
        self.results.append(result)
        log.info("[%s] Row Count — %s (src=%d, tgt=%d)", self.pipeline_name, status, src_count, tgt_count)
        return result

    # ── 2. Null / Missing Value Check ──────────────────────────────────────────
    def check_nulls(self, critical_columns: list[str]) -> list[CheckResult]:
        """Critical columns must have zero nulls in the target."""
        check_results = []
        for col in critical_columns:
            if col not in self.target.columns:
                r = CheckResult(
                    check_name=f"Null Check — {col}",
                    status="FAIL",
                    expected="0 nulls",
                    actual="Column missing",
                )
                self.results.append(r)
                check_results.append(r)
                continue
            null_count = self.target[col].isnull().sum()
            status = "PASS" if null_count == 0 else "FAIL"
            r = CheckResult(
                check_name=f"Null Check — {col}",
                status=status,
                expected="0 nulls",
                actual=f"{null_count} nulls",
            )
            self.results.append(r)
            check_results.append(r)
            log.info("[%s] Null check on '%s' — %s (%d nulls)", self.pipeline_name, col, status, null_count)
        return check_results

    # ── 3. Duplicate Key Detection ─────────────────────────────────────────────
    def check_duplicates(self, key_columns: list[str]) -> CheckResult:
        """Target must have no duplicate rows on the business key."""
        dup_count = self.target.duplicated(subset=key_columns).sum()
        status = "PASS" if dup_count == 0 else "FAIL"
        result = CheckResult(
            check_name=f"Duplicate Check — {key_columns}",
            status=status,
            expected="0 duplicates",
            actual=f"{dup_count} duplicates",
        )
        self.results.append(result)
        log.info("[%s] Duplicate check on %s — %s (%d dups)", self.pipeline_name, key_columns, status, dup_count)
        return result

    # ── 4. Column-Level Aggregation Reconciliation ────────────────────────────
    def check_aggregates(self, numeric_columns: list[str], tolerance_pct: float = 0.01) -> list[CheckResult]:
        """
        Source and target SUM must match within tolerance.
        Typical use: amount fields, balance totals after ETL transformation.
        """
        check_results = []
        for col in numeric_columns:
            if col not in self.source.columns or col not in self.target.columns:
                r = CheckResult(
                    check_name=f"Aggregate Check — {col}",
                    status="FAIL",
                    expected="Column present in both",
                    actual="Column missing in source or target",
                )
                self.results.append(r)
                check_results.append(r)
                continue
            src_sum = self.source[col].sum()
            tgt_sum = self.target[col].sum()
            delta_pct = abs(src_sum - tgt_sum) / (abs(src_sum) + 1e-9)
            status = "PASS" if delta_pct <= tolerance_pct else "FAIL"
            r = CheckResult(
                check_name=f"Aggregate Check — {col}",
                status=status,
                expected=f"SUM={src_sum:.2f} (±{tolerance_pct*100}%)",
                actual=f"SUM={tgt_sum:.2f} (delta={delta_pct*100:.3f}%)",
            )
            self.results.append(r)
            check_results.append(r)
            log.info("[%s] Aggregate '%s' — %s", self.pipeline_name, col, status)
        return check_results

    # ── 5. Value Range / Domain Validation ────────────────────────────────────
    def check_value_range(self, column: str, min_val=None, max_val=None,
                          allowed_values: Optional[set] = None) -> CheckResult:
        """Target column values must fall within expected domain."""
        if column not in self.target.columns:
            r = CheckResult(
                check_name=f"Value Range — {column}",
                status="FAIL",
                expected="Column present",
                actual="Column missing",
            )
            self.results.append(r)
            return r

        series = self.target[column].dropna()
        violations = 0

        if allowed_values:
            violations = (~series.isin(allowed_values)).sum()
            detail = f"Allowed: {allowed_values}"
        else:
            if min_val is not None:
                violations += (series < min_val).sum()
            if max_val is not None:
                violations += (series > max_val).sum()
            detail = f"Expected range: [{min_val}, {max_val}]"

        status = "PASS" if violations == 0 else "FAIL"
        r = CheckResult(
            check_name=f"Value Range — {column}",
            status=status,
            expected=detail,
            actual=f"{violations} violations",
        )
        self.results.append(r)
        log.info("[%s] Value range '%s' — %s (%d violations)", self.pipeline_name, column, status, violations)
        return r

    # ── 6. Referential Integrity ───────────────────────────────────────────────
    def check_referential_integrity(self, fk_column: str, reference_df: pd.DataFrame,
                                    ref_column: str) -> CheckResult:
        """All FK values in target must exist in the reference lookup table."""
        target_keys = set(self.target[fk_column].dropna().unique())
        ref_keys = set(reference_df[ref_column].dropna().unique())
        orphans = target_keys - ref_keys
        status = "PASS" if not orphans else "FAIL"
        r = CheckResult(
            check_name=f"Referential Integrity — {fk_column} → {ref_column}",
            status=status,
            expected="No orphan keys",
            actual=f"{len(orphans)} orphan keys: {list(orphans)[:5]}{'...' if len(orphans) > 5 else ''}",
        )
        self.results.append(r)
        log.info("[%s] Ref integrity '%s' — %s", self.pipeline_name, fk_column, status)
        return r

    # ── Summary Report ─────────────────────────────────────────────────────────
    def summary(self) -> pd.DataFrame:
        """Return all check results as a DataFrame."""
        df = pd.DataFrame([vars(r) for r in self.results])
        total   = len(df)
        passed  = (df["status"] == "PASS").sum()
        failed  = (df["status"] == "FAIL").sum()
        warned  = (df["status"] == "WARN").sum()

        print("\n" + "=" * 70)
        print(f"  DATA QUALITY REPORT — {self.pipeline_name}")
        print(f"  Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        print(f"  Total Checks : {total}")
        print(f"  PASSED       : {passed}  ✅")
        print(f"  FAILED       : {failed}  ❌")
        print(f"  WARNINGS     : {warned}  ⚠️")
        print("=" * 70)
        print(df[["check_name", "status", "expected", "actual", "details"]].to_string(index=False))
        print("=" * 70 + "\n")
        return df


# ─── Demo / Test Run ──────────────────────────────────────────────────────────
def create_sample_data():
    """Simulate a source extract and a target load (post-ETL) for demo purposes."""

    source = pd.DataFrame({
        "account_id":  [101, 102, 103, 104, 105],
        "customer_id": [1,   2,   3,   4,   5],
        "balance":     [5000.00, 12000.50, 8750.25, 3200.00, 22000.00],
        "currency":    ["USD", "USD", "GBP", "USD", "EUR"],
        "status":      ["ACTIVE", "ACTIVE", "ACTIVE", "CLOSED", "ACTIVE"],
        "region":      ["US", "UK", "UK", "US", "EU"],
    })

    # Simulate a target with one intentional defect for demo
    target = pd.DataFrame({
        "account_id":  [101, 102, 103, 104, 105],
        "customer_id": [1,   2,   3,   4,   5],
        "balance":     [5000.00, 12000.50, 8750.25, 3200.00, 22000.00],
        "currency":    ["USD", "USD", "GBP", "XXX", "EUR"],   # XXX = bad value
        "status":      ["ACTIVE", "ACTIVE", "ACTIVE", "CLOSED", "ACTIVE"],
        "region":      ["US", "UK", "UK", "US", "EU"],
    })

    customers = pd.DataFrame({"customer_id": [1, 2, 3, 4, 5, 6]})
    return source, target, customers


if __name__ == "__main__":
    log.info("Starting ETL Data Quality Validation...")

    source_df, target_df, customers_df = create_sample_data()

    validator = ETLDataQualityValidator(
        source=source_df,
        target=target_df,
        pipeline_name="ACCOUNTS_DWH_LOAD",
    )

    # Run all checks
    validator.check_row_count()
    validator.check_nulls(critical_columns=["account_id", "balance", "currency"])
    validator.check_duplicates(key_columns=["account_id"])
    validator.check_aggregates(numeric_columns=["balance"], tolerance_pct=0.001)
    validator.check_value_range(
        column="currency",
        allowed_values={"USD", "GBP", "EUR", "SGD", "AED"}
    )
    validator.check_value_range(
        column="balance",
        min_val=0,
        max_val=10_000_000
    )
    validator.check_referential_integrity(
        fk_column="customer_id",
        reference_df=customers_df,
        ref_column="customer_id",
    )

    # Print summary
    report_df = validator.summary()

    # Optional: save report to CSV
    report_path = "dq_report.csv"
    report_df.to_csv(report_path, index=False)
    log.info("Report saved to %s", report_path)
