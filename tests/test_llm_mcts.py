#!/usr/bin/env python3
"""
MCTS _collect_additional_candidates() interface correctness test.

Tests the new mcts module through the LLMOptimizer integration.
Validates:
  1. MCTS data preparation (qdf data structure)
  2. MCTS solving (solutions with executed_hints, reward, execution_time_s)
  3. Candidate conversion from solutions to CandidatePlan
  4. Result completeness (plan_id, hints_text, source_sql, mcts_results fields)

Usage:
    python tests/test_llm_mcts.py --ip 127.0.0.1 --port 3306 --user root --password "xxx"
"""
import sys
import os
import hashlib
import time
import json
import argparse
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import text
from ai_logger import aiopt_logger
from db_controller import DBController
from data_models import (
    TrainingEnvType,
    OutlineType,
    InstanceInfo,
    ProductType,
    Region,
    WorkloadSource,
    InstanceConfig,
)
from optimizer.llm_optimizer import LLMOptimizer
from optimizer.basic_optimizer import OptimizationContext, CandidatePlan
from feature_detector import detect_features
from ai_config import TrainingParameters


# ============================================================================
# Temp DB & test data
# ============================================================================
TEMP_DB_NAME = f"_test_mcts_{int(time.time()) % 100000}"

SETUP_DDLS = [
    f"CREATE DATABASE IF NOT EXISTS `{TEMP_DB_NAME}`",

    f"""CREATE TABLE `{TEMP_DB_NAME}`.`date_dim` (
        d_date_sk INT NOT NULL PRIMARY KEY,
        d_year INT NOT NULL,
        d_moy INT NOT NULL,
        d_date DATE NOT NULL,
        d_qoy INT NOT NULL,
        d_week_seq INT NOT NULL,
        KEY idx_year (d_year),
        KEY idx_moy (d_moy),
        KEY idx_qoy (d_qoy)
    ) ENGINE=InnoDB""",

    f"""CREATE TABLE `{TEMP_DB_NAME}`.`item` (
        i_item_sk INT NOT NULL PRIMARY KEY,
        i_item_id VARCHAR(16) NOT NULL,
        i_item_desc VARCHAR(200) NOT NULL,
        i_brand_id INT NOT NULL,
        i_brand VARCHAR(50) NOT NULL,
        i_manufact_id INT NOT NULL,
        i_category VARCHAR(50),
        i_class VARCHAR(50),
        i_current_price DECIMAL(7,2),
        KEY idx_brand_id (i_brand_id),
        KEY idx_manufact_id (i_manufact_id),
        KEY idx_category (i_category)
    ) ENGINE=InnoDB""",

    f"""CREATE TABLE `{TEMP_DB_NAME}`.`store_sales` (
        ss_sold_date_sk INT NOT NULL,
        ss_item_sk INT NOT NULL,
        ss_customer_sk INT,
        ss_store_sk INT,
        ss_ext_discount_amt DECIMAL(15,2) NOT NULL,
        ss_ext_sales_price DECIMAL(15,2),
        ss_ext_list_price DECIMAL(15,2),
        ss_quantity INT NOT NULL DEFAULT 1,
        KEY idx_date_sk (ss_sold_date_sk),
        KEY idx_item_sk (ss_item_sk),
        KEY idx_customer_sk (ss_customer_sk),
        KEY idx_store_sk (ss_store_sk),
        KEY idx_date_item (ss_sold_date_sk, ss_item_sk)
    ) ENGINE=InnoDB""",

    f"""CREATE TABLE `{TEMP_DB_NAME}`.`store` (
        s_store_sk INT NOT NULL PRIMARY KEY,
        s_store_id VARCHAR(16) NOT NULL,
        s_store_name VARCHAR(50),
        s_state VARCHAR(2),
        s_zip VARCHAR(10),
        KEY idx_state (s_state)
    ) ENGINE=InnoDB""",

    f"""CREATE TABLE `{TEMP_DB_NAME}`.`customer` (
        c_customer_sk INT NOT NULL PRIMARY KEY,
        c_customer_id VARCHAR(16) NOT NULL,
        c_current_addr_sk INT,
        c_current_cdemo_sk INT,
        KEY idx_addr_sk (c_current_addr_sk)
    ) ENGINE=InnoDB""",

    f"""CREATE TABLE `{TEMP_DB_NAME}`.`customer_address` (
        ca_address_sk INT NOT NULL PRIMARY KEY,
        ca_state VARCHAR(2),
        ca_county VARCHAR(50),
        ca_zip VARCHAR(10),
        KEY idx_state (ca_state),
        KEY idx_zip (ca_zip)
    ) ENGINE=InnoDB""",

    f"""CREATE TABLE `{TEMP_DB_NAME}`.`web_sales` (
        ws_sold_date_sk INT NOT NULL,
        ws_item_sk INT NOT NULL,
        ws_bill_customer_sk INT,
        ws_ext_sales_price DECIMAL(15,2),
        KEY idx_date_sk (ws_sold_date_sk),
        KEY idx_item_sk (ws_item_sk),
        KEY idx_customer_sk (ws_bill_customer_sk)
    ) ENGINE=InnoDB""",

    f"""CREATE TABLE `{TEMP_DB_NAME}`.`catalog_sales` (
        cs_sold_date_sk INT NOT NULL,
        cs_item_sk INT NOT NULL,
        cs_bill_customer_sk INT,
        cs_ext_sales_price DECIMAL(15,2),
        KEY idx_date_sk (cs_sold_date_sk),
        KEY idx_item_sk (cs_item_sk),
        KEY idx_customer_sk (cs_bill_customer_sk)
    ) ENGINE=InnoDB""",
]

INSERT_DMLS = [
    f"""INSERT INTO `{TEMP_DB_NAME}`.`date_dim` (d_date_sk, d_year, d_moy, d_date, d_qoy, d_week_seq) VALUES
        (2451911, 2001, 12, '2001-12-01', 4, 52),
        (2451912, 2001, 12, '2001-12-02', 4, 52),
        (2451913, 2001, 12, '2001-12-03', 4, 52),
        (2452276, 2002, 12, '2002-12-01', 4, 52),
        (2452277, 2002, 12, '2002-12-02', 4, 52),
        (2451914, 2001, 5, '2001-05-15', 2, 20),
        (2451915, 2001, 6, '2001-06-20', 2, 25)""",

    f"""INSERT INTO `{TEMP_DB_NAME}`.`item` (i_item_sk, i_item_id, i_item_desc, i_brand_id, i_brand, i_manufact_id, i_category, i_class, i_current_price) VALUES
        (1, 'ITEM_001', 'Item 001 Desc', 1001, 'Brand_A', 797, 'Electronics', 'Computers', 99.99),
        (2, 'ITEM_002', 'Item 002 Desc', 1002, 'Brand_B', 797, 'Books', 'Fiction', 19.99),
        (3, 'ITEM_003', 'Item 003 Desc', 1003, 'Brand_C', 123, 'Electronics', 'Phones', 299.99),
        (4, 'ITEM_004', 'Item 004 Desc', 1004, 'Brand_D', 797, 'Books', 'Non-Fiction', 24.99),
        (5, 'ITEM_005', 'Item 005 Desc', 1005, 'Brand_E', 456, 'Electronics', 'Tablets', 199.99)""",

    f"""INSERT INTO `{TEMP_DB_NAME}`.`store` (s_store_sk, s_store_id, s_store_name, s_state, s_zip) VALUES
        (1, 'S001', 'Store_One', 'TN', '37000'),
        (2, 'S002', 'Store_Two', 'CA', '90000'),
        (3, 'S003', 'Store_Three', 'NY', '10000')""",

    f"""INSERT INTO `{TEMP_DB_NAME}`.`customer_address` (ca_address_sk, ca_state, ca_county, ca_zip) VALUES
        (1, 'TN', 'Davidson County', '37000'),
        (2, 'CA', 'Los Angeles County', '90000'),
        (3, 'NY', 'New York County', '10000')""",

    f"""INSERT INTO `{TEMP_DB_NAME}`.`customer` (c_customer_sk, c_customer_id, c_current_addr_sk, c_current_cdemo_sk) VALUES
        (1, 'CUST_001', 1, 1),
        (2, 'CUST_002', 2, 2),
        (3, 'CUST_003', 3, 3)""",

    f"""INSERT INTO `{TEMP_DB_NAME}`.`store_sales` (ss_sold_date_sk, ss_item_sk, ss_customer_sk, ss_store_sk, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_list_price, ss_quantity) VALUES
        (2451911, 1, 1, 1, 10.50, 89.49, 99.99, 2),
        (2451911, 2, 1, 1, 20.30, 19.69, 19.99, 1),
        (2451912, 1, 2, 2, 5.00, 94.99, 99.99, 3),
        (2451912, 4, 2, 2, 15.75, 9.24, 24.99, 1),
        (2451913, 2, 3, 3, 8.20, 11.79, 19.99, 2),
        (2451913, 3, 3, 3, 12.00, 287.99, 299.99, 1),
        (2452276, 1, 1, 1, 25.00, 74.99, 99.99, 4),
        (2452276, 4, 2, 2, 30.50, -5.51, 24.99, 2),
        (2452277, 2, 3, 3, 18.00, 1.99, 19.99, 1),
        (2452277, 1, 1, 1, 22.10, 77.89, 99.99, 3)""",

    f"""INSERT INTO `{TEMP_DB_NAME}`.`web_sales` (ws_sold_date_sk, ws_item_sk, ws_bill_customer_sk, ws_ext_sales_price) VALUES
        (2451911, 1, 1, 89.49),
        (2451912, 2, 2, 19.69),
        (2451913, 3, 3, 287.99)""",

    f"""INSERT INTO `{TEMP_DB_NAME}`.`catalog_sales` (cs_sold_date_sk, cs_item_sk, cs_bill_customer_sk, cs_ext_sales_price) VALUES
        (2451911, 1, 1, 89.49),
        (2451912, 4, 2, 9.24),
        (2452276, 1, 1, 74.99)""",
]


# ============================================================================
# Test SQL cases
# ============================================================================

TEST_SQL_1_BASIC = (
    "SELECT dt.d_year, item.i_brand_id AS brand_id, item.i_brand AS brand, "
    "SUM(ss_ext_discount_amt) AS sum_agg "
    "FROM date_dim dt, store_sales, item "
    "WHERE dt.d_date_sk = store_sales.ss_sold_date_sk "
    "AND store_sales.ss_item_sk = item.i_item_sk "
    "AND item.i_manufact_id = 797 "
    "AND dt.d_moy = 12 "
    "GROUP BY dt.d_year, item.i_brand, item.i_brand_id "
    "ORDER BY dt.d_year, sum_agg DESC, brand_id "
    "LIMIT 100"
)

TEST_SQL_4_MULTI_JOIN = (
    "SELECT ca.ca_state AS state, COUNT(*) AS cnt, "
    "       i.i_brand_id, i.i_brand "
    "FROM customer_address ca, customer c, store_sales s, date_dim d, item i "
    "WHERE ca.ca_address_sk = c.c_current_addr_sk "
    "AND c.c_customer_sk = s.ss_customer_sk "
    "AND s.ss_sold_date_sk = d.d_date_sk "
    "AND s.ss_item_sk = i.i_item_sk "
    "AND d.d_moy = 12 "
    "AND d.d_year = 2001 "
    "AND i.i_manufact_id = 797 "
    "GROUP BY ca.ca_state, i.i_brand_id, i.i_brand "
    "HAVING COUNT(*) >= 1 "
    "ORDER BY cnt, i.i_brand_id "
    "LIMIT 100"
)

TEST_SQL_6_SIMPLE = (
    "SELECT i_item_id, i_brand_id, i_brand "
    "FROM item "
    "WHERE i_manufact_id = 797 "
    "ORDER BY i_item_id "
    "LIMIT 100"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="MCTS _collect_additional_candidates() Test",
    )
    parser.add_argument("--ip", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--user", type=str, default="tencentroot")
    parser.add_argument("--password", type=str, default="")
    parser.add_argument("--test-cases", type=str, default="all",
                        help="Comma-separated test case numbers or 'all'")
    return parser.parse_args()


def generate_digest(sql: str) -> str:
    return hashlib.md5(sql.encode()).hexdigest()[:16]


# ============================================================================
# Setup & Cleanup
# ============================================================================

def setup_temp_data(controller: DBController):
    print(f"\n[Setup] Creating temp database: {TEMP_DB_NAME}")
    for ddl in SETUP_DDLS:
        controller.execute(text(ddl))
    for dml in INSERT_DMLS:
        controller.execute(text(dml))
    controller.use_db(TEMP_DB_NAME)
    print("[Setup] Temp environment ready")


def cleanup_temp_data(controller: DBController):
    print(f"\n[Cleanup] Dropping temp database: {TEMP_DB_NAME}")
    try:
        controller.execute(text(f"DROP DATABASE IF EXISTS `{TEMP_DB_NAME}`"))
        print("[Cleanup] Done")
    except Exception as e:
        print(f"[Cleanup] Warning: {e}")


# ============================================================================
# Verification
# ============================================================================

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def check(self, condition: bool, description: str, detail: str = ""):
        if condition:
            self.passed += 1
            print(f"  ✓ PASS: {description}")
        else:
            self.failed += 1
            msg = f"  ✗ FAIL: {description}"
            if detail:
                msg += f" ({detail})"
            print(msg)
            self.errors.append(description)

    def summary(self) -> bool:
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"[Summary] {self.passed}/{total} checks passed")
        if self.errors:
            print(f"[Summary] Failed:")
            for e in self.errors:
                print(f"  - {e}")
        print(f"{'='*60}")
        return self.failed == 0


def verify_candidates(candidates: list, test: TestResult):
    print(f"\n[Verify] Candidates ({len(candidates)} items)")

    test.check(isinstance(candidates, list), "Return is list")

    for i, c in enumerate(candidates):
        pfx = f"Candidate[{i}]"
        test.check(isinstance(c, CandidatePlan), f"{pfx} is CandidatePlan")
        test.check(c.plan_id is not None and len(c.plan_id) > 0, f"{pfx} plan_id non-empty")
        test.check(c.hints_text is not None and len(c.hints_text) > 0, f"{pfx} hints_text non-empty")
        test.check(isinstance(c.indexes_dict, dict), f"{pfx} indexes_dict is dict")
        test.check(c.source_sql is not None and len(c.source_sql) > 0, f"{pfx} source_sql non-empty")

    if candidates:
        plan_ids = [c.plan_id for c in candidates]
        test.check(len(plan_ids) == len(set(plan_ids)), "All plan_ids unique")


def verify_mcts_results(optimizer: LLMOptimizer, test: TestResult, expected_sql: str):
    print(f"\n[Verify] MCTS Results")

    mcts_results = optimizer.mcts_results
    test.check(mcts_results is not None, "mcts_results not None")
    if mcts_results is None:
        return

    test.check(isinstance(mcts_results, list), "mcts_results is list")
    test.check(len(mcts_results) >= 1, "mcts_results has at least 1 item")

    for i, result in enumerate(mcts_results):
        pfx = f"MCTS[{i}]"
        test.check(isinstance(result, dict), f"{pfx} is dict")
        test.check("query" in result, f"{pfx} has 'query'")
        test.check("baseline_time" in result, f"{pfx} has 'baseline_time'")
        test.check("mcts_tree_nodes" in result, f"{pfx} has 'mcts_tree_nodes'")
        test.check("solutions" in result, f"{pfx} has 'solutions'")
        test.check("plan_digest_cache" in result, f"{pfx} has 'plan_digest_cache'")
        test.check("early_stopping_metrics" in result, f"{pfx} has 'early_stopping_metrics'")
        test.check("performance_metrics" in result, f"{pfx} has 'performance_metrics'")

        metrics = result.get("performance_metrics", {})
        for key in ["llm_call_count", "llm_output_chars", "llm_output_seconds",
                     "llm_chars_per_second", "llm_input_chars",
                     "db_explain_count", "db_execute_count", "db_execute_seconds",
                     "mcts_e2e_seconds"]:
            test.check(key in metrics, f"{pfx} performance_metrics has '{key}'")

        e2e = metrics.get("mcts_e2e_seconds", 0)
        test.check(isinstance(e2e, (int, float)) and e2e > 0, f"{pfx} e2e > 0")

        llm_calls = metrics.get("llm_call_count", 0)
        test.check(isinstance(llm_calls, (int, float)) and llm_calls > 0, f"{pfx} llm_call_count > 0")

        solutions = result.get("solutions", [])
        if solutions:
            print(f"    {pfx}: {len(solutions)} solutions, e2e={e2e:.2f}s")

            for j, sol in enumerate(solutions):
                sol_pfx = f"{pfx}.solution[{j}]"
                test.check("executed_hints" in sol, f"{sol_pfx} has 'executed_hints'")
                test.check("reward" in sol, f"{sol_pfx} has 'reward'")
                test.check("q_value" in sol, f"{sol_pfx} has 'q_value'")
                test.check("execution_time_s" in sol, f"{sol_pfx} has 'execution_time_s'")
                test.check("plan_digest" in sol, f"{sol_pfx} has 'plan_digest'")
                test.check("rollout_index" in sol, f"{sol_pfx} has 'rollout_index'")
                test.check("action_type" in sol, f"{sol_pfx} has 'action_type'")

            pdc = result.get("plan_digest_cache", {})
            test.check(isinstance(pdc, dict), f"{pfx} plan_digest_cache is dict")
            # plan_digest_cache should only have execution_time_s
            for digest, entry in pdc.items():
                test.check("execution_time_s" in entry,
                           f"{pfx} pdc[{digest[:16]}..] has execution_time_s")

            # early_stopping_metrics has full details
            esm = result.get("early_stopping_metrics", {})
            test.check(isinstance(esm, dict), f"{pfx} early_stopping_metrics is dict")
            for digest, entry in esm.items():
                # repeated_rollouts should have no duplicates
                rr = entry.get("repeated_rollouts", [])
                test.check(len(rr) == len(set(rr)),
                           f"{pfx} esm[{digest[:16]}..] repeated_rollouts has no duplicates")
                # Non-baseline entries should have root_children_stats populated
                if entry.get("first_rollout", -1) >= 0:
                    rcs = entry.get("root_children_stats", [])
                    test.check(len(rcs) > 0,
                               f"{pfx} esm[{digest[:16]}..] root_children_stats non-empty")

            # Check mcts_tree_nodes ordering (BFS: depth should be non-decreasing)
            tree_nodes = result.get("mcts_tree_nodes", {})
            if tree_nodes:
                depths = [v.get("node_info", {}).get("depth", 0) for v in tree_nodes.values()]
                test.check(depths == sorted(depths), f"{pfx} mcts_tree_nodes sorted by depth (BFS)")
                # Check that tree nodes have llm_response
                for tag, node_data in tree_nodes.items():
                    if tag != "0":  # skip root
                        test.check("llm_response" in node_data, f"{pfx} tree node {tag} has 'llm_response'")


def run_single_test(
    test_name: str,
    sql: str,
    sql_samples: list,
    optimizer: LLMOptimizer,
    test: TestResult,
    db_name: str,
):
    print(f"\n{'='*60}")
    print(f"[Test] {test_name}")
    print(f"{'='*60}")
    print(f"SQL: {sql[:150]}...")

    digest = generate_digest(sql)
    try:
        t0 = time.time()
        candidates = optimizer._collect_additional_candidates(
            db=db_name,
            digest=digest,
            sql_samples=sql_samples,
        )
        elapsed = time.time() - t0
        print(f"[Test] {test_name}: {len(candidates)} candidates, {elapsed:.2f}s")

        verify_candidates(candidates, test)
        verify_mcts_results(optimizer, test, expected_sql=sql)
        return True

    except Exception as e:
        print(f"[Test] {test_name} exception: {e}")
        traceback.print_exc()
        test.check(False, f"{test_name} no exception", f"Exception: {e}")
        return False


# ============================================================================
# Main
# ============================================================================

def main():
    args = parse_args()
    start_time = time.time()

    instance_id = f"test_mcts_{args.ip}_{args.port}"
    task_id = f"test_mcts_{int(time.time())}"

    print(f"{'='*60}")
    print(f"[Test] MCTS _collect_additional_candidates() Test")
    print(f"{'='*60}")
    print(f"[Test] DB: {TEMP_DB_NAME}")
    print(f"[Test] Connection: {args.ip}:{args.port}")

    setup_config = InstanceConfig(
        instance_id=instance_id,
        ip=args.ip,
        port=args.port,
        user=args.user,
        password=args.password,
        read_only=False,
        with_ai_marker=False,
        allow_reconnect=True,
    )
    setup_controller = DBController(setup_config)
    test = TestResult()

    try:
        # Setup
        setup_temp_data(setup_controller)

        # Init optimizer
        env_config = InstanceConfig(
            instance_id=instance_id,
            ip=args.ip,
            port=args.port,
            user=args.user,
            password=args.password,
            read_only=False,
            with_ai_marker=True,
            allow_reconnect=True,
        )

        instance_info = InstanceInfo(
            cluster_id=1,
            product_type=ProductType.CDB,
            instance_id=instance_id,
            node_uuid="test_mcts_node",
            workload_source=WorkloadSource.SLOW_LOG,
            outline_type=OutlineType.STATEMENT_OUTLINE,
            region=Region.test,
            comments="MCTS Test",
        )

        temp_controller = DBController(env_config)
        feature_flags = detect_features(temp_controller)

        training_controller = DBController(
            env_config,
            is_training_env=True,
            feature_flags=feature_flags,
        )

        context = OptimizationContext(
            task_id=task_id,
            instance_id=instance_id,
            outline_type=instance_info.outline_type,
            training_controller=training_controller,
            env_type=TrainingEnvType.CLONE,
            feature_flags=feature_flags,
            instance_info=instance_info,
        )

        optimizer = LLMOptimizer(context)
        print(f"[Phase 2] Created {optimizer.get_optimizer_name()}")

        # Run tests
        test_cases_to_run = []
        if args.test_cases.lower() == "all":
            test_cases_to_run = [1, 4, 6]
        else:
            test_cases_to_run = [int(x.strip()) for x in args.test_cases.split(",")]

        test_cases = {
            1: ("Basic 3-table JOIN", TEST_SQL_1_BASIC, [TEST_SQL_1_BASIC]),
            4: ("5+ Table Multi-JOIN", TEST_SQL_4_MULTI_JOIN, [TEST_SQL_4_MULTI_JOIN]),
            6: ("Simple Single-Table", TEST_SQL_6_SIMPLE, [TEST_SQL_6_SIMPLE]),
        }

        for tc_num in test_cases_to_run:
            if tc_num not in test_cases:
                print(f"[WARNING] Test case {tc_num} not found, skipping")
                continue
            name, sql, samples = test_cases[tc_num]
            run_single_test(name, sql, samples, optimizer, test, TEMP_DB_NAME)

        test.check(optimizer.training_time > 0, "training_time > 0")

        total = time.time() - start_time
        print(f"\n[Timing] Total: {total:.2f}s")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        traceback.print_exc()
        test.check(False, "No fatal exception", str(e))

    finally:
        try:
            cleanup_temp_data(setup_controller)
        except Exception:
            pass
        try:
            setup_controller.close()
        except Exception:
            pass

    all_passed = test.summary()
    if all_passed:
        print("\n[Result] ✓ ALL TESTS PASSED")
        sys.exit(0)
    else:
        print(f"\n[Result] ✗ {test.failed} TEST(S) FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
