
import sys
import os
import json
import textwrap
from sqlalchemy import text
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.config import generate_meta_server_config, GlobalConfig
from db_controller import DBController

# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    MAGENTA = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    RESET = '\033[0m'

def format_time(seconds):
    """Format time in human-readable format"""
    if seconds >= 1000000000:  # Timeout marker
        return "TIMEOUT"
    elif seconds >= 1:
        return f"{seconds:.2f}s"
    elif seconds >= 0.001:
        return f"{seconds*1000:.1f}ms"
    else:
        return f"{seconds*1000000:.0f}µs"

def format_speedup(speedup):
    """Format speedup with color coding"""
    if speedup >= 2.0:
        return f"{Colors.GREEN}{Colors.BOLD}{speedup:.2f}x{Colors.RESET}"
    elif speedup >= 1.2:
        return f"{Colors.GREEN}{speedup:.2f}x{Colors.RESET}"
    elif speedup >= 0.8:
        return f"{Colors.YELLOW}{speedup:.2f}x{Colors.RESET}"
    else:
        return f"{Colors.RED}{speedup:.2f}x{Colors.RESET}"

def truncate_hints(hints_text, max_length=80):
    """Truncate hints for display"""
    if not hints_text:
        return "N/A"
    # Remove outer markers for cleaner display
    hints = hints_text.replace("/*+ BEGIN_OUTLINE_DATA", "").replace("END_OUTLINE_DATA */", "").strip()
    if len(hints) > max_length:
        return hints[:max_length] + "..."
    return hints

def query_detailed_history(instance_id, limit=None, verbose=False):
    meta_config = generate_meta_server_config()
    db = DBController(meta_config)
    
    db.use_db(GlobalConfig.ai_metadata_database)
    
    # 1. Query History
    where_parts = ["operation != 'noop'"]
    if instance_id:
        where_parts.append("instance_id = :instance_id")
    
    where_clause = "WHERE " + " AND ".join(where_parts)
    
    # Build LIMIT clause only if limit is specified
    limit_clause = "LIMIT :limit" if limit else ""

    history_query = text(f"""
        SELECT 
            id, apply_time, task_id, db, digest, operation, 
            prev_plan_ids, curr_plan_ids, comments
        FROM rule_state_history
        {where_clause}
        AND operation != 'noop'
        ORDER BY apply_time DESC
        {limit_clause}
    """)
    params = {}
    if limit:
        params["limit"] = limit
    if instance_id:
        params["instance_id"] = instance_id
        
    history_rows = db.execute(history_query, params).fetchall()
    
    if not history_rows:
        print("No history found.")
        return

    # 2. Collect Task IDs to fetch logs
    task_ids = list(set(row.task_id for row in history_rows))
    
    # 3. Query Rule Logs (Validation Info)
    logs_query = text(f"""
        SELECT 
            task_id, db, digest, plan_id, 
            default_elapsed_time, elapsed_time, 
            sql_text_rewritten, 
            hints_text,
            is_best, is_better,
            default_plan_id
        FROM validation_logs
        WHERE task_id IN :task_ids
    """)
    
    logs_map = defaultdict(list)
    if task_ids:
        logs_rows = db.execute(logs_query, {"task_ids": task_ids}).fetchall()
        for log in logs_rows:
            key = (log.task_id, log.db, log.digest)
            logs_map[key].append(log)

    # 3.1 Query Rules Table (For Timeout and Hints)
    timeout_map = {}
    hints_map = {}
    if task_ids:
        rules_query = text(f"""
            SELECT task_id, db, digest, plan_id, feedback_timeout, hints_text
            FROM rules
            WHERE task_id IN :task_ids
        """)
        rules_rows = db.execute(rules_query, {"task_ids": task_ids}).fetchall()
        for row in rules_rows:
            if row.plan_id:
                timeout_map[(row.task_id, row.db, row.digest, row.plan_id)] = row.feedback_timeout
                hints_map[(row.task_id, row.db, row.digest, row.plan_id)] = row.hints_text

    # 4. Display - Group by DB/Digest
    # Group rows by (db, digest)
    history_by_digest = defaultdict(list)
    for row in history_rows:
        history_by_digest[(row.db, row.digest)].append(row)
    
    print()
    print(f"{Colors.BOLD}{'═' * 100}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}  SQL Optimization History - Detailed View (Grouped by SQL){Colors.RESET}")
    print(f"{Colors.BOLD}{'═' * 100}{Colors.RESET}")
    
    for (db_name, digest), rows in history_by_digest.items():
        # Header for this SQL
        digest_short = digest
        print()
        print(f"{Colors.BOLD}{Colors.BLUE}● SQL: {db_name}/{digest_short}{Colors.RESET}")
        print(f"{Colors.DIM}{'─' * 100}{Colors.RESET}")
        
        # Display rows for this SQL (already ordered by time DESC from query)
        for row in rows:
            # Operation color coding
            op_color = {
                'setup_plan': Colors.GREEN,
                'reset': Colors.YELLOW,
                'disable': Colors.RED,
                'enable': Colors.BLUE,
                'modify_plan': Colors.CYAN,  # Added modify_plan color
            }.get(row.operation, Colors.RESET)
            
            print(f"  {Colors.DIM}{row.apply_time}  (task_id={row.task_id}){Colors.RESET}")
            print(f"  {op_color}{Colors.BOLD}[{row.operation.upper()}]{Colors.RESET}")
            if row.comments:
                print(f"  {Colors.DIM}Reason: {row.comments}{Colors.RESET}")
            
            # Plan Change Details
            prev_plan_ids = json.loads(row.prev_plan_ids) if row.prev_plan_ids else []
            curr_plan_ids = json.loads(row.curr_plan_ids) if row.curr_plan_ids else []
            
            added_plans = [p for p in curr_plan_ids if p not in prev_plan_ids]
            removed_plans = [p for p in prev_plan_ids if p not in curr_plan_ids]
            
            # Always show the plan IDs from history
            print(f"  {Colors.BOLD}┌─ Plan Changes{Colors.RESET}")
            print(f"  │ {Colors.DIM}Previous: {prev_plan_ids if prev_plan_ids else '(none)'}{Colors.RESET}")
            print(f"  │ {Colors.DIM}Current:  {curr_plan_ids if curr_plan_ids else '(none)'}{Colors.RESET}")
            
            if added_plans or removed_plans:
                print(f"  │")
                
                if added_plans:
                    print(f"  │ {Colors.GREEN}+ Added Plans:{Colors.RESET}")
                    for plan_id in added_plans:
                        hints = hints_map.get((row.task_id, row.db, row.digest, plan_id), None)
                        hints_display = truncate_hints(hints, 60) if hints else "N/A"
                        print(f"  │   {Colors.GREEN}{plan_id}{Colors.RESET}")
                        print(f"  │     {Colors.DIM}Hints: {hints_display}{Colors.RESET}")
                
                if removed_plans:
                    print(f"  │ {Colors.RED}- Removed Plans:{Colors.RESET}")
                    for plan_id in removed_plans:
                        print(f"  │   {Colors.RED}{plan_id}{Colors.RESET}")
                
                print(f"  └─")
            
            # Associated Rules Detail - Group by PlanID
            logs = logs_map.get((row.task_id, row.db, row.digest), [])
            
            # For RESET: show all evaluated candidates to explain why RESET happened
            # For other operations: show only related plan_ids
            if row.operation == 'reset':
                # Show all candidates evaluated in this task only if verbose >= 2
                relevant_logs = logs if verbose >= 2 else []
            else:
                relevant_logs = [l for l in logs if l.plan_id in curr_plan_ids]
            
            if relevant_logs:
                # Group logs by plan_id
                logs_by_plan = defaultdict(list)
                for log in relevant_logs:
                    logs_by_plan[log.plan_id].append(log)
                
                print(f"  {Colors.BOLD}┌─ Associated Rules ({len(logs_by_plan)} rule(s)){Colors.RESET}")
                
                for rule_idx, (plan_id, plan_logs) in enumerate(logs_by_plan.items(), 1):
                    is_last_rule = rule_idx == len(logs_by_plan)
                    branch = "└" if is_last_rule else "├"
                    cont = " " if is_last_rule else "│"
                    
                    # Get timeout for this rule
                    timeout_us = timeout_map.get((row.task_id, row.db, row.digest, plan_id), 0)
                    timeout_ms = timeout_us // 1000 if timeout_us else 0
                    
                    # Get hints (same for all logs of same plan_id)
                    hints = plan_logs[0].hints_text if plan_logs else None
                    
                    # Calculate aggregated performance metrics
                    valid_samples = [(l.default_elapsed_time, l.elapsed_time) 
                                     for l in plan_logs 
                                     if l.elapsed_time > 0 and l.elapsed_time < 1000000000]
                    
                    print(f"  {branch}─ {Colors.BOLD}Rule {rule_idx}{Colors.RESET}: {Colors.CYAN}{plan_id}{Colors.RESET}")
                    
                    if timeout_ms > 0:
                        print(f"  {cont}    Timeout: {Colors.YELLOW}{timeout_ms} ms{Colors.RESET}")
                    
                    # Show validation samples
                    print(f"  {cont}    {Colors.DIM}Validation ({len(plan_logs)} sample(s)):{Colors.RESET}")
                    
                    for sample_idx, log in enumerate(plan_logs, 1):
                        before_time = format_time(log.default_elapsed_time)
                        after_time = format_time(log.elapsed_time)
                        
                        if log.elapsed_time > 0 and log.elapsed_time < 1000000000:
                            speedup = log.default_elapsed_time / log.elapsed_time
                            speedup_str = format_speedup(speedup)
                        else:
                            speedup_str = f"{Colors.RED}TIMEOUT{Colors.RESET}"
                        
                        # Add is_better indicator
                        better_indicator = f"{Colors.GREEN}✓{Colors.RESET}" if log.is_better else f"{Colors.RED}✗{Colors.RESET}"
                        
                        # Add Default Plan ID (Verbose >= 1)
                        default_id_str = f" {Colors.DIM}(Default: {log.default_plan_id}){Colors.RESET}" if (verbose >= 1 and log.default_plan_id) else ""
                        
                        print(f"  {cont}      #{sample_idx}: {before_time} → {after_time} ({speedup_str}) {better_indicator}{default_id_str}")
                    
                    # Show aggregated stats if multiple samples
                    if len(valid_samples) > 1:
                        avg_before = sum(s[0] for s in valid_samples) / len(valid_samples)
                        avg_after = sum(s[1] for s in valid_samples) / len(valid_samples)
                        avg_speedup = avg_before / avg_after if avg_after > 0 else 0
                        
                        print(f"  {cont}      {Colors.BOLD}Average: {format_time(avg_before)} → {format_time(avg_after)} ({format_speedup(avg_speedup)}){Colors.RESET}")
                    
                    # Show hints (truncated)
                    if hints:
                        hints_display = truncate_hints(hints, 70)
                        print(f"  {cont}    {Colors.DIM}Hints: {hints_display}{Colors.RESET}")
                    
            elif curr_plan_ids:
                print(f"  {Colors.DIM}  PlanIDs: {curr_plan_ids} (No detailed validation log found){Colors.RESET}")
            
            print() # Spacer between history items of same SQL
        
        print(f"  {Colors.DIM}{'═' * 100}{Colors.RESET}") # Separator between SQLs
    
    print()
    print(f"{Colors.DIM}  Total: {len(history_rows)} history record(s) across {len(history_by_digest)} unique SQL(s){Colors.RESET}")
    print()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Query SQL Optimization History with Plan Details")
    parser.add_argument("--instance", required=True,
                        help="Target instance ID")
    parser.add_argument("--limit", "-l", type=int, default=None,
                        help="Maximum number of history records to display (default: no limit)")
    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Increase output verbosity (-v: per-sample Default Plan, -vv: full RESET logs)")
    args = parser.parse_args()
    
    instance_id = args.instance
    
    limit_display = args.limit if args.limit else "unlimited"
    print(f"Querying history for instance_id: {instance_id}, limit: {limit_display}")
    query_detailed_history(instance_id, limit=args.limit, verbose=args.verbose)
