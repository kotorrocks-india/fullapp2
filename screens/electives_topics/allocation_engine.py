# screens/electives_topics/allocation_engine.py
"""
Allocation engine for elective student selections.
Implements ranked choice allocation with capacity constraints.
UPDATED: Integrated with electives_policy for cross-batch/cross-branch enforcement
"""

from __future__ import annotations
import logging
import random
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

# NEW: electives policy (optional)
try:
    from core import electives_policy as core_electives_policy
except Exception:
    core_electives_policy = None

logger = logging.getLogger(__name__)


# ===========================================================================
# DATABASE HELPERS
# ===========================================================================

def _exec(conn, sql: str, params: dict = None):
    """Execute SQL with parameters."""
    return conn.execute(sa_text(sql), params or {})


def _fetch_one(engine: Engine, sql: str, params: dict = None) -> Optional[Dict]:
    """Fetch single row."""
    with engine.begin() as conn:
        result = _exec(conn, sql, params).fetchone()
        return dict(result._mapping) if result else None


def _fetch_all(engine: Engine, sql: str, params: dict = None) -> List[Dict]:
    """Fetch all rows."""
    with engine.begin() as conn:
        results = _exec(conn, sql, params).fetchall()
        return [dict(r._mapping) for r in results]


# ===========================================================================
# ALLOCATION ENGINE
# ===========================================================================

class AllocationEngine:
    """
    Ranked choice allocation engine with capacity management.
    
    Algorithm:
    1. Group students' preferences by rank
    2. For each rank (1st, 2nd, 3rd...):
       - Shuffle students (fairness)
       - Try to assign each student to their choice at that rank
       - If capacity full, add to waitlist
    3. Continue until all students processed or max iterations reached
    
    UPDATED: Enforces cross-batch and cross-branch constraints from electives policy
    """
    
    def __init__(self, engine: Engine, subject_code: str, ay_label: str,
                 year: int, term: int, degree_code: str):
        self.engine = engine
        self.subject_code = subject_code
        self.ay_label = ay_label
        self.year = year
        self.term = term
        self.degree_code = degree_code
    
    def _load_policy(self):
        """
        Load effective electives policy for this allocation context.
        Currently we resolve at degree level (degree_code only).
        If your electives_policy supports program/branch later,
        you can extend this to pass those too.
        """
        if core_electives_policy is None:
            return None
        
        raw_conn = self.engine.raw_connection()
        try:
            # ElectivesPolicy dataclass instance (or None)
            return core_electives_policy.fetch_effective_policy(
                raw_conn,
                degree_code=self.degree_code,
                program_code=None,  # program_code (optional)
                branch_code=None,   # branch_code (optional)
            )
        finally:
            raw_conn.close()
    
    def _enforce_scope_constraints(self, policy, selections: List[Dict]) -> None:
        """
        Enforce cross-batch / cross-branch rules from electives policy.
        
        - If cross_batch_allowed is False -> all pending selections must be from
          a single batch (ignoring NULL/empty).
        - If cross_branch_allowed is False -> all pending selections must be from
          a single (program_code, branch_code) combination (ignoring NULL/empty).
        
        If policy is None, or both flags are True, we do nothing.
        If a violation is found, raise ValueError so the run is marked as failed.
        """
        if policy is None:
            return
        
        # Safely get booleans; default to True (more permissive) if absent
        cross_batch_allowed = getattr(policy, "cross_batch_allowed", True)
        cross_branch_allowed = getattr(policy, "cross_branch_allowed", True)
        
        if cross_batch_allowed and cross_branch_allowed:
            # Both relaxed (e.g., B.Arch vertical studios) -> nothing to enforce here
            return
        
        # Collect cohort info from denormalised selection rows
        batches = {
            s.get("batch")
            for s in selections
            if s.get("batch") not in (None, "", " ")
        }
        
        prog_branch_pairs = {
            (s.get("program_code"), s.get("branch_code"))
            for s in selections
            if s.get("program_code") or s.get("branch_code")
        }
        
        errors = []
        
        if not cross_batch_allowed and len(batches) > 1:
            errors.append(
                f"multiple batches in pending selections: {sorted(batches)}"
            )
        
        if not cross_branch_allowed and len(prog_branch_pairs) > 1:
            errors.append(
                "multiple program/branch combinations in pending selections: "
                f"{sorted(prog_branch_pairs)}"
            )
        
        if errors:
            msg = (
                "Electives policy violation: allocation run attempted for a "
                "mixed cohort but policy forbids cross-batch / cross-branch. "
                + "; ".join(errors)
            )
            logger.error(msg)
            raise ValueError(msg)
        
    def run_allocation(self, strategy: List[str] = None, 
                      min_satisfaction: float = 50.0,
                      max_iterations: int = 10) -> Dict:
        """
        Run allocation algorithm.
        
        Args:
            strategy: List of strategies in priority order
            min_satisfaction: Stop if this % of students get 1st choice
            max_iterations: Maximum rank iterations
        
        Returns:
            Dict with run_id, students_assigned, satisfaction, etc.
        """
        
        if strategy is None:
            strategy = ['student_select_ranked']
        
        logger.info(f"Starting allocation for {self.subject_code} in {self.ay_label}")
        
        # Create run record
        run_id = self._create_run_record(strategy, min_satisfaction)
        start_time = datetime.now()
        
        try:
            # Get pending selections
            selections = self._fetch_pending_selections()
            
            if not selections:
                self._update_run_status(run_id, 'completed', 
                                       error="No pending selections")
                logger.info("No pending selections found")
                return {
                    'run_id': run_id,
                    'students_assigned': 0,
                    'students_waitlisted': 0,
                    'top_choice_satisfaction': 0.0,
                    'message': 'No pending selections'
                }
            
            # Get topic capacities
            topics = self._fetch_topic_capacities()
            topic_map = {t['topic_code_ay']: t for t in topics}
            
            logger.info(f"Processing {len(selections)} selections across {len(topics)} topics")
            
            # NEW: Enforce cross-batch / cross-branch constraints from policy
            policy = self._load_policy()
            self._enforce_scope_constraints(policy, selections)
            
            # Run appropriate allocation strategy
            if 'student_select_ranked' in strategy:
                results = self._allocate_ranked(selections, topic_map, run_id, max_iterations)
            elif 'student_select_first_come' in strategy:
                results = self._allocate_first_come(selections, topic_map, run_id)
            else:
                results = self._confirm_manual_assignments(selections, run_id)
            
            # Calculate satisfaction
            satisfaction = self._calculate_satisfaction()
            
            # Calculate processing time
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            
            # Update run record
            self._update_run_status(
                run_id, 
                'completed',
                total_students=len(set(s['student_roll_no'] for s in selections)),
                students_assigned=results['assigned'],
                students_waitlisted=results['waitlisted'],
                top_choice_satisfaction=satisfaction,
                iterations=results.get('iterations', 1),
                processing_time=processing_time
            )
            
            logger.info(f"Allocation completed: {results['assigned']} assigned, "
                       f"{results['waitlisted']} waitlisted, "
                       f"{satisfaction:.1f}% satisfaction")
            
            return {
                'run_id': run_id,
                'students_assigned': results['assigned'],
                'students_waitlisted': results['waitlisted'],
                'top_choice_satisfaction': satisfaction,
                'iterations': results.get('iterations', 1),
                'processing_time_ms': processing_time
            }
            
        except Exception as e:
            logger.error(f"Allocation failed: {e}", exc_info=True)
            self._update_run_status(run_id, 'failed', error=str(e))
            raise
    
    def _allocate_ranked(self, selections: List[Dict], topic_map: Dict[str, Dict], 
                        run_id: int, max_iterations: int) -> Dict:
        """
        Allocate using ranked choice algorithm.
        
        Algorithm:
        1. Group by student
        2. For each rank (1, 2, 3...):
           - Get all students who have a choice at this rank and aren't assigned
           - Shuffle for fairness
           - Process each student in order
           - Assign if capacity available, else waitlist
        """
        
        assigned_count = 0
        waitlisted_count = 0
        iterations = 0
        
        # Group selections by student
        student_prefs = {}
        for sel in selections:
            roll = sel['student_roll_no']
            if roll not in student_prefs:
                student_prefs[roll] = []
            student_prefs[roll].append(sel)
        
        # Sort each student's preferences by rank
        for roll in student_prefs:
            student_prefs[roll].sort(key=lambda x: x['rank_choice'] or 999)
        
        logger.info(f"Processing {len(student_prefs)} students with ranked preferences")
        
        with self.engine.begin() as conn:
            # Process each rank level
            for rank in range(1, max_iterations + 1):
                students_at_rank = []
                
                # Find students who have a preference at this rank and aren't assigned
                for roll_no, prefs in student_prefs.items():
                    if self._is_student_assigned(conn, roll_no):
                        continue
                    
                    pref = next((p for p in prefs if p['rank_choice'] == rank), None)
                    if pref:
                        students_at_rank.append((roll_no, pref))
                
                if not students_at_rank:
                    logger.debug(f"No students at rank {rank}, stopping")
                    break
                
                iterations += 1
                logger.info(f"Processing rank {rank}: {len(students_at_rank)} students")
                
                # Apply tiebreaker (shuffle for fairness)
                students_at_rank = self._apply_tiebreaker(students_at_rank)
                
                # Process each student at this rank
                for roll_no, pref in students_at_rank:
                    topic_code = pref['topic_code_ay']
                    topic = topic_map.get(topic_code)
                    
                    if not topic:
                        logger.warning(f"Topic {topic_code} not found")
                        continue
                    
                    # Get current confirmed count for this topic
                    confirmed = self._get_confirmed_count(conn, topic_code)
                    capacity = topic['capacity']
                    
                    # Check if space available
                    if capacity == 0 or confirmed < capacity:
                        # Assign!
                        self._confirm_selection(conn, pref['id'], 'allocation_engine', run_id)
                        assigned_count += 1
                        logger.debug(f"✓ Assigned {roll_no} to {topic_code} (rank {rank})")
                    else:
                        # Waitlist
                        self._waitlist_selection(conn, pref['id'], 'allocation_engine', run_id)
                        waitlisted_count += 1
                        logger.debug(f"⏳ Waitlisted {roll_no} for {topic_code} (rank {rank})")
        
        logger.info(f"Ranked allocation complete: {iterations} iterations")
        
        return {
            'assigned': assigned_count,
            'waitlisted': waitlisted_count,
            'iterations': iterations
        }
    
    def _allocate_first_come(self, selections: List[Dict], 
                            topic_map: Dict[str, Dict], run_id: int) -> Dict:
        """Allocate based on submission time (first-come-first-served)."""
        
        assigned_count = 0
        waitlisted_count = 0
        
        # Sort by selected_at
        selections.sort(key=lambda x: x['selected_at'] or datetime.max)
        
        with self.engine.begin() as conn:
            for sel in selections:
                # Skip if already assigned
                if self._is_student_assigned(conn, sel['student_roll_no']):
                    continue
                
                topic_code = sel['topic_code_ay']
                topic = topic_map.get(topic_code)
                
                if not topic:
                    continue
                
                # Check capacity
                confirmed = self._get_confirmed_count(conn, topic_code)
                capacity = topic['capacity']
                
                if capacity == 0 or confirmed < capacity:
                    self._confirm_selection(conn, sel['id'], 'allocation_engine', run_id)
                    assigned_count += 1
                else:
                    self._waitlist_selection(conn, sel['id'], 'allocation_engine', run_id)
                    waitlisted_count += 1
        
        return {
            'assigned': assigned_count,
            'waitlisted': waitlisted_count,
            'iterations': 1
        }
    
    def _confirm_manual_assignments(self, selections: List[Dict], run_id: int) -> Dict:
        """Confirm manual assignments (no algorithm)."""
        
        assigned_count = 0
        
        with self.engine.begin() as conn:
            for sel in selections:
                if sel['selection_strategy'] == 'manual_assign':
                    self._confirm_selection(conn, sel['id'], 'allocation_engine', run_id)
                    assigned_count += 1
        
        return {
            'assigned': assigned_count,
            'waitlisted': 0,
            'iterations': 1
        }
    
    # ========================================================================
    # TIEBREAKER & FAIRNESS
    # ========================================================================
    
    def _apply_tiebreaker(self, students: List[Tuple[str, Dict]]) -> List[Tuple[str, Dict]]:
        """
        Apply tiebreaker rules when multiple students want same topic at same rank.
        
        Current implementation: Random shuffle with daily seed for fairness.
        
        TODO: Enhance with CGPA, credits earned, etc.
        """
        # Use date as seed for reproducibility within a day
        seed = datetime.now().date().isoformat()
        random.seed(seed)
        random.shuffle(students)
        
        # Future: Sort by CGPA, credits, etc.
        # students.sort(key=lambda x: x[1].get('cgpa', 0), reverse=True)
        
        return students
    
    # ========================================================================
    # HELPER QUERIES
    # ========================================================================
    
    def _is_student_assigned(self, conn, roll_no: str) -> bool:
        """Check if student already has confirmed selection."""
        result = conn.execute(sa_text("""
            SELECT COUNT(*) AS cnt FROM elective_student_selections
            WHERE student_roll_no = :roll
            AND subject_code = :subj
            AND ay_label = :ay
            AND status = 'confirmed'
        """), {
            "roll": roll_no,
            "subj": self.subject_code,
            "ay": self.ay_label
        }).scalar()
        return result > 0
    
    def _get_confirmed_count(self, conn, topic_code: str) -> int:
        """Get current confirmed count for topic."""
        result = conn.execute(sa_text("""
            SELECT COUNT(*) AS cnt FROM elective_student_selections
            WHERE topic_code_ay = :topic
            AND ay_label = :ay
            AND status = 'confirmed'
        """), {
            "topic": topic_code,
            "ay": self.ay_label
        }).scalar()
        return result or 0
    
    def _confirm_selection(self, conn, selection_id: int, actor: str, run_id: int):
        """Confirm a selection."""
        now = datetime.now()
        
        conn.execute(sa_text("""
            UPDATE elective_student_selections
            SET status = 'confirmed',
                confirmed_at = :now,
                confirmed_by = :actor,
                updated_at = :now
            WHERE id = :id
        """), {
            "id": selection_id,
            "now": now,
            "actor": f"{actor}_run_{run_id}"
        })
        
        # Audit
        conn.execute(sa_text("""
            INSERT INTO elective_selections_audit (
                selection_id, student_roll_no, topic_code_ay, subject_code, ay_label,
                action, old_status, new_status, actor, occurred_at, operation, source
            )
            SELECT 
                id, student_roll_no, topic_code_ay, subject_code, ay_label,
                'auto_confirm', 'draft', 'confirmed', :actor, :now, 'allocation', 'engine'
            FROM elective_student_selections
            WHERE id = :id
        """), {
            "id": selection_id,
            "actor": f"{actor}_run_{run_id}",
            "now": now
        })
    
    def _waitlist_selection(self, conn, selection_id: int, actor: str, run_id: int):
        """Waitlist a selection."""
        now = datetime.now()
        
        conn.execute(sa_text("""
            UPDATE elective_student_selections
            SET status = 'waitlisted',
                waitlisted_at = :now,
                updated_at = :now
            WHERE id = :id
        """), {
            "id": selection_id,
            "now": now
        })
        
        # Audit
        conn.execute(sa_text("""
            INSERT INTO elective_selections_audit (
                selection_id, student_roll_no, topic_code_ay, subject_code, ay_label,
                action, old_status, new_status, actor, occurred_at, operation, source
            )
            SELECT 
                id, student_roll_no, topic_code_ay, subject_code, ay_label,
                'auto_waitlist', 'draft', 'waitlisted', :actor, :now, 'allocation', 'engine'
            FROM elective_student_selections
            WHERE id = :id
        """), {
            "id": selection_id,
            "actor": f"{actor}_run_{run_id}",
            "now": now
        })
    
    def _fetch_pending_selections(self) -> List[Dict]:
        """Fetch all draft selections for this subject."""
        return _fetch_all(self.engine, """
            SELECT * FROM elective_student_selections
            WHERE subject_code = :subj
            AND ay_label = :ay
            AND year = :yr
            AND term = :trm
            AND degree_code = :deg
            AND status = 'draft'
            ORDER BY selected_at, rank_choice
        """, {
            "subj": self.subject_code,
            "ay": self.ay_label,
            "yr": self.year,
            "trm": self.term,
            "deg": self.degree_code
        })
    
    def _fetch_topic_capacities(self) -> List[Dict]:
        """Fetch topic capacities."""
        return _fetch_all(self.engine, """
            SELECT topic_code_ay, capacity, status
            FROM elective_topics
            WHERE subject_code = :subj
            AND ay_label = :ay
            AND year = :yr
            AND term = :trm
            AND status = 'published'
        """, {
            "subj": self.subject_code,
            "ay": self.ay_label,
            "yr": self.year,
            "trm": self.term
        })
    
    def _calculate_satisfaction(self) -> float:
        """Calculate % of students who got their 1st choice."""
        result = _fetch_one(self.engine, """
            SELECT 
                COUNT(*) AS total,
                SUM(CASE WHEN rank_choice = 1 THEN 1 ELSE 0 END) AS first_choice
            FROM elective_student_selections
            WHERE subject_code = :subj 
            AND ay_label = :ay
            AND status = 'confirmed'
        """, {
            "subj": self.subject_code,
            "ay": self.ay_label
        })
        
        if not result or result['total'] == 0:
            return 0.0
        
        return (result['first_choice'] / result['total']) * 100
    
    # ========================================================================
    # RUN TRACKING
    # ========================================================================
    
    def _create_run_record(self, strategy: List[str], min_satisfaction: float) -> int:
        """Create allocation run record."""
        with self.engine.begin() as conn:
            result = conn.execute(sa_text("""
                INSERT INTO elective_allocation_runs (
                    subject_code, degree_code, ay_label, year, term,
                    run_number, started_at, status, strategy, min_satisfaction_percent
                )
                VALUES (
                    :subj, :deg, :ay, :yr, :trm,
                    COALESCE(
                        (SELECT MAX(run_number) FROM elective_allocation_runs 
                         WHERE subject_code = :subj AND ay_label = :ay), 
                        0
                    ) + 1,
                    :now, 'running', :strat, :min_sat
                )
            """), {
                "subj": self.subject_code,
                "deg": self.degree_code,
                "ay": self.ay_label,
                "yr": self.year,
                "trm": self.term,
                "now": datetime.now(),
                "strat": ','.join(strategy),
                "min_sat": min_satisfaction
            })
            
            return result.lastrowid
    
    def _update_run_status(self, run_id: int, status: str, 
                          total_students: int = 0,
                          students_assigned: int = 0,
                          students_waitlisted: int = 0,
                          top_choice_satisfaction: float = 0.0,
                          iterations: int = 0,
                          processing_time: int = 0,
                          error: str = None):
        """Update run record with results."""
        with self.engine.begin() as conn:
            conn.execute(sa_text("""
                UPDATE elective_allocation_runs
                SET status = :status,
                    completed_at = :now,
                    total_students = :total,
                    students_assigned = :assigned,
                    students_waitlisted = :waitlisted,
                    students_unassigned = :total - :assigned - :waitlisted,
                    top_choice_satisfaction_percent = :satisfaction,
                    iterations_completed = :iterations,
                    processing_time_ms = :time,
                    error_message = :error
                WHERE id = :id
            """), {
                "id": run_id,
                "status": status,
                "now": datetime.now(),
                "total": total_students,
                "assigned": students_assigned,
                "waitlisted": students_waitlisted,
                "satisfaction": top_choice_satisfaction,
                "iterations": iterations,
                "time": processing_time,
                "error": error
            })


# ===========================================================================
# CONVENIENCE FUNCTIONS
# ===========================================================================

def trigger_allocation(engine: Engine, subject_code: str, ay_label: str,
                      year: int, term: int, degree_code: str,
                      strategy: List[str] = None,
                      min_satisfaction: float = 50.0) -> Dict:
    """
    Trigger allocation for a subject.
    
    Returns: Results dict with run_id, students_assigned, satisfaction, etc.
    """
    
    allocator = AllocationEngine(
        engine=engine,
        subject_code=subject_code,
        ay_label=ay_label,
        year=year,
        term=term,
        degree_code=degree_code
    )
    
    return allocator.run_allocation(
        strategy=strategy or ['student_select_ranked'],
        min_satisfaction=min_satisfaction
    )


def get_allocation_history(engine: Engine, subject_code: str = None, 
                          ay_label: str = None, limit: int = 10) -> List[Dict]:
    """Get allocation run history."""
    
    query = """
        SELECT * FROM elective_allocation_runs
        WHERE 1=1
    """
    params = {}
    
    if subject_code:
        query += " AND subject_code = :subj"
        params['subj'] = subject_code
    
    if ay_label:
        query += " AND ay_label = :ay"
        params['ay'] = ay_label
    
    query += " ORDER BY started_at DESC LIMIT :limit"
    params['limit'] = limit
    
    return _fetch_all(engine, query, params)


def get_allocation_run_details(engine: Engine, run_id: int) -> Optional[Dict]:
    """Get details of a specific allocation run."""
    return _fetch_one(engine, """
        SELECT * FROM elective_allocation_runs
        WHERE id = :id
    """, {"id": run_id})


# ===========================================================================
# TESTING & SIMULATION
# ===========================================================================

def simulate_allocation(engine: Engine, subject_code: str, ay_label: str,
                       year: int, term: int, degree_code: str,
                       dry_run: bool = True) -> Dict:
    """
    Simulate allocation without actually confirming.
    Useful for testing and preview.
    """
    
    logger.info(f"Simulating allocation (dry_run={dry_run})")
    
    # In dry run mode, we'd need to clone selections to temp table
    # For now, just return statistics
    
    allocator = AllocationEngine(
        engine=engine,
        subject_code=subject_code,
        ay_label=ay_label,
        year=year,
        term=term,
        degree_code=degree_code
    )
    
    # Get pending selections
    selections = allocator._fetch_pending_selections()
    topics = allocator._fetch_topic_capacities()
    
    # Calculate statistics
    total_students = len(set(s['student_roll_no'] for s in selections))
    total_capacity = sum(t['capacity'] for t in topics if t['capacity'] > 0)
    
    first_choices = {}
    for s in selections:
        if s['rank_choice'] == 1:
            topic = s['topic_code_ay']
            first_choices[topic] = first_choices.get(topic, 0) + 1
    
    return {
        'total_students': total_students,
        'total_topics': len(topics),
        'total_capacity': total_capacity,
        'capacity_sufficient': total_capacity >= total_students,
        'first_choice_distribution': first_choices,
        'estimated_satisfaction': 'High' if total_capacity >= total_students * 1.2 else 'Medium'
    }


if __name__ == "__main__":
    # Test allocation
    from core.db import get_engine
    
    logging.basicConfig(level=logging.INFO)
    
    engine = get_engine()
    
    # Simulate
    result = simulate_allocation(
        engine=engine,
        subject_code='TEST-ELECT',
        ay_label='2024-25',
        year=3,
        term=1,
        degree_code='BTECH',
        dry_run=True
    )
    
    print("Simulation results:", result)