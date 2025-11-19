# screens/electives_topics/offering_creation.py
"""
Post-allocation offering creation for electives.
Creates offerings after students are allocated to topics.
"""

from typing import List, Dict, Optional, Tuple
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger(__name__)


def _exec(conn, sql: str, params: dict = None):
    """Execute SQL with parameters."""
    return conn.execute(sa_text(sql), params or {})


def _fetch_one(conn, sql: str, params: dict = None) -> Optional[Dict]:
    row = _exec(conn, sql, params or {}).fetchone()
    return dict(row._mapping) if row else None


def _fetch_all(conn, sql: str, params: dict = None) -> List[Dict]:
    return [dict(r._mapping) for r in _exec(conn, sql, params or {}).fetchall()]


def get_catalog_subject_id(conn, subject_code: str, degree_code: str) -> Optional[int]:
    """
    Get catalog subject ID for a given subject_code + degree_code.
    """
    row = _fetch_one(conn, """
        SELECT id 
        FROM subjects_catalog
        WHERE subject_code = :sc
          AND degree_code = :deg
    """, {"sc": subject_code, "deg": degree_code})
    
    return row["id"] if row else None


def get_allocated_topics(
    engine: Engine,
    subject_code: str,
    ay_label: str,
    year: int,
    term: int,
    degree_code: str
) -> List[Dict]:
    """
    Get topics that have student allocations.
    Returns topics with student count and details.
    """
    with engine.begin() as conn:
        topics = _fetch_all(conn, """
            SELECT 
                et.id,
                et.topic_code_ay,
                et.topic_name,
                et.subject_code,
                et.degree_code,
                et.program_code,
                et.branch_code,
                et.division_code,
                et.owner_faculty_email,
                et.capacity,
                COUNT(DISTINCT ess.student_id) AS student_count
            FROM elective_topics et
            LEFT JOIN elective_student_selections ess
                ON ess.topic_code_ay = et.topic_code_ay
                AND ess.ay_label = et.ay_label
                AND ess.status = 'confirmed'
            WHERE et.subject_code = :subj
              AND et.ay_label = :ay
              AND et.year = :yr
              AND et.term = :trm
              AND et.degree_code = :deg
              AND et.status = 'published'
            GROUP BY et.id
            HAVING student_count > 0
            ORDER BY et.topic_no
        """, {
            "subj": subject_code,
            "ay": ay_label,
            "yr": year,
            "trm": term,
            "deg": degree_code
        })
        
        return topics


def create_offering_for_topic(
    engine: Engine,
    topic: Dict,
    catalog_subject_id: int,
    ay_label: str,
    year: int,
    term: int,
    actor: str
) -> int:
    """
    Create a subject_offering for a specific elective topic.
    
    This creates the offering that will be used for attendance, grades, etc.
    """
    with engine.begin() as conn:
        # Check if offering already exists (per-topic)
        existing = _fetch_one(conn, """
            SELECT id FROM subject_offerings
            WHERE subject_code = :sc
              AND degree_code = :dc
              AND ay_label = :ay
              AND year = :yr
              AND term = :trm
              AND COALESCE(program_code, '') = COALESCE(:pc, '')
              AND COALESCE(branch_code, '') = COALESCE(:bc, '')
              AND COALESCE(division_code, '') = COALESCE(:div, '')
        """, {
            "sc": topic['topic_code_ay'],
            "dc": topic['degree_code'],
            "ay": ay_label,
            "yr": year,
            "trm": term,
            "pc": topic.get('program_code'),
            "bc": topic.get('branch_code'),
            "div": topic.get('division_code')
        })
        
        if existing:
            logger.info(
                f"Offering already exists for topic {topic['topic_code_ay']} (ID={existing['id']})"
            )
            return existing["id"]
        
        # Get catalog subject details
        catalog = _fetch_one(conn, """
            SELECT * FROM subjects_catalog WHERE id = :id
        """, {"id": catalog_subject_id})
        
        if not catalog:
            raise ValueError(f"Catalog subject {catalog_subject_id} not found")
        
        # Create offering for this topic
        result = _exec(conn, """
            INSERT INTO subject_offerings (
                subject_code,
                base_subject_code,
                topic_code_ay,
                topic_name,
                is_elective_topic,
                degree_code,
                program_code,
                branch_code,
                curriculum_group_code,
                ay_label,
                year,
                term,
                division_code,
                applies_to_all_divisions,
                subject_type,
                is_elective_parent,
                instructor_email,
                credits_total,
                L, T, P, S,
                internal_marks_max,
                exam_marks_max,
                jury_viva_marks_max,
                total_marks_max,
                pass_threshold_overall,
                pass_threshold_internal,
                pass_threshold_external,
                direct_weight_percent,
                indirect_weight_percent,
                status,
                override_inheritance,
                created_by
            ) VALUES (
                :sc, :base_sc, :topic_ay, :topic_name, :is_topic,
                :dc, :pc, :bc, :cg,
                :ay, :yr, :trm, :div, :all_div,
                :stype, :elec_parent, :instr,
                :cred, :L, :T, :P, :S,
                :int_max, :exam_max, :jury_max, :total_max,
                :pass_ov, :pass_int, :pass_ext,
                :dir_w, :ind_w,
                :status, :override, :actor
            )
        """, {
            "sc": topic['topic_code_ay'],
            "base_sc": catalog['subject_code'],
            "topic_ay": topic['topic_code_ay'],
            "topic_name": topic['topic_name'],
            "is_topic": 1,
            "dc": topic['degree_code'],
            "pc": topic.get('program_code'),
            "bc": topic.get('branch_code'),
            "cg": catalog.get('curriculum_group_code'),
            "ay": ay_label,
            "yr": year,
            "trm": term,
            "div": topic.get('division_code'),
            "all_div": 0 if topic.get('division_code') else 1,
            "stype": catalog['subject_type'],
            "elec_parent": 0,  # parent bucket offering remains separate; this row is a topic-level offering
            "instr": topic.get('owner_faculty_email'),
            "cred": catalog['credits_total'],
            "L": catalog.get('L', 0),
            "T": catalog.get('T', 0),
            "P": catalog.get('P', 0),
            "S": catalog.get('S', 0),
            "int_max": catalog.get('internal_marks_max', 40),
            "exam_max": catalog.get('exam_marks_max', 60),
            "jury_max": catalog.get('jury_viva_marks_max', 0),
            "total_max": catalog.get('internal_marks_max', 40)
                         + catalog.get('exam_marks_max', 60)
                         + catalog.get('jury_viva_marks_max', 0),
            "pass_ov": catalog.get('min_overall_percent', 40.0),
            "pass_int": catalog.get('min_internal_percent', 50.0),
            "pass_ext": catalog.get('min_external_percent', 40.0),
            "dir_w": catalog.get('direct_internal_weight_percent', 80.0),
            "ind_w": 100.0 - catalog.get('direct_internal_weight_percent', 80.0),
            "status": "published",
            "override": 0,
            "actor": actor
        })
        
        offering_id = result.lastrowid
        
        # Link offering back to topic
        _exec(conn, """
            UPDATE elective_topics
            SET offering_id = :oid
            WHERE id = :tid
        """, {"oid": offering_id, "tid": topic['id']})
        
        logger.info(f"Created offering {offering_id} for topic {topic['topic_code_ay']}")
        return offering_id


def create_offerings_from_allocations(
    engine: Engine,
    subject_code: str,
    ay_label: str,
    year: int,
    term: int,
    degree_code: str,
    actor: str,
    auto_publish: bool = True
) -> Tuple[List[int], List[str]]:
    """
    Create offerings for all allocated topics after allocation is complete.
    
    Args:
        engine: SQLAlchemy engine
        subject_code: Base elective subject code (from catalog)
        ay_label: Academic year
        year: Year of study
        term: Term number
        degree_code: Degree code
        actor: User creating offerings
        auto_publish: If True, set offerings to published status
    
    Returns:
        (list of offering IDs, list of messages)
    """
    messages: List[str] = []
    offering_ids: List[int] = []
    
    with engine.begin() as conn:
        # Get catalog subject ID
        catalog_id = get_catalog_subject_id(conn, subject_code, degree_code)
        if not catalog_id:
            return [], [f"Error: Subject {subject_code} not found in catalog"]
    
    # Get allocated topics
    topics = get_allocated_topics(engine, subject_code, ay_label, year, term, degree_code)
    
    if not topics:
        return [], ["No topics with student allocations found"]
    
    messages.append(f"Found {len(topics)} topics with student allocations")
    
    # Create offerings for each topic
    for topic in topics:
        try:
            offering_id = create_offering_for_topic(
                engine,
                topic,
                catalog_id,
                ay_label,
                year,
                term,
                actor
            )
            offering_ids.append(offering_id)
            messages.append(
                f"✅ Topic '{topic['topic_name']}' ({topic['topic_code_ay']}): "
                f"Offering {offering_id} created for {topic['student_count']} students"
            )
        except Exception as e:
            msg = f"❌ Topic '{topic.get('topic_name')}' ({topic.get('topic_code_ay')}): Error - {str(e)}"
            messages.append(msg)
            logger.error(msg, exc_info=True)
    
    messages.append(
        f"\nSummary: Created {len(offering_ids)} offerings out of {len(topics)} topics"
    )
    
    return offering_ids, messages


def link_students_to_offerings(
    engine: Engine,
    subject_code: str,
    ay_label: str,
    year: int,
    term: int,
    degree_code: str
) -> Tuple[int, List[str]]:
    """
    Link confirmed student selections to their topic's offering.
    
    This will insert rows into student_enrollment with enrollment_type='elective'.
    """
    messages: List[str] = []
    count = 0
    
    with engine.begin() as conn:
        selections = _fetch_all(conn, """
            SELECT 
                ess.student_id,
                ess.student_roll_no,
                ess.topic_code_ay,
                et.offering_id,
                et.topic_name
            FROM elective_student_selections ess
            JOIN elective_topics et
                ON et.topic_code_ay = ess.topic_code_ay
                AND et.ay_label = ess.ay_label
            WHERE ess.subject_code = :subj
              AND ess.ay_label = :ay
              AND ess.year = :yr
              AND ess.term = :trm
              AND ess.degree_code = :deg
              AND ess.status = 'confirmed'
              AND et.offering_id IS NOT NULL
        """, {
            "subj": subject_code,
            "ay": ay_label,
            "yr": year,
            "trm": term,
            "deg": degree_code
        })
        
        if not selections:
            return 0, ["No confirmed selections with offerings found"]
        
        messages.append(f"Found {len(selections)} confirmed selections to link")
        
        for sel in selections:
            try:
                # Avoid duplicates
                existing = _fetch_one(conn, """
                    SELECT id FROM student_enrollment
                    WHERE student_id = :sid AND offering_id = :oid
                """, {"sid": sel['student_id'], "oid": sel['offering_id']})
                
                if existing:
                    continue
                
                _exec(conn, """
                    INSERT INTO student_enrollment (
                        student_id,
                        offering_id,
                        enrollment_type,
                        status,
                        enrolled_at
                    ) VALUES (
                        :sid, :oid, 'elective', 'active', CURRENT_TIMESTAMP
                    )
                """, {"sid": sel['student_id'], "oid": sel['offering_id']})
                
                count += 1
                
            except Exception as e:
                messages.append(
                    f"❌ Student {sel['student_roll_no']}: Error - {str(e)}"
                )
                logger.error(f"Error linking student {sel['student_id']} to offering {sel['offering_id']}: {e}")
        
        messages.append(f"✅ Linked {count} students to offerings")
    
    return count, messages
