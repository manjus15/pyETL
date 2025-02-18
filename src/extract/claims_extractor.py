from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
from config.config import Config
from src.database.connection import DatabaseConnection
from src.models.claims import (
    ClaimSubmission, RemittanceAdvice, Hospital, 
    Plan, Doctor, DenialCode, EncounterType
)


class ClaimsExtractor:
    """Extractor for claims and remittance data with master data relationships."""

    def __init__(self):
        self.db = DatabaseConnection()

    def extract_claims(
        self,
        start_date: datetime,
        end_date: datetime,
        center_ids: Optional[List[str]] = None,
        claim_status: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Extract claims data with master data joins.

        Args:
            start_date: Start date for bill finalization
            end_date: End date for bill finalization
            center_ids: Optional list of center IDs to filter
            claim_status: Optional list of claim statuses to filter
            limit: Optional limit on number of records

        Returns:
            DataFrame containing claims data with master information
        """
        query = """
            WITH latest_denial AS (
                SELECT 
                    claim_id,
                    activity_id,
                    denial_code as latest_denial_code
                FROM (
                    SELECT 
                        claim_id,
                        activity_id,
                        denial_code,
                        ROW_NUMBER() OVER (
                            PARTITION BY claim_id, activity_id 
                            ORDER BY ra_seq DESC
                        ) as rn
                    FROM nmc.f_remittance_advice
                    WHERE denial_code IS NOT NULL
                ) t
                WHERE rn = 1
            )
            SELECT 
                c.claimid, c.memberid, c.gross, c.patientshare, c.netclaim,
                c.encountertype, c.activitynet, c.bill_finalized_date,
                c.claim_status, c.center_id, c.activityid, c.activitytype,
                c.activitycode, c.org_id, c.submission_date,
                c.org_sub_seq, c.cor_sub_seq, c.is_claim_corrected,
                c.category, c.is_correction, c.doctor_id, c.ordering_doc_id,
                c.insurance_co_id, c.plan_id, c.tpa_id, c.primary_icd,
                c.visit_id, c.encounter_start_date, c.closure_type,
                c.priority, c.denial_code,
                -- Master data joins
                h.center_name, h.cluster_name,
                p.plan_name, p.category_name,
                d.doctor_name as doctor_name,
                d.dept_name as doctor_dept,
                od.doctor_name as ordering_doctor_name,
                od.dept_name as ordering_doctor_dept,
                et.encounter_type_desc,
                ld.latest_denial_code,
                dc.denial_description,
                dc.denial_responsibility
            FROM nmc.f_claim_submission c
            LEFT JOIN nmc.m_hospital_center_master h ON c.center_id = h.center_id
            LEFT JOIN nmc.m_cluster_master cm ON h.center_id = cm.center_id
            LEFT JOIN nmc.m_plans p ON c.plan_id = p.plan_id
            LEFT JOIN nmc.m_doctors d ON c.doctor_id = d.doctor_id
            LEFT JOIN nmc.m_doctors od ON c.ordering_doc_id = od.doctor_id
            LEFT JOIN nmc.m_encounter_type et ON c.encountertype = et.encountertype
            LEFT JOIN latest_denial ld ON c.claimid = ld.claim_id 
                AND c.activityid = ld.activity_id
            LEFT JOIN nmc.m_denial_codes dc ON ld.latest_denial_code = dc.denial_code
            WHERE c.bill_finalized_date BETWEEN %(start_date)s AND %(end_date)s
        """

        params: Dict = {
            "start_date": start_date,
            "end_date": end_date,
        }

        if center_ids:
            query += " AND c.center_id = ANY(%(center_ids)s)"
            params["center_ids"] = center_ids

        if claim_status:
            query += " AND c.claim_status = ANY(%(claim_status)s)"
            params["claim_status"] = claim_status

        if limit:
            query += f" LIMIT {limit}"

        results = self.db.execute_query(query, params)
        df = pd.DataFrame(results)

        if not df.empty:
            # Convert to proper data types
            numeric_cols = ["gross", "patientshare", "netclaim", "activitynet"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

            date_cols = [
                "bill_finalized_date", "submission_date", 
                "encounter_start_date"
            ]
            df[date_cols] = df[date_cols].apply(pd.to_datetime, errors="coerce")

            # Validate data using Pydantic model
            df = pd.DataFrame([
                ClaimSubmission(**record).model_dump()
                for record in df.to_dict(orient="records")
            ])

        return df

    def extract_remittance(
        self,
        start_date: datetime,
        end_date: datetime,
        org_ids: Optional[List[int]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Extract remittance advice data with sequence information.

        Args:
            start_date: Start date for transactions
            end_date: End date for transactions
            org_ids: Optional list of organization IDs to filter
            limit: Optional limit on number of records

        Returns:
            DataFrame containing remittance advice data
        """
        query = """
            WITH ra_dates AS (
                SELECT 
                    claim_id,
                    activity_id,
                    MIN(CASE WHEN ra_seq = 2 THEN transaction_date END) as first_received,
                    MIN(CASE WHEN ra_seq = 3 THEN transaction_date END) as second_received,
                    MIN(CASE WHEN ra_seq = 4 THEN transaction_date END) as third_received
                FROM nmc.f_remittance_advice
                GROUP BY claim_id, activity_id
            )
            SELECT 
                r.claim_id, r.received_amount, r.activity_id,
                r.transaction_date, r.insurance_claim_amt, r.org_id,
                r.denial_code, r.xml_payment_reference, r.reference_no,
                r.bill_no, r.ra_seq,
                DATE_TRUNC('day', r.transaction_date) as ra_date,
                EXTRACT(YEAR FROM r.transaction_date) as ra_year,
                EXTRACT(MONTH FROM r.transaction_date) as ra_month,
                rd.first_received, rd.second_received, rd.third_received,
                dc.denial_description, dc.denial_responsibility
            FROM nmc.f_remittance_advice r
            LEFT JOIN ra_dates rd ON r.claim_id = rd.claim_id 
                AND r.activity_id = rd.activity_id
            LEFT JOIN nmc.m_denial_codes dc ON r.denial_code = dc.denial_code
            WHERE r.transaction_date BETWEEN %(start_date)s AND %(end_date)s
        """

        params: Dict = {
            "start_date": start_date,
            "end_date": end_date,
        }

        if org_ids:
            query += " AND r.org_id = ANY(%(org_ids)s)"
            params["org_ids"] = org_ids

        if limit:
            query += f" LIMIT {limit}"

        results = self.db.execute_query(query, params)
        df = pd.DataFrame(results)

        if not df.empty:
            # Convert to proper data types
            numeric_cols = ["received_amount", "insurance_claim_amt"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

            date_cols = [
                "transaction_date", "ra_date", "first_received",
                "second_received", "third_received"
            ]
            df[date_cols] = df[date_cols].apply(pd.to_datetime, errors="coerce")

            # Validate data using Pydantic model
            df = pd.DataFrame([
                RemittanceAdvice(**record).model_dump()
                for record in df.to_dict(orient="records")
            ])

        return df

    def extract_matched_claims_remittance(
        self,
        start_date: datetime,
        end_date: datetime,
        org_ids: Optional[List[int]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Extract matched claims and remittance data with master information.

        The join logic follows QlikView script's approach, considering:
        - Activity ID transformations (removing -I and -R suffixes)
        - Sequence tracking
        - Master data relationships

        Args:
            start_date: Start date for transactions
            end_date: End date for transactions
            org_ids: Optional list of organization IDs to filter
            limit: Optional limit on number of records

        Returns:
            DataFrame containing matched claims and remittance data
        """
        query = """
            WITH cleaned_activity AS (
                SELECT 
                    *,
                    CASE 
                        WHEN activityid LIKE '%-I%' THEN 
                            SUBSTRING(activityid, 1, POSITION('-I' IN activityid) - 1)
                        WHEN activityid LIKE '%-R%' THEN 
                            SUBSTRING(activityid, 1, POSITION('-R' IN activityid) - 1)
                        ELSE activityid
                    END as cleaned_activityid
                FROM nmc.f_claim_submission
            )
            SELECT 
                c.claimid, c.memberid, c.gross, c.patientshare, c.netclaim,
                c.encountertype, c.activitynet, c.bill_finalized_date,
                c.claim_status, c.center_id, c.cleaned_activityid as activityid,
                c.org_sub_seq, c.cor_sub_seq, c.is_claim_corrected,
                r.received_amount, r.insurance_claim_amt, r.transaction_date,
                r.denial_code, r.reference_no, r.ra_seq,
                h.center_name, h.cluster_name,
                p.plan_name, p.category_name,
                d.doctor_name, d.dept_name,
                et.encounter_type_desc,
                dc.denial_description, dc.denial_responsibility,
                CASE 
                    WHEN c.claim_status = 'N' THEN 'Pending Reconciliation'
                    WHEN c.claim_status = 'S' AND c.org_sub_seq = 1 
                        AND COALESCE(r.ra_seq, 0) = 0 THEN 'Submitted'
                    WHEN c.claim_status = 'S' AND c.org_sub_seq = 1 
                        AND COALESCE(r.ra_seq, 0) >= 1 THEN 'Denied'
                    -- Add more status logic as per QlikView script
                    ELSE 'Unknown'
                END as new_claim_status
            FROM cleaned_activity c
            INNER JOIN nmc.f_remittance_advice r 
                ON c.claimid = r.claim_id 
                AND c.cleaned_activityid = r.activity_id
            LEFT JOIN nmc.m_hospital_center_master h ON c.center_id = h.center_id
            LEFT JOIN nmc.m_plans p ON c.plan_id = p.plan_id
            LEFT JOIN nmc.m_doctors d ON c.doctor_id = d.doctor_id
            LEFT JOIN nmc.m_encounter_type et ON c.encountertype = et.encountertype
            LEFT JOIN nmc.m_denial_codes dc ON r.denial_code = dc.denial_code
            WHERE r.transaction_date BETWEEN %(start_date)s AND %(end_date)s
        """

        params: Dict = {
            "start_date": start_date,
            "end_date": end_date,
        }

        if org_ids:
            query += " AND c.org_id = ANY(%(org_ids)s)"
            params["org_ids"] = org_ids

        if limit:
            query += f" LIMIT {limit}"

        results = self.db.execute_query(query, params)
        return pd.DataFrame(results)

    def extract_master_data(self, master_type: str) -> pd.DataFrame:
        """Extract master data.

        Args:
            master_type: Type of master data to extract 
                (hospital, plan, doctor, denial, encounter)

        Returns:
            DataFrame containing master data
        """
        master_queries = {
            "hospital": """
                SELECT h.center_id, h.center_name, 
                       c.cluster_name, UPPER(c.cluster_name) as cluster_sa
                FROM nmc.m_hospital_center_master h
                LEFT JOIN nmc.m_cluster_master c ON h.center_id = c.center_id
                WHERE h.center_id != '0'
            """,
            "plan": """
                SELECT p.plan_id, p.category_id, p.plan_name, 
                       p.rateplan_rule_id, n.category_name
                FROM nmc.m_plans p
                LEFT JOIN nmc.m_network_type n ON p.category_id = n.category_id
            """,
            "doctor": """
                SELECT d.doctor_id, d.doctor_name, d.dept_id, dp.dept_name
                FROM nmc.m_doctors d
                LEFT JOIN nmc.m_department dp ON d.dept_id = dp.dept_id
            """,
            "denial": """
                SELECT denial_code, code_description as denial_description,
                       responsibility as denial_responsibility
                FROM nmc.m_denial_codes
            """,
            "encounter": """
                SELECT encountertype, encounter_type_desc
                FROM nmc.m_encounter_type
            """
        }

        if master_type not in master_queries:
            raise ValueError(f"Invalid master_type: {master_type}")

        results = self.db.execute_query(master_queries[master_type])
        df = pd.DataFrame(results)

        # Validate using appropriate Pydantic model
        model_map = {
            "hospital": Hospital,
            "plan": Plan,
            "doctor": Doctor,
            "denial": DenialCode,
            "encounter": EncounterType
        }

        if not df.empty:
            df = pd.DataFrame([
                model_map[master_type](**record).model_dump()
                for record in df.to_dict(orient="records")
            ])

        return df 