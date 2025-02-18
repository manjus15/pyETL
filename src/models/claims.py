from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, validator


class Hospital(BaseModel):
    """Model for hospital/center data."""
    center_id: str = Field(description="Center identifier")
    center_name: str = Field(description="Center name")
    cluster_name: Optional[str] = Field(None, description="Cluster name")
    cluster_sa: Optional[str] = Field(None, description="Cluster SA name")


class Plan(BaseModel):
    """Model for insurance plan data."""
    plan_id: str = Field(description="Plan identifier")
    category_id: Optional[str] = Field(None, description="Category identifier")
    plan_name: str = Field(description="Plan name")
    rateplan_rule_id: Optional[str] = Field(None, description="Rate plan rule identifier")
    category_name: Optional[str] = Field(None, description="Category name")


class Doctor(BaseModel):
    """Model for doctor data."""
    doctor_id: str = Field(description="Doctor identifier")
    doctor_name: str = Field(description="Doctor name")
    dept_id: Optional[str] = Field(None, description="Department identifier")
    dept_name: Optional[str] = Field(None, description="Department name")


class ClaimSubmission(BaseModel):
    """Model for claim submission data with enhanced fields from QlikView."""
    # Core fields
    claimid: str = Field(description="Unique identifier for the claim")
    memberid: str = Field(description="Member identifier")
    gross: float = Field(description="Gross amount")
    patientshare: float = Field(description="Patient share amount")
    netclaim: float = Field(description="Net claim amount")
    encountertype: str = Field(description="Type of encounter")
    activitynet: float = Field(description="Activity net amount")
    bill_finalized_date: datetime = Field(description="Bill finalization date")
    claim_status: str = Field(description="Status of the claim")
    center_id: str = Field(description="Center identifier")
    
    # Additional fields from QlikView
    org_sub_seq: Optional[int] = Field(None, description="Original submission sequence")
    cor_sub_seq: Optional[int] = Field(None, description="Correction submission sequence")
    is_claim_corrected: Optional[str] = Field(None, description="Claim correction flag")
    category: Optional[str] = Field(None, description="Claim category")
    submission_date: Optional[datetime] = Field(None, description="Submission date")
    is_correction: Optional[str] = Field(None, description="Correction flag")
    activityid: Optional[str] = Field(None, description="Activity identifier")
    activitytype: Optional[str] = Field(None, description="Type of activity")
    activitycode: Optional[str] = Field(None, description="Activity code")
    
    # Doctor related fields
    doctor_id: Optional[str] = Field(None, description="Doctor identifier")
    ordering_doc_id: Optional[str] = Field(None, description="Ordering doctor identifier")
    
    # Insurance related fields
    insurance_co_id: Optional[str] = Field(None, description="Insurance company identifier")
    plan_id: Optional[str] = Field(None, description="Insurance plan identifier")
    tpa_id: Optional[str] = Field(None, description="TPA identifier")
    
    # Clinical fields
    primary_icd: Optional[str] = Field(None, description="Primary ICD code")
    visit_id: Optional[str] = Field(None, description="Visit identifier")
    encounter_start_date: Optional[datetime] = Field(None, description="Encounter start date")
    
    # Status and tracking
    denial_code: Optional[str] = Field(None, description="Denial code if rejected")
    closure_type: Optional[str] = Field(None, description="Closure type")
    priority: Optional[int] = Field(None, description="Claim priority")
    
    class Config:
        from_attributes = True


class RemittanceAdvice(BaseModel):
    """Model for remittance advice data with enhanced fields."""
    # Core fields
    claim_id: str = Field(description="Reference to claim identifier")
    received_amount: float = Field(description="Amount received")
    activity_id: str = Field(description="Activity identifier")
    transaction_date: datetime = Field(description="Transaction date")
    insurance_claim_amt: float = Field(description="Insurance claim amount")
    org_id: int = Field(description="Organization identifier")
    
    # Additional fields
    ra_seq: Optional[int] = Field(None, description="Remittance sequence number")
    denial_code: Optional[str] = Field(None, description="Denial code if any")
    xml_payment_reference: Optional[str] = Field(None, description="Payment reference")
    reference_no: Optional[str] = Field(None, description="Reference number")
    bill_no: Optional[str] = Field(None, description="Bill number")
    
    # Derived fields from QlikView
    ra_date: Optional[datetime] = Field(None, description="Remittance date")
    ra_year: Optional[int] = Field(None, description="Remittance year")
    ra_month: Optional[int] = Field(None, description="Remittance month")
    first_received: Optional[datetime] = Field(None, description="First receipt date")
    second_received: Optional[datetime] = Field(None, description="Second receipt date")
    third_received: Optional[datetime] = Field(None, description="Third receipt date")
    
    class Config:
        from_attributes = True
        
    @validator('ra_year', 'ra_month', pre=True)
    def validate_date_parts(cls, v):
        if v is not None and not isinstance(v, int):
            try:
                return int(v)
            except (ValueError, TypeError):
                return None
        return v


class DenialCode(BaseModel):
    """Model for denial codes."""
    denial_code: str = Field(description="Denial code")
    denial_description: str = Field(description="Denial description")
    denial_responsibility: Optional[str] = Field(None, description="Denial responsibility")


class EncounterType(BaseModel):
    """Model for encounter types."""
    encountertype: str = Field(description="Encounter type code")
    encounter_type_desc: str = Field(description="Encounter type description") 