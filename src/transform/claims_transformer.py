from typing import Dict, List

import pandas as pd
import numpy as np


class ClaimsTransformer:
    """Transformer for claims and remittance data."""

    @staticmethod
    def calculate_claim_metrics(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate key metrics for claims data.

        Args:
            df: DataFrame containing claims data

        Returns:
            DataFrame with additional calculated metrics
        """
        # Calculate reimbursement rate
        df["reimbursement_rate"] = (df["netclaim"] / df["gross"]).round(4)

        # Calculate patient share percentage
        df["patient_share_pct"] = (df["patientshare"] / df["gross"]).round(4)

        # Calculate days to finalize (if submission_date exists)
        if "submission_date" in df.columns and "bill_finalized_date" in df.columns:
            df["days_to_finalize"] = (
                df["bill_finalized_date"] - df["submission_date"]
            ).dt.total_seconds() / (24 * 60 * 60)
            df["days_to_finalize"] = df["days_to_finalize"].round(0)

        return df

    @staticmethod
    def calculate_remittance_metrics(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate key metrics for remittance data.

        Args:
            df: DataFrame containing remittance data

        Returns:
            DataFrame with additional calculated metrics
        """
        # Calculate payment ratio
        df["payment_ratio"] = (df["received_amount"] / df["insurance_claim_amt"]).round(4)

        # Flag full payments
        df["is_full_payment"] = df["payment_ratio"] >= 0.99

        # Flag denials
        df["is_denied"] = df["denial_code"].notna()

        return df

    @staticmethod
    def calculate_matched_metrics(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate metrics for matched claims and remittance data.

        Args:
            df: DataFrame containing matched claims and remittance data

        Returns:
            DataFrame with additional calculated metrics
        """
        # Calculate payment efficiency
        df["payment_efficiency"] = (df["received_amount"] / df["netclaim"]).round(4)

        # Calculate days to payment
        df["days_to_payment"] = (
            df["transaction_date"] - df["bill_finalized_date"]
        ).dt.total_seconds() / (24 * 60 * 60)
        df["days_to_payment"] = df["days_to_payment"].round(0)

        # Flag delayed payments (more than 30 days)
        df["is_delayed_payment"] = df["days_to_payment"] > 30

        # Calculate write-off amount
        df["write_off_amount"] = (df["netclaim"] - df["received_amount"]).round(2)

        return df

    @staticmethod
    def aggregate_by_center(df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate claims data by center.

        Args:
            df: DataFrame containing claims data

        Returns:
            DataFrame with center-level aggregations
        """
        agg_dict = {
            "claimid": "count",
            "gross": "sum",
            "netclaim": "sum",
            "received_amount": "sum",
            "write_off_amount": "sum",
            "days_to_payment": "mean",
            "is_delayed_payment": "mean",
            "payment_efficiency": "mean",
        }

        center_stats = df.groupby("center_id").agg(agg_dict).round(2)
        center_stats.columns = [
            "total_claims",
            "total_gross",
            "total_net",
            "total_received",
            "total_write_off",
            "avg_days_to_payment",
            "delayed_payment_rate",
            "avg_payment_efficiency",
        ]

        return center_stats

    @staticmethod
    def analyze_denial_patterns(df: pd.DataFrame) -> pd.DataFrame:
        """Analyze patterns in claim denials.

        Args:
            df: DataFrame containing claims data with denial information

        Returns:
            DataFrame with denial analysis
        """
        if "denial_code" not in df.columns:
            return pd.DataFrame()

        denial_stats = df[df["denial_code"].notna()].groupby("denial_code").agg({
            "claimid": "count",
            "netclaim": "sum",
            "received_amount": "sum",
        }).round(2)

        denial_stats.columns = ["denied_claims", "denied_amount", "recovered_amount"]
        denial_stats["recovery_rate"] = (
            denial_stats["recovered_amount"] / denial_stats["denied_amount"]
        ).round(4)

        return denial_stats.sort_values("denied_claims", ascending=False) 