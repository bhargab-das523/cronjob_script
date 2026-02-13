import logging
import os
import pandas as pd
import pytz
from celery import shared_task
from dataclasses import dataclass
from django.core.exceptions import ValidationError
from django.db import transaction
from enum import Enum
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

from api.bbps_conts import (
    BILLER_PARAMS_CONFIG,
)
from api.consts import CREDIT_CARD_DPS_CACHE_KEY
from api.helpers.payu import (
    fetch_and_save_bill,
    fetch_bill_with_retries,
)
from api.helpers.redis import redis_hget, redis_hset
from api.models import BillDetail
from api.tasks import send_bill_generate_event

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set the timezone as IST
ist_tz = pytz.timezone("Asia/Kolkata")
TODAY = datetime.now(ist_tz).date()

CRON_EXECUTION_CSV_DIR = "/home/ubuntu/Documents/cron_execution_logs"


class BillType(Enum):
    """Enumeration for different bill types"""

    CREDIT_CARD = "credit_card"
    BAJA = "baja"
    DUE_DATE = "due_date"
    NO_DUE_DATE = "no_due_date"
    WEEKLY = "weekly"
    CUSTOM_FETCH = "custom_fetch"


class CronName(Enum):
    """Enumeration for bill fetch cron job names"""

    CREDIT_CARD = "auto_fetch_and_save_credit_card_bill"
    BAJA = "auto_fetch_and_save_baja_bill"
    DUE_DATE = "auto_fetch_and_save_due_date_bill"
    NO_DUE_DATE = "auto_fetch_and_save_no_due_date_bill"
    WEEKLY = "auto_fetch_and_save_weekly_bill"
    CUSTOM_FETCH = "auto_fetch_and_save_custom_fetch_bill"
    GMAIL = "gmail_auto_fetch_and_save_credit_card_bill"


@dataclass
class BillFetchConfig:
    """Configuration for bill fetching operations"""

    bill_type: BillType
    error_tag: str
    queue_name: str = "high_priority"
    check_due_date: bool = False
    use_retries: bool = False
    biller_id: Optional[str] = None


class BillFetchStrategy:
    """Strategy pattern for different bill fetching methods"""

    @staticmethod
    def fetch_standard_bill(
        customer_name: str,
        customer_phone_number: str,
        loan_id: str,
        customer_params: Dict,
    ) -> Tuple[Optional[Dict], Optional[str], Optional[str]]:
        """Standard bill fetching using fetch_and_save_bill"""
        return fetch_and_save_bill(
            customer_name=customer_name,
            customer_phone_number=customer_phone_number,
            loan_id=loan_id,
            customer_params=customer_params,
        )

    @staticmethod
    def fetch_baja_bill(
        customer_name: str,
        customer_phone_number: str,
        loan_id: str,
        unmasked_account_number: str,
    ) -> Tuple[Optional[Dict], Optional[str], Optional[str]]:
        """BAJA bill fetching with custom parameters and retries"""
        biller_id = "BAJA00000NATGS"

        # Optimize parameter formatting
        custom_params_retries = []
        for params in BILLER_PARAMS_CONFIG[biller_id]:
            formatted_params = {}
            for key, value in params.items():
                if isinstance(value, str) and value in {
                    "{account_number}",
                    "{phone_number}",
                }:
                    formatted_params[key] = value.format(
                        account_number=unmasked_account_number,
                        phone_number=customer_phone_number,
                    )
                else:
                    formatted_params[key] = value
            custom_params_retries.append(formatted_params)

        return fetch_bill_with_retries(
            customer_name=customer_name,
            customer_phone_number=customer_phone_number,
            loan_id=loan_id,
            bbps_biller_id=biller_id,
            custom_params_retries=custom_params_retries,
            save_bill=True,
        )


def validate_bill_fetch_inputs(
    account_info_id: str, customer_name: str, customer_phone_number: str
) -> bool:
    """Validate inputs for bill fetching"""
    if not account_info_id:
        logger.warning("Bill fetch called with empty account_info_id")
        return False

    if not customer_name or not customer_phone_number:
        logger.warning(f"Missing customer details for account {account_info_id}")
        return False

    return True


def add_account_to_next_day_cache(account_info_id: str) -> bool:
    """
    Add account_id to next day's cache for retry when ERR033 (rate limit) is received.

    Parameters:
        account_info_id (str): The account info primary key to add to next day's cache.

    Returns:
        bool: True if successfully added, False otherwise.
    """
    try:
        # Calculate tomorrow's date key (day of month)
        tomorrow = datetime.now(ist_tz).date() + timedelta(days=1)
        next_date_key = str(tomorrow.day)

        redis_key = CREDIT_CARD_DPS_CACHE_KEY

        # Get existing account_ids for next day
        existing_account_ids = redis_hget(
            redis_key, next_date_key, deserialize=True, is_central_cache=True
        )

        if existing_account_ids:
            # Add to existing list if not already present
            if account_info_id not in existing_account_ids:
                existing_account_ids.append(account_info_id)
                merged_account_ids = existing_account_ids
            else:
                # Already in cache, no need to update
                return True
        else:
            merged_account_ids = [account_info_id]

        # Store updated list back to Redis
        redis_hset(redis_key, next_date_key, merged_account_ids, is_central_cache=True)

        logger.info(
            f"[ERR033_RETRY] Added account {account_info_id} to next day cache (date_key: {next_date_key})"
        )
        return True

    except Exception as err:
        logger.error(
            f"[ERR033_RETRY] Failed to add account {account_info_id} to next day cache",
            exc_info=err,
        )
        return False


def write_account_info_to_csv(account_info_id: str, customer_phone_number: str) -> None:
    """
    Save account_info_id and customer_phone_number to a CSV file using pandas.
    The CSV filename is 'bill_fetch_without_notification_<today's date>.csv'
    """
    try:
        today_str = datetime.now(ist_tz).strftime("%Y-%m-%d")
        csv_filename = f"bill_fetch_without_notification_{today_str}.csv"

        # Create DataFrame with the new row
        new_row = pd.DataFrame(
            [
                {
                    "account_info_id": account_info_id,
                    "customer_phone_number": customer_phone_number,
                }
            ]
        )

        # Check if file exists to determine if we need header
        file_exists = os.path.exists(csv_filename)

        # Append to CSV (write header only if file doesn't exist)
        new_row.to_csv(
            csv_filename,
            mode="a",
            header=not file_exists,
            index=False,
        )

        logger.info(
            f"Saved account info to CSV: {account_info_id}, {customer_phone_number}"
        )
    except Exception as err:
        logger.error(
            f"Failed to save account info to CSV for {account_info_id}",
            exc_info=err,
        )


@transaction.atomic
def save_bill_and_notify(
    bill_detail: Dict,
    customer_phone_number: str,
    bill_type: BillType,
    skip_sms: bool = False,
    save_account_info_to_csv: bool = False,
    account_info_id: str = None,
) -> tuple[bool, BillDetail | None]:
    """Save bill detail and send notifiation if appropriate"""
    try:
        # Create bill detail
        obj = BillDetail.objects.create(**bill_detail)

        if skip_sms:
            logger.info(f"Skipping SMS notification for bill {obj.id} as skip_sms=True")
        else:
            try:
                bill_date = obj.due_date or obj.timestamp

                if bill_date >= TODAY:
                    send_bill_generate_event(
                        phone_number=customer_phone_number,
                        event_name="Bill_auto_fetch",
                        is_auto_refreshed=True,
                    )
            except Exception as err:
                logger.warning(
                    f"Failed to send bill generation event for {obj}", exc_info=err
                )

        # Save account info to CSV if requested
        if save_account_info_to_csv and account_info_id:
            write_account_info_to_csv(account_info_id, customer_phone_number)

        return True, obj

    except Exception as e:
        logger.error(f"Failed to save bill detail: {e}", exc_info=True)
        raise


def write_cron_execution_csv(row: Dict) -> None:
    """
    Append a single cron execution row to CSV.
    Filename: cron_execution_<YYYY-MM-DD>.csv
    Stored under CRON_EXECUTION_CSV_DIR
    """
    try:
        today_str = datetime.now(ist_tz).strftime("%Y-%m-%d")
        csv_filename = f"cron_execution_{today_str}.csv"

        os.makedirs(CRON_EXECUTION_CSV_DIR, exist_ok=True)
        file_path = os.path.join(CRON_EXECUTION_CSV_DIR, csv_filename)

        df = pd.DataFrame([row])
        file_exists = os.path.exists(file_path)

        df.to_csv(
            file_path,
            mode="a",
            header=not file_exists,  # create header only if current file doesn't exist
            index=False,
        )
    except Exception as err:
        logger.error("Failed to write cron execution CSV", exc_info=err)


def execute_bill_fetch(
    config: BillFetchConfig,
    account_info_id: str,
    bbps_biller_id: str,
    customer_name: str,
    customer_phone_number: str,
    retry_on_next_day: bool = False,
    skip_sms: bool = False,
    save_account_info_to_csv: bool = False,
    cron_name: str = "unknown_cron",
    **kwargs,
) -> Tuple[str, str, bool]:
    """Core bill fetching logic"""
    started_at = datetime.now(ist_tz).strftime("%Y-%m-%d")
    bill_detail = None
    bill_obj = None
    err_code = None
    err_msg = None
    is_bill_fetch = False
    customer_params = kwargs.get("customer_params", {})

    # Validate inputs
    if not validate_bill_fetch_inputs(
        account_info_id, customer_name, customer_phone_number
    ):
        return account_info_id, customer_phone_number, False

    account_info_id = str(account_info_id)

    try:
        # Fetch bill based on type
        if config.bill_type == BillType.BAJA:
            unmasked_account_number = kwargs.get("unmasked_account_number")
            if not unmasked_account_number:
                raise ValueError("unmasked_account_number is required for BAJA bills")

            bill_detail, err_msg, err_code = BillFetchStrategy.fetch_baja_bill(
                customer_name=customer_name,
                customer_phone_number=customer_phone_number,
                loan_id=account_info_id,
                unmasked_account_number=unmasked_account_number,
            )
        else:
            bill_detail, err_msg, err_code = BillFetchStrategy.fetch_standard_bill(
                customer_name=customer_name,
                customer_phone_number=customer_phone_number,
                loan_id=account_info_id,
                customer_params=customer_params,
            )

        # Process successful bill fetch
        if bill_detail:
            is_bill_fetch, bill_obj = save_bill_and_notify(
                bill_detail=bill_detail,
                customer_phone_number=customer_phone_number,
                bill_type=config.bill_type,
                skip_sms=skip_sms,
                save_account_info_to_csv=save_account_info_to_csv,
                account_info_id=account_info_id,
            )
        else:
            logger.info(
                f"{config.error_tag} No bill details returned for account. {account_info_id},{bbps_biller_id},{customer_params},{err_code},{err_msg}"
            )

            # Handle ERR033 and BBPSERR001 - add to next day's cache for retry
            # Only applies to gmail credit card cron flow
            if retry_on_next_day and err_code in {"ERR033", "BBPSERR001"}:
                add_account_to_next_day_cache(account_info_id)

    except ValidationError as e:
        logger.warning(
            f"{config.error_tag} Validation error for account {account_info_id}: {e}"
        )
    except Exception as e:
        logger.warning(
            f"{config.error_tag} Could not auto-fetch bill for account {account_info_id}: {e}",
            exc_info=True,
        )

    write_cron_execution_csv(
        {
            "cron_name": cron_name,
            "started_at": started_at,
            "account_info_id": account_info_id,
            "biller_id": bbps_biller_id,
            "failure_code": err_code,
            "failure_reason": err_msg,
            "bill_found": "Y" if bill_detail else "N",
            "bill_detail_pk": bill_obj.id if bill_obj else None,
            "bill_amount": bill_detail.get("amount") if bill_detail else None,
            "bill_date": bill_detail.get("timestamp") if bill_detail else None,
            "bill_due_date": bill_detail.get("due_date") if bill_detail else None,
            "param_json": customer_params,
        }
    )

    return account_info_id, bbps_biller_id, customer_phone_number, is_bill_fetch


# Optimized task definitions using the shared logic
@shared_task(queue="high_priority")
def auto_fetch_and_save_credit_card_bill(
    account_info_id: str,
    bbps_biller_id: str,
    customer_name: str,
    customer_phone_number: str,
    customer_params: Dict,
    retry_on_next_day: bool = False,
):
    """Optimized credit card bill fetching task"""
    config = BillFetchConfig(
        bill_type=BillType.CREDIT_CARD,
        error_tag="[BBPS_CREDIT_CARD]",
        check_due_date=True,
    )

    try:
        return execute_bill_fetch(
            config=config,
            account_info_id=account_info_id,
            bbps_biller_id=bbps_biller_id,
            customer_name=customer_name,
            customer_phone_number=customer_phone_number,
            retry_on_next_day=retry_on_next_day,
            customer_params=customer_params,
            cron_name=CronName.CREDIT_CARD.value,
        )
    except Exception as exc:
        logger.error(f"Credit card bill fetch failed for {account_info_id}: {exc}")

    return account_info_id, bbps_biller_id, customer_phone_number, False

@shared_task(queue="high_priority")
def gmail_auto_fetch_and_save_credit_card_bill(
    account_info_id: str,
    bbps_biller_id: str,
    customer_name: str,
    customer_phone_number: str,
    customer_params: Dict,
    retry_on_next_day: bool = False,
):
    """Optimized credit card bill fetching task"""
    config = BillFetchConfig(
        bill_type=BillType.CREDIT_CARD,
        error_tag="[BBPS_CREDIT_CARD]",
        check_due_date=True,
    )

    try:
        return execute_bill_fetch(
            config=config,
            account_info_id=account_info_id,
            bbps_biller_id=bbps_biller_id,
            customer_name=customer_name,
            customer_phone_number=customer_phone_number,
            retry_on_next_day=retry_on_next_day,
            customer_params=customer_params,
            cron_name=CronName.GMAIL.value,
        )
    except Exception as exc:
        logger.error(f"Credit card bill fetch failed for {account_info_id}: {exc}")

    return account_info_id, bbps_biller_id, customer_phone_number, False


@shared_task(queue="high_priority")
def auto_fetch_and_save_baja_bill(
    account_info_id: str,
    bbps_biller_id: str,
    customer_name: str,
    customer_phone_number: str,
    unmasked_account_number: str,
    skip_sms: bool = False,
    save_account_info_to_csv: bool = False,
):
    """Optimized BAJA bill fetching task"""
    config = BillFetchConfig(
        bill_type=BillType.BAJA,
        error_tag="[BBPS_BAJA]",
        use_retries=True,
        biller_id="BAJA00000NATGS",
    )

    try:
        return execute_bill_fetch(
            config=config,
            account_info_id=account_info_id,
            bbps_biller_id=bbps_biller_id,
            customer_name=customer_name,
            customer_phone_number=customer_phone_number,
            unmasked_account_number=unmasked_account_number,
            skip_sms=skip_sms,
            save_account_info_to_csv=save_account_info_to_csv,
            cron_name=CronName.BAJA.value,
        )
    except Exception as exc:
        logger.error(f"BAJA bill fetch failed for {account_info_id}: {exc}")

    return account_info_id, bbps_biller_id, customer_phone_number, False


@shared_task(queue="high_priority")
def auto_fetch_and_save_due_date_bill(
    account_info_id: str,
    bbps_biller_id: str,
    customer_name: str,
    customer_phone_number: str,
    customer_params: Dict,
):
    """Optimized due date bill fetching task"""
    config = BillFetchConfig(
        bill_type=BillType.DUE_DATE, error_tag="[BBPS_DUE_DATE_FETCH]"
    )

    try:
        return execute_bill_fetch(
            config=config,
            account_info_id=account_info_id,
            bbps_biller_id=bbps_biller_id,
            customer_name=customer_name,
            customer_phone_number=customer_phone_number,
            customer_params=customer_params,
            cron_name=CronName.DUE_DATE.value,
        )
    except Exception as exc:
        logger.error(f"Due date bill fetch failed for {account_info_id}: {exc}")

    return account_info_id, bbps_biller_id, customer_phone_number, False


@shared_task(queue="high_priority")
def auto_fetch_and_save_no_due_date_bill(
    account_info_id: str,
    bbps_biller_id: str,
    customer_name: str,
    customer_phone_number: str,
    customer_params: Dict,
):
    """Optimized no due date bill fetching task"""
    config = BillFetchConfig(
        bill_type=BillType.DUE_DATE, error_tag="[BBPS_NO_DUE_DATE_FETCH]"
    )

    try:
        return execute_bill_fetch(
            config=config,
            account_info_id=account_info_id,
            bbps_biller_id=bbps_biller_id,
            customer_name=customer_name,
            customer_phone_number=customer_phone_number,
            customer_params=customer_params,
            cron_name=CronName.NO_DUE_DATE.value,
        )
    except Exception as exc:
        logger.error(f"No due date bill fetch failed for {account_info_id}: {exc}")

    return account_info_id, bbps_biller_id, customer_phone_number, False


@shared_task(queue="high_priority")
def auto_fetch_and_save_weekly_bill(
    account_info_id: str,
    bbps_biller_id: str,
    customer_name: str,
    customer_phone_number: str,
    customer_params: Dict,
):
    """Optimized weekly bill fetching task"""
    config = BillFetchConfig(bill_type=BillType.WEEKLY, error_tag="[BBPS_WEEKLY_FETCH]")

    try:
        return execute_bill_fetch(
            config=config,
            account_info_id=account_info_id,
            bbps_biller_id=bbps_biller_id,
            customer_name=customer_name,
            customer_phone_number=customer_phone_number,
            customer_params=customer_params,
            cron_name=CronName.WEEKLY.value,
        )
    except Exception as exc:
        logger.error(f"Weekly bill fetch failed for {account_info_id}: {exc}")

    return account_info_id, bbps_biller_id, customer_phone_number, False


@shared_task(queue="high_priority")
def auto_fetch_and_save_custom_fetch_bill(
    account_info_id: str,
    bbps_biller_id: str,
    customer_name: str,
    customer_phone_number: str,
    customer_params: Dict,
    skip_sms: bool = False,
    save_account_info_to_csv: bool = False,
):
    """Optimized due date bill fetching task"""
    config = BillFetchConfig(
        bill_type=BillType.CUSTOM_FETCH, error_tag="[BBPS_CUSTOM_FETCH]"
    )

    try:
        return execute_bill_fetch(
            config=config,
            account_info_id=account_info_id,
            bbps_biller_id=bbps_biller_id,
            customer_name=customer_name,
            customer_phone_number=customer_phone_number,
            customer_params=customer_params,
            skip_sms=skip_sms,
            save_account_info_to_csv=save_account_info_to_csv,
            cron_name=CronName.CUSTOM_FETCH.value,
        )
    except Exception as exc:
        logger.error(f"Custom bill fetch failed for {account_info_id}: {exc}")

    return account_info_id, bbps_biller_id, customer_phone_number, False