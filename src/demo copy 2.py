"""Demo usage of Opower library."""

import asyncio
from datetime import datetime, timedelta
import os
from typing import Optional

import aiohttp
from dotenv import load_dotenv
import pandas as pd

from opower import AggregateType, MeterType, Opower

load_dotenv()
CONED_USERNAME = os.getenv("CONED_USERNAME")
CONED_PASSWORD = os.getenv("CONED_PASSWORD")
CONED_MFA_SECRET = os.getenv("CONED_MFA_SECRET")


async def _main() -> None:
    utility = "coned"
    username = CONED_USERNAME
    password = CONED_PASSWORD
    mfa_secret = CONED_MFA_SECRET
    start_date = (datetime.now() - timedelta(days=14))
    end_date = datetime.now()

    async with aiohttp.ClientSession() as session:
        opower = Opower(session, utility, username, password, mfa_secret)
        await opower.async_login()
        # Re-login to make sure code handles already logged in sessions.
        await opower.async_login()
        for account in await opower.async_get_accounts():
            if account.meter_type == MeterType.ELEC:
                aggregate_type = AggregateType.QUARTER_HOUR
            elif account.meter_type == MeterType.GAS:
                aggregate_type = AggregateType.HOUR
            print(
                "\nGetting historical data: account=",
                account,
                "aggregate_type=",
                aggregate_type,
                "start_date=",
                start_date,
                "end_date=",
                end_date,
            )
            prev_end: Optional[datetime] = None
            usage_data = await opower.async_get_usage_reads(
                account,
                aggregate_type,
                start_date,
                end_date,
            )

            df = pd.DataFrame(usage_data)
            df['type'] = account.meter_type
            print(df)
            # print(
            #     "start_time\tend_time\tconsumption"
            #     "\tstart_minus_prev_end\tend_minus_prev_end"
            # )
            # for usage_read in usage_data:
            #     start_minus_prev_end = (
            #         None if prev_end is None else usage_read.start_time - prev_end
            #     )
            #     end_minus_prev_end = (
            #         None if prev_end is None else usage_read.end_time - prev_end
            #     )
            #     prev_end = usage_read.end_time
            #     print(
            #         f"{usage_read.start_time}"
            #         f"\t{usage_read.end_time}"
            #         f"\t{usage_read.consumption}"
            #         f"\t{start_minus_prev_end}"
            #         f"\t{end_minus_prev_end}"
            #     )
            # print()


asyncio.run(_main())
