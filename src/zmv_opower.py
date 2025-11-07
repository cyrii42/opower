""" ZMV-adapted module for pulling & return Con Ed OPower data """

import asyncio
from datetime import datetime, timedelta

import aiohttp
import pandas as pd

from opower import AggregateType, MeterType, Opower
from zmv_const import CONED_UTILITY_NAME, CONED_USERNAME, CONED_PASSWORD, CONED_MFA_SECRET


async def get_opower_electric_data(num_days: int = 7) -> pd.DataFrame:
    utility = CONED_UTILITY_NAME
    username = CONED_USERNAME
    password = CONED_PASSWORD
    mfa_secret = CONED_MFA_SECRET
    aggregate_type = AggregateType.HALF_HOUR
    start_date = (datetime.now() - timedelta(days=num_days))
    end_date = datetime.now()

    async with aiohttp.ClientSession() as session:
        opower = Opower(session, utility, username, password, mfa_secret)
        await opower.async_login()
        # Re-login to make sure code handles already logged in sessions.
        await opower.async_login()
        for account in await opower.async_get_accounts():
            if account.meter_type == MeterType.GAS:
                continue
            elif account.meter_type == MeterType.ELEC:
                aggregate_type = AggregateType.QUARTER_HOUR
            usage_data = await opower.async_get_usage_reads(
                account,
                aggregate_type,
                start_date,
                end_date,
            )

            df = pd.DataFrame(usage_data)
            df['type'] = account.meter_type

    return df

async def get_opower_electric_data_custom_dates(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    utility = CONED_UTILITY_NAME
    username = CONED_USERNAME
    password = CONED_PASSWORD
    mfa_secret = CONED_MFA_SECRET
    aggregate_type = AggregateType.HALF_HOUR
    start_date = start_date
    end_date = end_date

    async with aiohttp.ClientSession() as session:
        opower = Opower(session, utility, username, password, mfa_secret)
        await opower.async_login()
        # Re-login to make sure code handles already logged in sessions.
        await opower.async_login()
        for account in await opower.async_get_accounts():
            if account.meter_type == MeterType.GAS:
                continue
            elif account.meter_type == MeterType.ELEC:
                aggregate_type = AggregateType.QUARTER_HOUR
            usage_data = await opower.async_get_usage_reads(
                account,
                aggregate_type,
                start_date,
                end_date,
            )

            df = pd.DataFrame(usage_data)
            df['type'] = account.meter_type

    return df

if __name__ == '__main__':
    ...
