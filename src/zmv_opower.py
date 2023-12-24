""" ZMV-adapted module for pulling & return Con Ed OPower data """

import asyncio
from datetime import datetime, timedelta

import aiohttp
import pandas as pd

from opower import AggregateType, MeterType, Opower
import zmv_const as const


async def get_opower_electric_data(num_days: int = 7) -> pd.DataFrame:
    utility = const.CONED_UTILITY_NAME
    username = const.CONED_USERNAME
    password = const.CONED_PASSWORD
    mfa_secret = const.CONED_MFA_SECRET
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
    utility = const.CONED_UTILITY_NAME
    username = const.CONED_USERNAME
    password = const.CONED_PASSWORD
    mfa_secret = const.CONED_MFA_SECRET
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
