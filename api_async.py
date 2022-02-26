import asyncio
import aiohttp
import os
import time
import json


url = 'https://sellermetrix.com/api/v2/cached-reports/'
headers = {'Connection': 'keep-alive',
           'Accept': 'application/json, text/plain, */*',
           'Authorization': 'JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6InBlcm9sYS5lcmljc3NvbkBnbWFpbC5jb20iLCJpYXQiOjE2NDQyOTkxMzgsImV4cCI6MTY0Njg5MTEzOCwianRpIjoiNDUwYTMyZGUtMTVkNi00NjI4LTkzMDctYzFkZjQ0MGRlNDAwIiwidXNlcl9pZCI6MTUsInVzZXJfcHJvZmlsZV9pZCI6WzEzXSwib3JpZ19pYXQiOjE2NDQyOTkxMzh9.FxLSD2CWgGePZ1a-kpm6-QBM2spUV85UPiGSN8fVG-I',
           'Content-Type': 'application/json;charset=UTF-8',
           'Origin': 'http://54.218.163.142',
           'Referer': 'http://54.218.163.142/reports'
           }


async def report_status(session, report_id, timeout) -> bool:
    """
    Checks report status by id.
    :param session:     aiohttp ClientSession
    :param report_id:   str
    :param timeout:     timeout in seconds for polling
    :return:            True if report is generated
    """
    time_start = time.time()
    report_is_ready = False
    try:
        while time.time() < time_start + timeout and not report_is_ready:
            async with await session.get(url, headers=headers, timeout=timeout, ssl=False) as resp:
                content_byte = await resp.content.read()
                content_json = json.loads(content_byte.decode('utf-8'))
                status = [item.get('status', '') == 'completed' for item in content_json if item.get('id', 0) == int(report_id)]
                if any(status):
                    report_is_ready = True
                else:
                    await asyncio.sleep(0.2)
    except Exception as e:
        print(e)

    return report_is_ready


async def request_report(session, mp_id, date_from, date_to, timeout) -> dict:
    """
    Requests and collects a report.
    :param session:     aiohttp ClientSession
    :param mp_id:       marketplace identifier
    :param date_from:   start date string
    :param date_to:     end date string
    :param timeout:     response timeout in seconds
    :return:            dict(date_range: report)
    """
    report = {}
    try:
        params = {
            "type": "profit-and-loss",
            "marketplace_id": mp_id,
            "brandIds": "",
            "productIds": "",
            "parentIds": "",
            "name": "Name",
            "from": date_from,
            "to": date_to
        }
        async with await session.post(url, headers=headers, json=params, timeout=timeout, ssl=False) as resp:
            if resp.reason == 'Created':
                report_id_bytes = await resp.content.read()
                report_id = report_id_bytes.decode('utf-8')
                if await report_status(session, report_id, timeout):
                    async with await session.get(url + report_id, headers=headers, timeout=timeout, ssl=False) as report_resp:
                        report_bytes = await report_resp.read()
                        report = json.loads(report_bytes.decode('utf-8'))
    except Exception as e:
        print(e)

    return {(date_from, date_to): report}


async def start(mp_id, date_range, timeout):
    """
    An asynchronous function to get reports by  marketplace id and date range.
    :param mp_id:       marketplace identifier
    :param date_range:  (date_from, date_to)
    :param timeout:     response timeout in seconds
    :return:            dict(date_range: report)
    """
    result = {}
    try:
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.create_task(request_report(session, mp_id, *dt_range, timeout)) for dt_range in date_range]
            done, _ = await asyncio.wait(tasks)
            result = {k: v for item in done for k, v in item.result().items()}
    except Exception as e:
        print(e)
    return result


def main_api(mp_id, date_range, timeout=10) -> dict:
    """
    Starts an asynchronous report retrieval process.
    :param mp_id:       marketplace identifier
    :param date_range:  (date_from, date_to)
    :param timeout:     response timeout in seconds
    :return:             dict(date_range: report)
    """
    return asyncio.run(start(mp_id, date_range, timeout))

