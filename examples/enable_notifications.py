# -*- coding: utf-8 -*-
"""
Notifications
-------------

Example showing how to add notifications to a characteristic and handle the responses.

Updated on 2019-07-03 by hbldh <henrik.blidh@gmail.com>

"""

import logging
import anyio
import platform

from bleak import BleakClient
from bleak import _logger as logger


CHARACTERISTIC_UUID = "f000aa65-0451-4000-b000-000000000000"  # <--- Change to the characteristic you want to enable notifications from.


async def run(address, debug=False):
    if debug:
        import sys

        l = logging.getLogger("asyncio")
        l.setLevel(logging.DEBUG)
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(logging.DEBUG)
        l.addHandler(h)
        logger.addHandler(h)

    async with BleakClient(address) as client:
        x = await client.is_connected()
        logger.info("Connected: {0}".format(x))

        with anyio.move_on_after(5.0):
            async with client.notification(CHARACTERISTIC_UUID) as msgs:
                async for data in msgs:
                    print(f"{CHARACTERISTIC_UUID}: {data!r}")


if __name__ == "__main__":
    import os

    os.environ["PYTHONASYNCIODEBUG"] = str(1)
    address = (
        "24:71:89:cc:09:05"  # <--- Change to your device's address here if you are using Windows or Linux
        if platform.system() != "Darwin"
        else "B9EA5233-37EF-4DD6-87A8-2A875E821C46"  # <--- Change to your device's address here if you are using macOS
    )
    anyio.run(run, backend="asyncio")
