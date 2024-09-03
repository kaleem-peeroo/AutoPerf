import asyncio
from tapo import ApiClient
from pprint import pprint

from my_secrets import TAPO_USERNAME, TAPO_PASSWORD

async def main():
    client = ApiClient(TAPO_USERNAME, TAPO_PASSWORD)
    device = await client.p100('192.168.1.102')

    device_info = await device.get_device_info()
    device_usage = await device.get_device_usage()
    pprint(device_info.to_dict())
    pprint(device_usage.to_dict())

asyncio.run(main())
