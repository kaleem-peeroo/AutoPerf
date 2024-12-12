import asyncio
from tapo import ApiClient
from my_secrets import TAPO_USERNAME, TAPO_PASSWORD

async def restart():
    client = ApiClient(TAPO_USERNAME, TAPO_PASSWORD)

    plugs = [
        {"name": "3p1", "ip": "192.168.1.31"},
        {"name": "3p2", "ip": "192.168.1.31"},

        {"name": "5k2", "ip": "192.168.1.52"},
        {"name": "5k3", "ip": "192.168.1.53"},
        {"name": "5k4", "ip": "192.168.1.54"},
        {"name": "5k5", "ip": "192.168.1.55"},
    ]

    for plug in plugs:
        device = await client.p100(plug['ip'])

        print(f"Turning off {plug['name']} smart plug")
        await device.off()

        print("Waiting 3 seconds...")
        await asyncio.sleep(3)

        print(f"Turning on {plug['name']} smart plug")
        await device.on()

asyncio.run(restart())
