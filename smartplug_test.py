import asyncio
from tapo import ApiClient
from my_secrets import TAPO_USERNAME, TAPO_PASSWORD

async def restart():
    client = ApiClient(TAPO_USERNAME, TAPO_PASSWORD)

    device = await client.p100("192.168.1.31")

    logger.debug(f"Turning off {self.name} smart plug")
    await device.off()

    logger.debug("Waiting 3 seconds...")
    await asyncio.sleep(3)

    logger.debug(f"Turning on {self.name} smart plug")
    await device.on()

asyncio.run(restart())
