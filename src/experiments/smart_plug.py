import asyncio
import datetime

from tapo import ApiClient

from src.my_secrets import TAPO_USERNAME, TAPO_PASSWORD
from src.logger import logger

class SmartPlug:
    def __init__(
            self,
            name: str,
            ip: str,
        ):
        self.name       = name
        self.ip         = ip

    def get_name(self) -> str:
        return self.name

    def get_ip(self) -> str:
        return self.ip

    def get_now_timestamp(self) -> str:
        return datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')

    def set_ip(self, ip: str) -> None:
        self.ip = ip

    def set_name(self, name: str) -> None:
        self.name = name

    async def restart(self):
        client = ApiClient(TAPO_USERNAME, TAPO_PASSWORD)

        try:
            device = await client.p100(self.ip)

        except Exception as e:
            logger.error(
                f"Failed to get plug to restart for plug {self.name}:\n\t{e}"
            )
            with open('logs/tapo_restart.log', 'a+') as f:
                f.write(
                    "[{}] Failed to restart {} with IP {}: {}\n".format(
                        self.get_now_timestamp(),
                        self.name,
                        self.ip,
                        e
                    )
                )
            raise e

        logger.debug(f"Turning off {self.name} smart plug")
        await device.off()

        logger.debug("Waiting 3 seconds...")
        await asyncio.sleep(3)

        logger.debug(f"Turning on {self.name} smart plug")
        await device.on()

        with open('logs/tapo_restart.log', 'a+') as f:
            f.write(
                "[{}] Restarted {} smart plug with IP {}\n".format(
                    self.get_now_timestamp(),
                    self.name,
                    self.ip
                )
            )
