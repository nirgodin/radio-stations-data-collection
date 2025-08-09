from time import sleep
from typing import Optional

from genie_common.utils import random_port
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


class PlaywrightContainer(DockerContainer):
    def __init__(self, version: str = "1.52.0", port: Optional[int] = None):
        super().__init__(image=f"mcr.microsoft.com/playwright:v{version}-noble")
        self.port = port or random_port()
        self.with_exposed_ports(self.port)
        self.with_command(f'/bin/sh -c "npx -y playwright@{version} run-server --port {self.port} --host 0.0.0.0"')
        self.with_kwargs(user="pwuser", init=True, privileged=False)

    def start(self) -> "PlaywrightContainer":
        super().start()
        wait_for_logs(self, f"Listening on ws://0.0.0.0:{self.port}/", timeout=60)
        sleep(10)

        return self
