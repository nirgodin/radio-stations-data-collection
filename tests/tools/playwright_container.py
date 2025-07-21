from time import sleep

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


class PlaywrightContainer(DockerContainer):
    def __init__(
        self, image: str = "mcr.microsoft.com/playwright:v1.51.0-noble", port: int = 3001
    ):  # TODO: Use randomized port
        super().__init__(image=image)
        self.port = port
        self.with_exposed_ports(self.port)
        self.with_command(f'/bin/sh -c "npx -y playwright@1.51.0 run-server --port {port} --host 0.0.0.0"')
        self.with_kwargs(user="pwuser", init=True, privileged=False)

    def start(self) -> "PlaywrightContainer":
        super().start()
        # wait_for_logs(self, f"Listening on ws://0.0.0.0:{self.port}/", timeout=60)
        sleep(10)

        return self
