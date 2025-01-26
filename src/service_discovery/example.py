from service_watcher import ServiceWatcher
from zmq import Poller
from service_announcement import ServiceAnnouncement
from loguru import logger
from socket import gethostname, gethostbyname
import time
from rich import print
import sys
import datetime as dt
from logging_helper.logging_utils import LogFilters, custom_log_format


def get_log_configuration():
    log_filters = LogFilters()
    log_filters.add_filter(
        where="DiscoveryAgent",
        inside=["check_for_peer_pings", "send_ping_info", "remove_dead_peers"],
    )
    # log_filters.add_filter(where="PeerRegistry", inside=["add_peer", "remove_peer"])
    # log_filters.add_filter(where="Service Discovery Example", inside=["main"])
    # log_filters.add_filter(where="MulticastHelper", inside=["receive", "send"])
    return log_filters


logger.remove()
# Uncomment the next line to see the logs
logger.add(sys.stdout, filter=get_log_configuration(), level="DEBUG")


def main():
    log = logger.bind(where="Service Discovery Example", inside="main")

    my_ip = gethostbyname(gethostname())
    my_services = [
        ServiceAnnouncement(
            service="file.storage.local",
            ip=my_ip,
            port=8080,
        ),
        ServiceAnnouncement(
            service="tag.storage.local",
            ip=my_ip,
            port=8082,
        ),
        ServiceAnnouncement(
            service="node.chord.local",
            ip=my_ip,
            port=8081,
        ),
    ]

    watcher = ServiceWatcher(
        services_to_announce=my_services,
        expiration_time=dt.timedelta(seconds=10),
        # enable_logs=True,
    )
    log.info(f"Service watcher created... MI IP is {my_ip}")

    while True:
        print(
            "================================= Update ==================================\n\n"
        )
        result = watcher.get_services("file.storage.local", max_amount=10)
        print(f"The peers are -> {result}")
        print(
            "\n===========================================================================\n\n"
        )
        time.sleep(3)


if __name__ == "__main__":
    main()
