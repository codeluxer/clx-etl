import datetime
import os

ASCII_LOGO = r"""
 ______     ______     __     __   __        __         __  __     __  __     ______     ______
/\  ___\   /\  __ \   /\ \   /\ "-.\ \      /\ \       /\ \/\ \   /\_\_\_\   /\  ___\   /\  == \
\ \ \____  \ \ \/\ \  \ \ \  \ \ \-.  \     \ \ \____  \ \ \_\ \  \/_/\_\/_  \ \  __\   \ \  __<
 \ \_____\  \ \_____\  \ \_\  \ \_\\"\_\     \ \_____\  \ \_____\   /\_\/\_\  \ \_____\  \ \_\ \_\
  \/_____/   \/_____/   \/_/   \/_/ \/_/      \/_____/   \/_____/   \/_/\/_/   \/_____/   \/_/ /_/
"""


def print_banner():
    version = os.getenv("VERSION", "0.0.0")
    env = os.getenv("ENV", "local")
    slogan = os.getenv("CLX_SLOGAN", "Aggregating the world's liquidity flow.")
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(ASCII_LOGO)
    print(f" Environment : {env}")
    print(f" Version     : {version}")
    print(f" Start Time  : {start_time}")
    print(f" Slogan      : {slogan}")
    print("-" * 100)


if __name__ == "__main__":
    print_banner()
