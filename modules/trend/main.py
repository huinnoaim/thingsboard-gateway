from __future__ import annotations
import builtins
import sys
import argparse
import os
import logging
import logging.config
from typing import NamedTuple, Union
from pathlib import Path
import datetime as dt

import uvicorn
import yaml
from fastapi import FastAPI, Depends
from dotenv import load_dotenv
from sqlalchemy import text
import redis

import database
from database import get_session, get_engine, AsyncEngine, AsyncSession

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
app = FastAPI()


class Envs(NamedTuple):
    REDIS_URL: str
    REDIS_PORT: int
    REDIS_PASSWORD: str
    POSTGRESQL_URL: str
    POSTGRESQL_PORT: int
    POSTGRESQL_USERNAME: str
    POSTGRESQL_PASSWORD: str
    POSTGRESQL_DATABASE: str

    @staticmethod
    def getenv(fpath: Path | None = None) -> Envs:
        load_dotenv(fpath)
        kwargs = {}
        for field in Envs.__dict__["_fields"]:
            casting = getattr(builtins, Envs.get_typing(field), None)
            value = os.getenv(field)
            try:
                kwargs[field] = casting(value) if casting else value
            except ValueError:
                kwargs[field] = None
        return Envs(**kwargs)

    @classmethod
    def get_typing(cls, field: str) -> str:
        return cls.__annotations__[field].__forward_arg__


def setup_logger(fpath: str):
    try:
        with open(fpath, "r") as f:
            full_cfg = yaml.safe_load(f.read())
            cfg = full_cfg["log"]
        logging.config.dictConfig(cfg)
        logger.info(f"Logging config is loaded from `{fpath}`")

        child_loggers = logging.Logger.manager.loggerDict
        for child_logger in child_loggers.values():
            logger.info(f"{str(child_logger)} is loaded")
    except:
        logger.info("Use Default logging config")


def update_cfgfile(envs: Envs, cfg_fpath: Path):
    """It updates the config yaml file by using envs."""
    # fmt: off
    with open(cfg_fpath, "r") as f:
        yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
        yaml_data["postgresql"]["url"] = envs.POSTGRESQL_URL
        yaml_data["postgresql"]["port"] = envs.POSTGRESQL_PORT
        yaml_data["postgresql"]["username"] = envs.POSTGRESQL_USERNAME
        yaml_data["postgresql"]["password"] = envs.POSTGRESQL_PASSWORD
        yaml_data["postgresql"]["database"] = envs.POSTGRESQL_DATABASE

        yaml_data["redis"]["url"] = envs.REDIS_URL
        yaml_data["redis"]["port"] = envs.REDIS_PORT
        yaml_data["redis"]["password"] = envs.REDIS_PASSWORD

    # fmt: on
    with open(cfg_fpath, "w") as f:
        yaml.safe_dump(yaml_data, f)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ECG Converters")
    # fmt: off
    parser.add_argument(
        "--env-fpath",
        "-e",
        default=".env",
        help="envfile file path",
        type=Path
    )
    parser.add_argument(
        "--cfg-fpath",
        "-c",
        default="./config.yaml",
        help="config file path",
        type=os.path.abspath,
    )
    # fmt: on
    return parser.parse_args()


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/trend/")
# async def read_trend(
#     device_ids: list[str],
#     start_ts: dt.datetime,
#     range: int,
#     engine: AsyncEngine = Depends(get_engine),
# ):
async def read_trend(engine: AsyncEngine = Depends(get_engine)):
    start_ts = "2023-07-20T06:30:51.171Z"
    device_ids = """('f38856b0-253f-11ee-8044-891158445ad2','dbc7e150-2538-11ee-8044-891158445ad2','42b11460-1fbf-11ee-8044-891158445ad2')"""
    sql = f"""SELECT ts, val->>0 as val_text, 'type'
                FROM public.vital
                WHERE 'type' <> 'ecg'
                AND device_id IN {device_ids}
                AND ts > '{start_ts}'::timestamptz
                AND ts > CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' - INTERVAL '144000 minutes'
                ORDER BY type ASC, ts DESC;"""
    async with engine.connect() as conn:
        records = await conn.execute(text(sql))
    return [row._asdict() for row in records]


def main(args: argparse.Namespace):
    setup_logger(args.cfg_fpath)
    envs = Envs.getenv(args.env_fpath)
    if envs:
        update_cfgfile(envs, args.cfg_fpath)
    logger.info(envs)
    logger.info(args.cfg_fpath)

    db_cfg = database.DBConfig(
        drivername="postgresql+asyncpg",
        username=envs.POSTGRESQL_USERNAME,
        password=envs.POSTGRESQL_PASSWORD,
        host=envs.POSTGRESQL_URL,
        port=envs.POSTGRESQL_PORT,
        database=envs.POSTGRESQL_DATABASE,
    )

    database.on_startup(db_config=db_cfg)
    uvicorn.run(app, host="127.0.0.1", port=8000)
    database.on_shutdown()


if __name__ == "__main__":
    args = get_args()
    main(args)
