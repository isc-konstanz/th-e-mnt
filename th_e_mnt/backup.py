# -*- coding: utf-8 -*-
"""
    th-e-mnt.backup
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import os
import re
import pytz as tz
import pandas as pd
import logging

from subprocess import Popen
from dateutil.relativedelta import relativedelta
from corsys import System, Configurable, Configurations
from corsys.io import Database, DatabaseUnavailableException
from corsys.io.sql import SqlDatabase
from corsys.tools import floor_date, ceil_date, slice_range

logger = logging.getLogger(__name__)


class Backup(Configurable):

    SECTION = 'Backup'

    # noinspection PyTypeChecker
    @classmethod
    def read(cls,
             system: System,
             configs: Configurations,
             conf_file: str = 'backup.cfg',
             section: str = SECTION) -> Backup:
        if not configs.has_section(section) and not os.path.exists(os.path.join(configs.dirs.conf, conf_file)):
            raise BackupException(f"System \"{system.name}\" backup is not configured")

        config_args = {
            'require': False
        }
        if configs.has_section(section):
            config_args.update(configs.items(section))

        sections = {s: configs.items(s) for s in configs.sections() if s.startswith(cls.SECTION)}
        configs = Configurations.from_configs(configs, conf_file, **config_args)
        for section, config_params in sections.items():
            config_section = section[len(cls.SECTION)+1:]
            configs.add_section(config_section)
            for key, val in config_params:
                configs.set(config_section, key, val)
        try:
            return cls(configs, system.database)

        except DatabaseUnavailableException:
            raise BackupUnavailableException(f"System \"{system.name}\" backup has no database configured")

    def __init__(self, configs: Configurations, database: Database, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self.database = database

    def __call__(self) -> None:
        if not self.configs.has_section(Database.SECTION):
            raise BackupException("Unable to backup to unconfigured remote backup location")

        if not self.configs.has_option(Database.SECTION, 'timezone'):
            self.configs.set(Database.SECTION, 'timezone', self.database.timezone.zone)

        with Database.from_configs(self.configs) as database:
            self._sync_new_data(self.database, database)

    def synchronize(self):
        sync_section = 'Synchronize'
        if not self.configs.has_section(sync_section):
            raise BackupException("Unable to synchronize from unconfigured backup")

        sync_params = dict(self.configs.items(sync_section))
        sync_method = re.sub('[^A-Za-z0-9]+', '', sync_params.pop('method', '')).lower()
        if sync_method in ['sqldump', 'mysqldump']:
            self._sync_sql_dump(**sync_params)
        else:
            raise BackupException(f"Unknown synchronization method: {sync_method}")

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _sync_sql_dump(self, database, dump):
        if not isinstance(self.database, SqlDatabase):
            raise BackupException(f"Invalid database type : {type(self.database)}")

        mysql = "mysql " \
                f"--host={self.database.host} --port={self.database.port} " \
                f"--user={self.database.user} --password='{self.database.password}'"

        Popen(f"{mysql} -e 'CREATE DATABASE IF NOT EXISTS {database} DEFAULT CHARACTER SET utf8;'", shell=True).wait()
        Popen(f"{mysql} {database} < {dump}", shell=True).wait()

        with SqlDatabase(host=self.database.host, port=self.database.port,
                         user=self.database.user, password=self.database.password,
                         database=database, timezone=self.database.timezone) as sync_database:

            self._sync_new_data(sync_database, self.database)

        Popen(f"{mysql} -e 'DROP DATABASE IF EXISTS {database};'", shell=True).wait()

    # noinspection PyMethodMayBeStatic, PyUnresolvedReferences
    def _sync_new_data(self, source: Database, target: Database) -> None:
        for source_table in source.tables.values():
            if source_table.name not in target.tables:
                table_type = source_table.get_type()
                target.create(source_table.name, data_type=table_type)
            target_table = target.tables[source_table.name]

            sync_end, _ = source_table.get_last()
            if sync_end is not None:
                sync_end = min(sync_end, pd.Timestamp.now(tz=tz.UTC))

            sync_start, _ = target_table.get_last()
            if sync_start is None:
                sync_start, _ = source_table.get_first()
            else:
                sync_start += pd.Timedelta(seconds=1)

            if (not any(t is None for t in [sync_start, sync_end]) and sync_start >= sync_end) or \
                    all(t is None for t in [sync_start, sync_end]):
                logger.debug(f"Skip copying values of table {source_table.name} without any new values found")
                continue

            for start, end in slice_range(sync_start, sync_end, freq='M'):
                logger.debug(f"Start copying values of table {source_table.name} "
                             f"from {start.strftime('%d.%m.%Y (%H:%M:%S)')} "
                             f"to {end.strftime('%d.%m.%Y (%H:%M:%S)')}")

                data = source_table.get(start=start, end=end)
                if not data.empty:
                    logger.info(f"Copying {len(data)} values of table {source_table.name} "
                                f"from {start.strftime('%d.%m.%Y (%H:%M:%S)')} "
                                f"to {end.strftime('%d.%m.%Y (%H:%M:%S)')}")
                    target_table.write(data)


class BackupException(Exception):
    """
    Raise if an error occurred accessing the backup.

    """
    pass


class BackupUnavailableException(BackupException):
    """
    Raise if a configured backup can not be found.
    """
    pass
