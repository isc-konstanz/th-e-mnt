# -*- coding: utf-8 -*-
"""
    th-e-mnt.backup
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import List

import os
import pytz as tz
import pandas as pd
import logging

from corsys import System, Configurable, Configurations
from corsys.io import Database, DatabaseUnavailableException
from corsys.tools import floor_date, slice_range, to_timedelta
from scisys.validate import find_nan
from scisys.process import homogenize

logger = logging.getLogger(__name__)


# noinspection PyShadowBuiltins
class Rotation(Configurable):

    SECTION = 'Rotation'

    # noinspection PyTypeChecker
    @classmethod
    def read(cls,
             system: System,
             configs: Configurations,
             conf_file: str = 'rotate.cfg',
             section: str = SECTION) -> Rotation:
        if not configs.has_section(section) and not os.path.exists(os.path.join(configs.dirs.conf, conf_file)):
            raise RotationUnavailableException(f"System \"{system.name}\" rotation is not configured")

        config_args = {
            'require': False
        }
        if configs.has_section(section):
            config_args.update(configs.items(section))
        try:
            return cls(Configurations.from_configs(configs, conf_file, **config_args), system.database)

        except DatabaseUnavailableException:
            raise RotationUnavailableException(f"System \"{system.name}\" rotation has no database configured")

    def __init__(self, configs: Configurations, database: Database, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self.database = database

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self.rotate = configs.get(Configurations.GENERAL, 'rotate')
        self.resolution = configs.getfloat(Configurations.GENERAL, 'resolution')

        # noinspection PyUnresolvedReferences
        def parse_table_names(config_key: str) -> List[str]:
            if not configs.has_option(Configurations.GENERAL, config_key):
                return []
            tables = configs.get(Configurations.GENERAL, config_key)
            if tables == 'all':
                return self.database.tables.values()
            return tables.splitlines()

        self._resample_by_max = parse_table_names('resample_by_max')
        self._resample_by_last = parse_table_names('resample_by_last')
        self._resample_by_mean = parse_table_names('resample_by_mean')
        self._tables_to_trim = parse_table_names('tables_to_trim')

    # noinspection PyUnresolvedReferences
    def __call__(self, full: bool = False) -> None:
        rotate = floor_date(pd.Timestamp.now(tz=tz.UTC) - to_timedelta(self.rotate), freq='D')
        rotate_min = pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

        for table in self.database.tables.values():
            rotate_start, _ = table.get_first()
            rotate_end, _ = table.get_last()

            if any(t is None for t in [rotate_start, rotate_end]):
                logger.debug(f"Skip rotating values of empty table {table.name} without any new values found")
                continue

            rotate_end = min(rotate_end, rotate)
            if rotate_end - rotate_start < rotate_min:
                logger.debug(f"Skip rotating values of table {table.name} with short rotation period "
                             f"from {rotate_start.strftime('%d.%m.%Y (%H:%M:%S)')} "
                             f"to {rotate_end.strftime('%d.%m.%Y (%H:%M:%S)')}")
                continue

            if table.name in self._tables_to_trim:
                table.delete(rotate_start, rotate_end)

                logger.debug(f"Trimmed table {table.name} "
                             f"from {rotate_start.strftime('%d.%m.%Y (%H:%M:%S)')} "
                             f"to {rotate_end.strftime('%d.%m.%Y (%H:%M:%S)')}")
                continue

            rotation_days = slice_range(rotate_start, rotate_end, freq='D')
            rotation_days.reverse()

            for start, end in rotation_days:
                if end - start < rotate_min:
                    continue

                logger.debug(f"Start rotating values of table {table.name} "
                             f"from {start.strftime('%d.%m.%Y (%H:%M:%S)')} "
                             f"to {end.strftime('%d.%m.%Y (%H:%M:%S)')}")

                data = table.get(start=start, end=end)
                if not data.empty:
                    # errors = validate(data)
                    # if not errors.empty:
                    #     # TODO: Write detected errors into yaml file or smth similar
                    #     print(f"Unable to rotate values of table {table.name} "
                    #           f"from {start.strftime('%d.%m.%Y (%H:%M:%S)')} "
                    #           f"to {end.strftime('%d.%m.%Y (%H:%M:%S)')} "
                    #           f"with {len(errors)} unprocessed errors")
                    #     print(errors)
                    #     continue

                    homogeneous_gap = pd.Timedelta(seconds=self.resolution * 60 - 1)
                    homogeneous_data = homogenize(data, self.resolution * 60)

                    # Drop rows with outages longer than the resolution
                    gaps = find_nan(data, self.resolution * 60)
                    for _, gap in gaps.iterrows():
                        gap_data = homogeneous_data[(homogeneous_data.index > gap['start'] + homogeneous_gap) &
                                                    (homogeneous_data.index < gap['end'])]
                        homogeneous_data = homogeneous_data.drop(gap_data.index)

                    if len(homogeneous_data)-len(data) <= 0 and len(homogeneous_data.index.difference(data.index)) > 1:
                        logger.info(f"Rotating {len(data)} to {len(homogeneous_data)} homogenized values "
                                    f"of table {table.name} "
                                    f"from {start.strftime('%d.%m.%Y (%H:%M:%S)')} "
                                    f"to {end.strftime('%d.%m.%Y (%H:%M:%S)')}")

                        table.delete(start, end)
                        table.write(homogeneous_data)

                        if not full:
                            break


class RotationException(Exception):
    """
    Raise if an error occurred accessing the backup.

    """
    pass


class RotationUnavailableException(RotationException):
    """
    Raise if a configured backup can not be found.
    """
    pass
