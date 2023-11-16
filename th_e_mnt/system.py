# -*- coding: utf-8 -*-
"""
    th-e-mnt.system
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import logging
import corsys as core

from corsys import Configurations
from .backup import Backup, BackupUnavailableException
from .rotate import Rotation, RotationUnavailableException

logger = logging.getLogger(__name__)


# noinspection PyAbstractClass
class System(core.System):

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        try:
            self._backup = self.__backup__(self.configs)

        except BackupUnavailableException:
            self._backup = None

        super().__configure__(configs)
        try:
            self._rotate = self.__rotate__(self.configs)

        except RotationUnavailableException:
            self._rotate = None

    # noinspection PyUnusedLocal
    def __backup__(self, configs: Configurations) -> Backup:
        return Backup.read(self, configs)

    # noinspection PyUnusedLocal
    def __rotate__(self, configs: Configurations) -> Rotation:
        return Rotation.read(self, configs)

    @property
    def backup(self) -> Backup:
        if self._backup is None:
            raise BackupUnavailableException(f"System \"{self.name}\" has no backup configured")
        if not self._backup.enabled:
            raise BackupUnavailableException(f"System \"{self.name}\" backup is disabled")
        return self._backup

    @property
    def rotate(self) -> Rotation:
        if self._rotate is None:
            raise RotationUnavailableException(f"System \"{self.name}\" has no rotation configured")
        if not self._rotate.enabled:
            raise RotationUnavailableException(f"System \"{self.name}\" rotation is disabled")
        return self._rotate
