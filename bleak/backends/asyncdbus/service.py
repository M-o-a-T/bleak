from typing import List

from ..service import BleakGATTService as baseGATTService
from .characteristic import BleakGATTCharacteristic


class BleakGATTService(BaseGATTService):
    """GATT Service implementation for the BlueZ DBus backend"""

    def __init__(self, obj, path):
        super().__init__(obj)
        self.__characteristics = []
        self.__path = path

    @property
    def uuid(self) -> str:
        """The UUID to this service"""
        return self.obj["UUID"]

    @property
    def characteristics(self) -> List[BleakGATTCharacteristic]:
        """List of characteristics for this service"""
        return self.__characteristics

    def add_characteristic(self, characteristic: BleakGATTCharacteristic):
        """Add a :py:class:`~BleakGATTCharacteristic` to the service.

        Should not be used by end user, but rather by `bleak` itself.
        """
        self.__characteristics.append(characteristic)

    @property
    def path(self):
        """The DBus path. Mostly needed by `bleak`, not by end user"""
        return self.__path
