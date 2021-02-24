import logging
import pathlib
from typing import List
from contextlib import asynccontextmanager
from pprint import pformat
import anyio

from ..scanner import BaseBleakScanner, AdvertisementData
from ..device import BLEDevice
from . import defs
from .utils import validate_mac_address, de_variate

from asyncdbus import MessageBus, BusType, Message
from asyncdbus.signature import Variant,Str

logger = logging.getLogger(__name__)


def _filter_on_adapter(objs, pattern="hci0"):
    for path, interfaces in objs[0].items():
        adapter = interfaces.get("org.bluez.Adapter1")
        if adapter is None:
            continue

        if not pattern or pattern == adapter["Address"] or path.endswith(pattern):
            return path, interfaces

    raise Exception("Bluetooth adapter not found")


def _filter_on_device(objs):
    for path, interfaces in objs[0].items():
        device = interfaces.get("org.bluez.Device1")
        if device is None:
            continue

        yield path, de_variate(device)


def _device_info(path, props):
    try:
        name = props.get("Alias", "Unknown")
        address = props.get("Address", None)
        if address is None:
            try:
                address = path[-17:].replace("_", ":")
                if not validate_mac_address(address):
                    address = None
            except Exception:
                address = None
        rssi = props.get("RSSI", "?")
        return name, address, rssi, path
    except Exception:
        return None, None, None, None


class BleakScanner(BaseBleakScanner):
    """The native Linux Bleak BLE Scanner.

    For possible values for `filters`, see the parameters to the
    ``SetDiscoveryFilter`` method in the `BlueZ docs
    <https://git.kernel.org/pub/scm/bluetooth/bluez.git/tree/doc/adapter-api.txt?h=5.48&id=0d1e3b9c5754022c779da129025d493a198d49cf>`_

    Keyword Args:
        adapter (str): Bluetooth adapter to use for discovery.
        filters (dict): A dict of filters to be applied on discovery.

    """
    _ctx_ = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # kwarg "device" is for backwards compatibility
        self._adapter = kwargs.get("adapter", kwargs.get("device", "hci0"))
        self._bus = None

        self._cached_devices = {}
        self._devices = {}
        self._rules = list()

        # Discovery filters
        self._filters = kwargs.get("filters", {})
        if "Transport" not in self._filters:
            self._filters["Transport"] = Variant(Str, "le")

        self._adapter_path = None
        self._interface = None

    async def start(self):
        raise RuntimeError("You need to use an async context manager")

    async def stop(self):
        raise RuntimeError("You need to use an async context manager")

    async def __aenter__(self):
        if self._ctx_ is not None:
            raise RuntimeError("You can't nest contexts")
        self._ctx_ = self._ctx()
        return await self._ctx_.__aenter__()

    async def __aexit__(self, *tb):
        ctx, self._ctx_ = self._ctx_, None
        return await ctx.__aexit__(*tb)  # pylint:disable=no-member  # YES IT HAS

    @asynccontextmanager
    async def _ctx(self):
        async with MessageBus(bus_type=BusType.SYSTEM).connect() as self._bus:
            await self._start()
            try:
                yield self
            finally:
                await self._stop()

    async def _start(self):
        # Add signal listener
        self._bus.add_message_handler(self.parse_msg)

        # Find the HCI device to use for scanning and get cached device properties
        objects = await self._callRemote(
            "/",
            "GetManagedObjects",
            interface=defs.OBJECT_MANAGER_INTERFACE,
            destination=defs.BLUEZ_SERVICE,
        )
        self._adapter_path, self._interface = _filter_on_adapter(objects, self._adapter)
        self._cached_devices = dict(_filter_on_device(objects))
        for path, props in self._cached_devices.items():
            if True: # 'RSSI' in props:
                self._devices[path] = props
                self._send_callback(path, props, None)

        # Apply the filters
        await self._callRemote(
            self._adapter_path,
            "SetDiscoveryFilter",
            interface="org.bluez.Adapter1",
            destination="org.bluez",
            signature="a{sv}",
            body=[self._filters],
        )

        # Add signal listeners
        await self._callRemote(
            '/org/freedesktop/DBus',
            'AddMatch',
            interface='org.freedesktop.DBus',
            destination='org.freedesktop.DBus',
            body=['interface=org.freedesktop.DBus.ObjectManager,member=InterfacesAdded'],
            signature='s',
        )

        await self._callRemote(
            '/org/freedesktop/DBus',
            'AddMatch',
            interface='org.freedesktop.DBus',
            destination='org.freedesktop.DBus',
            body=['interface=org.freedesktop.DBus.ObjectManager,member=InterfacesRemoved'],
            signature='s',
        )

        # Start scanning
        await self._callRemote(
            self._adapter_path,
            "StartDiscovery",
            interface="org.bluez.Adapter1",
            destination="org.bluez",
        )


    async def _stop(self):
        await self._callRemote(
            self._adapter_path,
            "StopDiscovery",
            interface="org.bluez.Adapter1",
            destination="org.bluez",
        )

        for rule in self._rules:
            await self._bus.delMatch(rule)
        self._rules.clear()

        self._bus = None

    async def set_scanning_filter(self, **kwargs):
        """Sets OS level scanning filters for the BleakScanner.

        For possible values for `filters`, see the parameters to the
        ``SetDiscoveryFilter`` method in the `BlueZ docs
        <https://git.kernel.org/pub/scm/bluetooth/bluez.git/tree/doc/adapter-api.txt?h=5.48&id=0d1e3b9c5754022c779da129025d493a198d49cf>`_

        Keyword Args:
            filters (dict): A dict of filters to be applied on discovery.

        """
        self._filters = kwargs.get("filters", {})
        if "Transport" not in self._filters:
            self._filters["Transport"] = Variant(Str,"le")

    async def get_discovered_devices(self) -> List[BLEDevice]:
        # Reduce output.
        discovered_devices = []
        for path, props in self._devices.items():
            if not props:
                logger.debug(
                    "Disregarding %s since no properties could be obtained." % path
                )
                continue
            name, address, _, path = _device_info(path, props)
            if address is None:
                continue
            uuids = props.get("UUIDs", [])
            manufacturer_data = props.get("ManufacturerData", {})
            discovered_devices.append(
                BLEDevice(
                    address,
                    name,
                    {"path": path, "props": props},
                    props.get("RSSI", 0),
                    uuids=uuids,
                    manufacturer_data=manufacturer_data,
                )
            )
        return discovered_devices

    # Helper methods

    async def _callRemote(self, path, method, *, returnSignature=None, **kwargs):
        msg = Message(path=path, member=method, **kwargs)
        res = await self._bus.call(msg)
        if returnSignature is not None and res.signature != returnSignature:
            raise RuntimeError("Res %r: want %s" % (res, returnSignature))
        return res.body

    def parse_msg(self, message):
        if message.member == "InterfacesAdded":
            msg_path = message.body[0]
            device_interface = message.body[1].get(defs.DEVICE_INTERFACE, {})
            if msg_path == "/":
                return
            self._devices[msg_path] = props = (
                {**self._devices[msg_path], **device_interface}
                if msg_path in self._devices
                else device_interface
            )
            if 'Address' in props:
                self._send_callback(msg_path, props, message)

        elif message.member == "PropertiesChanged":
            iface, changed, invalidated = message.body
            if iface != defs.DEVICE_INTERFACE:
                return

            msg_path = message.path
            # the PropertiesChanged signal only sends changed properties, so we
            # need to get remaining properties from cached_devices. However, we
            # don't want to add all cached_devices to the devices dict since
            # they may not actually be nearby or powered on.
            if msg_path not in self._devices and msg_path in self._cached_devices:
                self._devices[msg_path] = self._cached_devices[msg_path]
            self._devices[msg_path] = props = (
                {**self._devices[msg_path], **changed}
                if msg_path in self._devices
                else changed
            )
            self._send_callback(msg_path, props, message)

        elif (
            message.member == "InterfacesRemoved"
            and message.body[1][0] == defs.BATTERY_INTERFACE
        ):
            logger.debug(
                "{0}, {1} ({2}): {3}".format(
                    message.member, message.interface, message.path, pformat(message.body)
                )
            )
            return
        else:
            msg_path = message.path
            logger.debug(
                "{0}, {1} ({2}): {3}".format(
                    message.member, message.interface, message.path, pformat(message.body)
                )
            )

        logger.debug(
            "{0}, {1} ({2} dBm), Object Path: {3}".format(
                *_device_info(msg_path, self._devices.get(msg_path))
            )
        )

    def _send_callback(self, path, props, message):
        if self._callback is None:
            return

        props = de_variate(props)
        dev_props = {'path': path, 'props': props}

        # Get all the information wanted to pack in the advertisement data
        _local_name = props.get("Name")
        _manufacturer_data = {
            k: bytes(v) for k, v in props.get("ManufacturerData", {}).items()
        }
        _service_data = {
            k: bytes(v) for k, v in props.get("ServiceData", {}).items()
        }
        _service_uuids = props.get("UUIDs", [])

        # Pack the advertisement data
        advertisement_data = AdvertisementData(
            local_name=_local_name,
            manufacturer_data=_manufacturer_data,
            service_data=_service_data,
            service_uuids=_service_uuids,
            platform_data=(props, message),
        )

        try:
            device = BLEDevice(
                props["Address"], props["Alias"], dev_props, props.get("RSSI", 0)
            )
        except KeyError:
            pass
        else:
            self._callback(device, advertisement_data)


    @classmethod
    async def find_device_by_address( 
        cls, device_identifier: str, timeout: float = 10.0, **kwargs   
    ) -> BLEDevice:
        """A convenience method for obtaining a ``BLEDevice`` object specified by Bluetooth address.

        Args:
            device_identifier (str): The Bluetooth/UUID address of the Bluetooth peripheral sought.
            timeout (float): Optional timeout to wait for detection of specified peripheral before giving up. Defaults to 10.0 seconds.
        
        Keyword Args:
            adapter (str): Bluetooth adapter to use for discovery.
        
        Returns:
            The ``BLEDevice`` sought or ``None`` if not detected.
        
        """
        device_identifier = device_identifier.lower()
        stop_scanning_event = anyio.create_event()
            
        def stop_if_detected(d: BLEDevice, ad: AdvertisementData):
            if d.address.lower() == device_identifier:
                stop_scanning_event.set()
            
        async with cls(detection_callback=stop_if_detected, **kwargs) as scanner:
            with anyio.move_on_after(timeout):
                await stop_scanning_event.wait()
                return next(
                    d
                    for d in await scanner.get_discovered_devices()
                    if d.address.lower() == device_identifier
                )
            return None

    @classmethod
    async def discover(cls, timeout=5.0, **kwargs) -> List[BLEDevice]:
        """Scan continuously for ``timeout`` seconds and return discovered devices.

        Args:
            timeout: Time to scan for.

        Keyword Args:
            **kwargs: Implementations might offer additional keyword arguments sent to the constructor of the
                      BleakScanner class.

        Returns:

        """
        async with cls(**kwargs) as scanner:
            await anyio.sleep(timeout)
            return await scanner.get_discovered_devices()
