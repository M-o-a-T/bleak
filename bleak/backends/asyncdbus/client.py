# -*- coding: utf-8 -*-
"""
BLE Client for BlueZ on Linux
"""
import logging
import os
import re
import subprocess
import uuid
import warnings
import inspect
import anyio
from functools import wraps
from typing import Callable, Union
from contextlib import asynccontextmanager
from uuid import UUID


from asyncdbus import MessageBus, BusType, Message, MessageType

from ..device import BLEDevice
from ..service import BleakGATTServiceCollection
from ...exc import BleakError
from ..client import BaseBleakClient
from . import defs, utils
from .scanner import BleakScanner
from .utils import get_managed_objects, assert_reply, de_variate
from .service import BleakGATTService
from .characteristic import BleakGATTCharacteristic
from .descriptor import BleakGATTDescriptor
from .signals import MatchRules, add_match, remove_match

## from txdbus.client import connect as txdbus_connect
## from txdbus.error import RemoteError


logger = logging.getLogger(__name__)


class BleakClient(BaseBleakClient):
    """A native Linux Bleak Client

    Implemented by using the `BlueZ DBUS API <https://docs.ubuntu.com/core/en/stacks/bluetooth/bluez/docs/reference/dbus-api>`_.

    Args:
        address_or_ble_device (`BLEDevice` or str): The Bluetooth address of the BLE peripheral to connect to or the `BLEDevice` object representing it.

    Keyword Args:
        disconnected_callback (callable): Callback that will be scheduled in the
            event loop when the client is disconnected. The callable must take one
            argument, which will be this client object.
        adapter (str): Bluetooth adapter to use for discovery.
    """

    _ctx_ = None

    def __init__(self, address_or_ble_device: Union[BLEDevice, str], **kwargs):
        super().__init__(address_or_ble_device, **kwargs)
        # kwarg "device" is for backwards compatibility
        self._adapter = kwargs.get("adapter", kwargs.get("device", "hci0"))

        # Backend specific, TXDBus objects and data
        if isinstance(address_or_ble_device, BLEDevice):
            self._device_path = address_or_ble_device.details["path"]
            self._device_info = address_or_ble_device.details.get("props")
        else:
            self._device_path = None
            self._device_info = None

        # D-Bus message bus
        self._bus: MessageBus = None

        # match rules we are subscribed to that need to be removed on disconnect
        self._rules: List[MatchRules] = []

        # D-Bus properties for the device
        self._properties: Dict[str, Any] = {}

        # list of characteristic handles that have notifications enabled
        self._subscriptions: List[int] = []

        # This maps DBus paths of GATT Characteristics to their BLE handles.
        self._char_path_to_handle = {}

        # provides synchronization between get_services() and PropertiesChanged signal
        self._services_resolved_event: anyio.abc.Event = None

        # indicates disconnect request in progress when not None
        self._disconnecting_event: anyio.abc.Event = None

        # used to ensure device gets disconnected if event loop crashes
        self._disconnect_monitor_event: anyio.abc.Event = None


        # We need to know BlueZ version since battery level characteristic
        # are stored in a separate DBus interface in the BlueZ >= 5.48.
        p = subprocess.Popen(["bluetoothctl", "--version"], stdout=subprocess.PIPE)
        out, _ = p.communicate()
        s = re.search(b"(\\d+).(\\d+)", out.strip(b"'"))
        self._bluez_version = tuple(map(int, s.groups()))

    # Connectivity methods.

    async def connect(self) -> bool:
        raise NotImplementedError("You must use a context manager.")

    async def __aenter__(self):
        if self._ctx_ is not None:
            raise RuntimeError("A ScopeSet can only be used once")
        self._ctx_ = self._ctx()
        return await self._ctx_.__aenter__()

    async def __aexit__(self, *tb):
        ctx, self._ctx_ = self._ctx_, None
        return await ctx.__aexit__(*tb)  # pylint:disable=no-member  # YES IT HAS

    @asynccontextmanager
    async def _ctx(self) -> None:
        """Connect to the specified GATT server.

        Returns:
            Boolean representing connection status.

        """
        # A Discover must have been run before connecting to any devices.
        # Find the desired device before trying to connect.
        if self._device_path is None:
            device = await BleakScanner.find_device_by_address(
                self.address, adapter=self._adapter
            )

            if device:
                self._device_info = device.details.get("props")
                self._device_path = device.details["path"]
            else:
                raise BleakError(
                    "Device with address {0} was not found.".format(self.address)
                )

        # Connect to system bus
        async with MessageBus(bus_type=BusType.SYSTEM).connect() as self._bus:

            self._bus.add_message_handler(self._parse_msg)

            rules = MatchRules(
                interface=defs.OBJECT_MANAGER_INTERFACE,
                member="InterfacesAdded",
                arg0path=f"{self._device_path}/",
            )
            reply = await add_match(self._bus, rules)
            assert_reply(reply)
            self._rules.append(rules)

            rules = MatchRules(
                interface=defs.OBJECT_MANAGER_INTERFACE,
                member="InterfacesRemoved",
                arg0path=f"{self._device_path}/",
            )    
            reply = await add_match(self._bus, rules)
            assert_reply(reply)
            self._rules.append(rules)

            rules = MatchRules(
                interface=defs.PROPERTIES_INTERFACE,
                member="PropertiesChanged",
                path_namespace=self._device_path,
            )
            reply = await add_match(self._bus, rules)
            assert_reply(reply)
            self._rules.append(rules)

            # Find the HCI device to use for scanning and get cached device properties
            reply = await self._bus.call(
                Message(
                    destination=defs.BLUEZ_SERVICE,
                    path="/",
                    member="GetManagedObjects",
                    interface=defs.OBJECT_MANAGER_INTERFACE,
                )
            )
            assert_reply(reply)

            interfaces_and_props: Dict[str, Dict[str, Variant]] = reply.body[0]

            # The device may have been removed from BlueZ since the time we stopped scanning
            if self._device_path not in interfaces_and_props:
                # Sometimes devices can be removed from the BlueZ object manager
                # before we connect to them. In this case we try using the
                # org.bluez.Adapter1.ConnectDevice method instead. This method
                # requires that bluetoothd is run with the --experimental flag
                # and is available since BlueZ 5.49.
                logger.debug(
                    f"org.bluez.Device1 object not found, trying org.bluez.Adapter1.ConnectDevice ({self._device_path})"
                )
                reply = await self._bus.call(
                    Message(
                        destination=defs.BLUEZ_SERVICE,
                        interface=defs.ADAPTER_INTERFACE,
                        path=f"/org/bluez/{self._adapter}",
                        member="ConnectDevice",
                        signature="a{sv}",
                        body=[
                            {
                                "Address": Variant(
                                    "s", self._device_info["Address"]
                                ),
                                "AddressType": Variant(
                                    "s", self._device_info["AddressType"]
                                ),
                            }
                        ],
                    )
                )

                if (
                    reply.message_type == MessageType.ERROR
                    and reply.error_name == ErrorType.UNKNOWN_METHOD.value
                ):
                    logger.debug(
                        f"org.bluez.Adapter1.ConnectDevice not found ({self._device_path}), try enabling + bluetoothd --experimental"
                    )
                    raise BleakError(
                        "Device with address {0} could not be found. "
                        "Try increasing the timeout or moving the device closer.".format(
                            self.address
                        )
                    )

                assert_reply(reply)
            else:
                # required interface
                self._properties = de_variate(
                    interfaces_and_props[self._device_path][defs.DEVICE_INTERFACE]
                )

                # optional interfaces - services and characteristics may not
                # be populated yet
                for path, interfaces in interfaces_and_props.items():
                    if not path.startswith(self._device_path):
                        continue

                    if defs.GATT_SERVICE_INTERFACE in interfaces:
                        obj = de_variate(interfaces[defs.GATT_SERVICE_INTERFACE])
                        self.services.add_service(BleakGATTService(obj, path))

                    if defs.GATT_CHARACTERISTIC_INTERFACE in interfaces:
                        obj = de_variate(
                            interfaces[defs.GATT_CHARACTERISTIC_INTERFACE]
                        )
                        service = interfaces_and_props[obj["Service"]][
                            defs.GATT_SERVICE_INTERFACE
                        ]
                        uuid = service["UUID"].value
                        self.services.add_characteristic(
                            BleakGATTCharacteristic(obj, path, uuid)
                        )

                    if defs.GATT_DESCRIPTOR_INTERFACE in interfaces:
                        obj = de_variate(
                            interfaces[defs.GATT_DESCRIPTOR_INTERFACE]
                        )
                        characteristic = interfaces_and_props[obj["Characteristic"]][
                            defs.GATT_CHARACTERISTIC_INTERFACE
                        ]
                        uuid = characteristic["UUID"].value
                        handle = int(obj["Characteristic"][-4:], 16)
                        self.services.add_descriptor(
                            BleakGATTDescriptor(obj, path, uuid, handle)
                        )

            logger.debug(
                "Connecting to BLE device @ {0} with {1}".format(
                    self.address, self._adapter
                )
            )
            try:
                await self._callRemote(
                    self._device_path,
                    "Connect",
                    interface=defs.DEVICE_INTERFACE,
                    destination=defs.BLUEZ_SERVICE,
                )
            except BaseException:
                # calling Disconnect cancels any pending connect request
                with anyio.move_on_after(2, shield=True):
                    await self._call_disconnect()
                raise

            if self.is_connected:
                logger.debug("Connection successful.")
            else:
                raise BleakError(
                    "Connection to {0} was not successful!".format(self.address)
                )

            # Get all services. This means making the actual connection.
            await self.get_services()

            try:
                yield self
            finally:
                with anyio.move_on_after(2, shield=True):
                    await self._disconnect()

    async def _cleanup_notifications(self) -> None:
        """
        Remove all pending notifications of the client. This method is used to
        free the DBus matches that have been established.
        """
        for rule in self._rules:
            try:
                await remove_match(self._bus, rule)
            except Exception as e:
                logger.error(
                    "Could not remove rule {0} ({1}): {2}".format(rule_id, rule_name, e)
                )
        self._rules = {}

        for _uuid in list(self._subscriptions):
            try:
                await self.stop_notify(_uuid)
            except Exception as e:
                logger.error(
                    "Could not remove notifications on characteristic {0}: {1}".format(
                        _uuid, e
                    )
                )
        self._subscriptions = []

    async def _cleanup_all(self) -> None:
        """
        Free all the allocated resource in DBus and Twisted. Use this method to
        eventually cleanup all otherwise leaked resources.
        """
        self._char_path_to_handle.clear()
        await self._cleanup_notifications()

    async def cleanup_all_and_cb(self) -> None:
        await self._cleanup_all()
        if self._disconnected_callback is not None:
            res = self._disconnected_callback(self)
            if inspect.iscoroutine(res):
                await res

    async def disconnect(self) -> bool:
        raise NotImplementedError("You must use the context manager.")

    async def _disconnect(self) -> None:
        """Disconnect from the specified GATT server.

        Returns:
            Boolean representing if device is disconnected.

        """
        logger.debug("Disconnecting (%s)", self._device_path)
        if self._bus is None:
            # No connection exists. Either one hasn't been created or
            # we have already called disconnect and closed the txdbus
            # connection.
            return True

        # Remove all residual notifications.
        await self._cleanup_notifications()

        if self.is_connected:
            # Try to disconnect the actual device/peripheral
            await self._call_disconnect()

        # Reset all stored services.
        self.services = BleakGATTServiceCollection()
        self._services_resolved = False
        self._bus = None

    async def _call_disconnect(self):
        if self._disconnect_monitor_event is None:
            self._disconnect_monitor_event = evt = anyio.create_event()
        else:
            evt = self._disconnect_monitor_event

        try:
            await self._callRemote(
                self._device_path,
                "Disconnect",
                interface=defs.DEVICE_INTERFACE,
                destination=defs.BLUEZ_SERVICE,
            )
            await evt.wait()
        except Exception as e:
            logger.error("Attempt to disconnect device failed: {0}".format(e))

    async def pair(self, *args, **kwargs) -> bool:
        """Pair with the peripheral.

        You can use ConnectDevice method if you already know the MAC address of the device.
        Else you need to StartDiscovery, Trust, Pair and Connect in sequence.

        Returns:
            Boolean regarding success of pairing.

        """
        # See if it is already paired.
        is_paired = await self._callRemote(
            self._device_path,
            "Get",
            interface=defs.PROPERTIES_INTERFACE,
            destination=defs.BLUEZ_SERVICE,
            signature="ss",
            body=[defs.DEVICE_INTERFACE, "Paired"],
            returnSignature="v",
        )
        if is_paired:
            return is_paired

        # Set device as trusted.
        await self._callRemote(
            self._device_path,
            "Set",
            interface=defs.PROPERTIES_INTERFACE,
            destination=defs.BLUEZ_SERVICE,
            signature="ssv",
            body=[defs.DEVICE_INTERFACE, "Trusted", True],
            returnSignature="",
        )

        logger.debug(
            "Pairing to BLE device @ {0} with {1}".format(self.address, self._adapter)
        )
        try:
            await self._callRemote(
                self._device_path,
                "Pair",
                interface=defs.DEVICE_INTERFACE,
                destination=defs.BLUEZ_SERVICE,
            )
        except RemoteError:
            await self._cleanup_all()
            raise BleakError(
                "Device with address {0} could not be paired with.".format(self.address)
            )

        return await self._callRemote(
            self._device_path,
            "Get",
            interface=defs.PROPERTIES_INTERFACE,
            destination=defs.BLUEZ_SERVICE,
            signature="ss",
            body=[defs.DEVICE_INTERFACE, "Paired"],
            returnSignature="v",
        )

    async def unpair(self) -> bool:
        """Unpair with the peripheral.

        Returns:
            Boolean regarding success of unpairing.

        """
        warnings.warn(
            "Unpairing is seemingly unavailable in the BlueZ DBus API at the moment."
        )
        return False

    @property
    def is_connected(self) -> bool:
        """Check connection status between this client and the server.
                
        Returns:
            Boolean representing connection status.
                    
        """
        if not self._bus:
            return False
                        
        return self._properties.get("Connected", False)

    # GATT services methods

    async def get_services(self, **kwargs) -> BleakGATTServiceCollection:
        """Get all services registered for this GATT server.

        Returns:
           A :py:class:`bleak.backends.service.BleakGATTServiceCollection` with this device's services tree.

        """
        if self._services_resolved:
            return self.services

        self._services_resolved_event = anyio.create_event()
        sleep_loop_sec = 0.1
        total_slept_sec = 0
        services_resolved = False

        while total_slept_sec < 5.0:
            properties = await self._get_device_properties()

            services_resolved = properties.get("ServicesResolved", False)
            if services_resolved:
                break
            with anyio.move_on_after(sleep_loop_sec):
                await self._services_resolved_event.wait()
            total_slept_sec += sleep_loop_sec

        if not services_resolved:
            raise BleakError("Services discovery error")

        logger.debug("Get Services...")
        objs = await get_managed_objects(self._bus, self._device_path + "/service")

        # There is no guarantee that services are listed before characteristics
        # Managed Objects dict.
        # Need multiple iterations to construct the Service Collection

        _chars, _descs = [], []

        for object_path, interfaces in objs.items():
            logger.debug(utils.format_GATT_object(object_path, interfaces))
            if defs.GATT_SERVICE_INTERFACE in interfaces:
                service = interfaces.get(defs.GATT_SERVICE_INTERFACE)
                self.services.add_service(
                    BleakGATTService(service, object_path)
                )
            elif defs.GATT_CHARACTERISTIC_INTERFACE in interfaces:
                char = interfaces.get(defs.GATT_CHARACTERISTIC_INTERFACE)
                _chars.append([char, object_path])
            elif defs.GATT_DESCRIPTOR_INTERFACE in interfaces:
                desc = interfaces.get(defs.GATT_DESCRIPTOR_INTERFACE)
                _descs.append([desc, object_path])

        for char, object_path in _chars:
            _service = list(filter(lambda x: x.path == char["Service"], self.services))
            self.services.add_characteristic(
                BleakGATTCharacteristic(char, object_path, _service[0].uuid)
            )

            # D-Bus object path contains handle as last 4 characters of 'charYYYY'
            self._char_path_to_handle[object_path] = int(object_path[-4:], 16)

        for desc, object_path in _descs:
            _characteristic = list(
                filter(
                    lambda x: x.path == desc["Characteristic"],
                    self.services.characteristics.values(),
                )
            )
            self.services.add_descriptor(
                BleakGATTDescriptor(
                    desc,
                    object_path,
                    _characteristic[0].uuid,
                    int(_characteristic[0].handle),
                )
            )

        self._services_resolved = True
        return self.services

    # IO methods

    async def read_gatt_char(
        self,
        char_specifier: Union[BleakGATTCharacteristic, int, str, uuid.UUID],
        **kwargs
    ) -> bytearray:
        """Perform read operation on the specified GATT characteristic.

        Args:
            char_specifier (BleakGATTCharacteristic, int, str or UUID): The characteristic to read from,
                specified by either integer handle, UUID or directly by the
                BleakGATTCharacteristicobject representing it.

        Returns:
            (bytearray) The read data.

        """
        if not isinstance(char_specifier, BleakGATTCharacteristic):
            characteristic = self.services.get_characteristic(char_specifier)
        else:
            characteristic = char_specifier

        if not characteristic:
            # Special handling for BlueZ >= 5.48, where Battery Service (0000180f-0000-1000-8000-00805f9b34fb:)
            # has been moved to interface org.bluez.Battery1 instead of as a regular service.
            if str(char_specifier) == "00002a19-0000-1000-8000-00805f9b34fb" and (
                self._bluez_version[0] == 5 and self._bluez_version[1] >= 48
            ):
                props = await self._get_device_properties(
                    interface=defs.BATTERY_INTERFACE
                )
                # Simulate regular characteristics read to be consistent over all platforms.
                value = bytearray([props.get("Percentage", "")])
                logger.debug(
                    "Read Battery Level {0} | {1}: {2}".format(
                        char_specifier, self._device_path, value
                    )
                )
                return value
            if str(char_specifier) == "00002a00-0000-1000-8000-00805f9b34fb" and (
                self._bluez_version[0] == 5 and self._bluez_version[1] >= 48
            ):
                props = await self._get_device_properties(
                    interface=defs.DEVICE_INTERFACE
                )
                # Simulate regular characteristics read to be consistent over all platforms.
                value = bytearray(props.get("Alias", "").encode("ascii"))
                logger.debug(
                    "Read Device Name {0} | {1}: {2}".format(
                        char_specifier, self._device_path, value
                    )
                )
                return value

            raise BleakError(
                "Characteristic with UUID {0} could not be found!".format(
                    char_specifier
                )
            )

        value = bytearray(
            await self._callRemote(
                characteristic.path,
                "ReadValue",
                interface=defs.GATT_CHARACTERISTIC_INTERFACE,
                destination=defs.BLUEZ_SERVICE,
                signature="a{sv}",
                body=[{}],
                returnSignature="ay",
            )
        )

        logger.debug(
            "Read Characteristic {0} | {1}: {2}".format(
                characteristic.uuid, characteristic.path, value
            )
        )
        return value

    async def read_gatt_descriptor(self, handle: int, **kwargs) -> bytearray:
        """Perform read operation on the specified GATT descriptor.

        Args:
            handle (int): The handle of the descriptor to read from.

        Returns:
            (bytearray) The read data.

        """
        descriptor = self.services.get_descriptor(handle)
        if not descriptor:
            raise BleakError("Descriptor with handle {0} was not found!".format(handle))

        value = bytearray(
            await self._callRemote(
                descriptor.path,
                "ReadValue",
                interface=defs.GATT_DESCRIPTOR_INTERFACE,
                destination=defs.BLUEZ_SERVICE,
                signature="a{sv}",
                body=[{}],
                returnSignature="ay",
            )
        )

        logger.debug(
            "Read Descriptor {0} | {1}: {2}".format(handle, descriptor.path, value)
        )
        return value

    async def write_gatt_char(
        self,
        char_specifier: Union[BleakGATTCharacteristic, int, str, uuid.UUID],
        data: bytearray,
        response: bool = False,
    ) -> None:
        """Perform a write operation on the specified GATT characteristic.

        .. note::

            The version check below is for the "type" option to the
            "Characteristic.WriteValue" method that was added to `Bluez in 5.51
            <https://git.kernel.org/pub/scm/bluetooth/bluez.git/commit?id=fa9473bcc48417d69cc9ef81d41a72b18e34a55a>`_
            Before that commit, ``Characteristic.WriteValue`` was only "Write with
            response". ``Characteristic.AcquireWrite`` was `added in Bluez 5.46
            <https://git.kernel.org/pub/scm/bluetooth/bluez.git/commit/doc/gatt-api.txt?id=f59f3dedb2c79a75e51a3a0d27e2ae06fefc603e>`_
            which can be used to "Write without response", but for older versions
            of Bluez, it is not possible to "Write without response".

        Args:
            char_specifier (BleakGATTCharacteristic, int, str or UUID): The characteristic to write
                to, specified by either integer handle, UUID or directly by the
                BleakGATTCharacteristic object representing it.
            data (bytes or bytearray): The data to send.
            response (bool): If write-with-response operation should be done. Defaults to `False`.

        """
        if not isinstance(char_specifier, BleakGATTCharacteristic):
            characteristic = self.services.get_characteristic(char_specifier)
        else:
            characteristic = char_specifier

        if not characteristic:
            raise BleakError("Characteristic {0} was not found!".format(char_specifier))
        if (
            "write" not in characteristic.properties
            and "write-without-response" not in characteristic.properties
        ):
            raise BleakError(
                "Characteristic %s does not support write operations!"
                % str(characteristic.uuid)
            )
        if not response and "write-without-response" not in characteristic.properties:
            response = True
            # Force response here, since the device only supports that.
        if (
            response
            and "write" not in characteristic.properties
            and "write-without-response" in characteristic.properties
        ):
            response = False
            logger.warning(
                "Characteristic %s does not support Write with response. Trying without..."
                % str(characteristic.uuid)
            )

        # See docstring for details about this handling.
        if not response and self._bluez_version[0] == 5 and self._bluez_version[1] < 46:
            raise BleakError("Write without response requires at least BlueZ 5.46")
        if response or (self._bluez_version[0] == 5 and self._bluez_version[1] > 50):
            # TODO: Add OnValueUpdated handler for response=True?
            await self._callRemote(
                characteristic.path,
                "WriteValue",
                interface=defs.GATT_CHARACTERISTIC_INTERFACE,
                destination=defs.BLUEZ_SERVICE,
                signature="aya{sv}",
                body=[data, {"type": "request" if response else "command"}],
                returnSignature="",
            )
        else:
            # Older versions of BlueZ don't have the "type" option, so we have
            # to write the hard way. This isn't the most efficient way of doing
            # things, but it works.
            fd, _ = await self._callRemote(
                characteristic.path,
                "AcquireWrite",
                interface=defs.GATT_CHARACTERISTIC_INTERFACE,
                destination=defs.BLUEZ_SERVICE,
                signature="a{sv}",
                body=[{}],
                returnSignature="hq",
            )
            os.write(fd, data)
            os.close(fd)

        logger.debug(
            "Write Characteristic {0} | {1}: {2}".format(
                characteristic.uuid, characteristic.path, data
            )
        )

    async def write_gatt_descriptor(self, handle: int, data: bytearray) -> None:
        """Perform a write operation on the specified GATT descriptor.

        Args:
            handle (int): The handle of the descriptor to read from.
            data (bytes or bytearray): The data to send.

        """
        descriptor = self.services.get_descriptor(handle)
        if not descriptor:
            raise BleakError("Descriptor with handle {0} was not found!".format(handle))
        await self._callRemote(
            descriptor.path,
            "WriteValue",
            interface=defs.GATT_DESCRIPTOR_INTERFACE,
            destination=defs.BLUEZ_SERVICE,
            signature="aya{sv}",
            body=[data, {"type": "command"}],
            returnSignature="",
        )

        logger.debug(
            "Write Descriptor {0} | {1}: {2}".format(handle, descriptor.path, data)
        )

    async def start_notify(
        self,
        char_specifier: Union[BleakGATTCharacteristic, int, str, UUID],
        callback: Callable[[int, bytearray], None],
        **kwargs,
    ) -> None:
        """Activate notifications/indications on a characteristic.

        Callbacks must accept two inputs. The first will be a integer handle of the characteristic       + generating the
        data and the second will be a ``bytearray`` containing the data sent from the connected server.
                        
        .. code-block:: python
                         
            def callback(sender: int, data: bytearray):
                print(f"{sender}: {data}")
            client.start_notify(char_uuid, callback)
                         
        Args:
            char_specifier (BleakGATTCharacteristic, int, str or UUID): The characteristic to   + activate
                notifications/indications on a characteristic, specified by either integer handle,
                UUID or directly by the BleakGATTCharacteristicobject representing it.
            callback (function): The function to be called on notification.
        """
        if not isinstance(char_specifier, BleakGATTCharacteristic):
            characteristic = self.services.get_characteristic(char_specifier)
        else:
            characteristic = char_specifier

        if not characteristic:
            # Special handling for BlueZ >= 5.48, where Battery Service (0000180f-0000-1000-8000-00805f9b34fb:)
            # has been moved to interface org.bluez.Battery1 instead of as a regular service.
            # The org.bluez.Battery1 on the other hand does not provide a notification method, so here we cannot
            # provide this functionality...
            # See https://kernel.googlesource.com/pub/scm/bluetooth/bluez/+/refs/tags/5.48/doc/battery-api.txt
            if str(char_specifier) == "00002a19-0000-1000-8000-00805f9b34fb" and (
                self._hides_battery_characteristic
            ):
                raise BleakError(
                    "Notifications on Battery Level Char ({0}) is not "
                    "possible in BlueZ >= 5.48. Use regular read instead.".format(
                        char_specifier
                    )
                )
            raise BleakError(
                "Characteristic with UUID {0} could not be found!".format(
                    char_specifier
                )
            )

        self._notification_callbacks[characteristic.path] = callback
        self._subscriptions.append(characteristic.handle)

        reply = await self._bus.call(
            Message(
                destination=defs.BLUEZ_SERVICE,
                path=characteristic.path,
                interface=defs.GATT_CHARACTERISTIC_INTERFACE,
                member="StartNotify",
            )
        )
        assert_reply(reply)

    async def stop_notify(
        self,
        char_specifier: Union[BleakGATTCharacteristic, int, str, UUID],
    ) -> None:
        """Deactivate notification/indication on a specified characteristic.

        Args:
            char_specifier (BleakGATTCharacteristic, int, str or UUID): The characteristic to deactivate
                notification/indication on, specified by either integer handle, UUID or
                directly by the BleakGATTCharacteristic object representing it.

        """
        if not isinstance(char_specifier, BleakGATTCharacteristic):
            characteristic = self.services.get_characteristic(char_specifier)
        else:
            characteristic = char_specifier
        if not characteristic:
            raise BleakError("Characteristic {} not found!".format(char_specifier))

        reply = await self._bus.call(
            Message(
                destination=defs.BLUEZ_SERVICE,
                path=characteristic.path,
                interface=defs.GATT_CHARACTERISTIC_INTERFACE,
                member="StopNotify",
            )
        )
        assert_reply(reply)

        self._notification_callbacks.pop(characteristic.path, None)

        self._subscriptions.remove(characteristic.handle)


    # DBUS introspection method for characteristics.

    async def get_all_for_characteristic(
        self,
        char_specifier: Union[BleakGATTCharacteristic, int, str, uuid.UUID],
    ) -> dict:
        """Get all properties for a characteristic.

        This method should generally not be needed by end user, since it is a DBus specific method.

        Args:
            char_specifier: The characteristic to get properties for, specified by either
                integer handle, UUID or directly by the BleakGATTCharacteristic
                object representing it.

        Returns:
            (dict) Properties dictionary

        """
        if not isinstance(char_specifier, BleakGATTCharacteristic):
            characteristic = self.services.get_characteristic(char_specifier)
        else:
            characteristic = char_specifier
        if not characteristic:
            raise BleakError("Characteristic {} not found!".format(char_specifier))

        out = await self._callRemote(
            characteristic.path,
            "GetAll",
            interface=defs.PROPERTIES_INTERFACE,
            destination=defs.BLUEZ_SERVICE,
            signature="s",
            body=[defs.GATT_CHARACTERISTIC_INTERFACE],
            returnSignature="a{sv}",
        )
        return out

    async def _get_device_properties(self, interface=defs.DEVICE_INTERFACE) -> dict:
        """Get properties of the connected device.

        Args:
            interface: Which DBus interface to get properties on. Defaults to `org.bluez.Device1`.

        Returns:
            (dict) The properties.

        """
        return await self._callRemote(
            self._device_path,
            "GetAll",
            interface=defs.PROPERTIES_INTERFACE,
            destination=defs.BLUEZ_SERVICE,
            signature="s",
            body=[interface],
            returnSignature="a{sv}",
        )

    async def _callRemote(self, path, method, *, returnSignature="", **kwargs):
        msg = Message(path=path, member=method, **kwargs)
        try:
            res = await self._bus.call(msg)
            if res.signature != returnSignature:
                raise RuntimeError("Res %r: want %s" % (res, returnSignature))
        except Exception as exc:
            breakpoint()
            raise

        if isinstance(res.body,(tuple,list)) and len(res.body) == 1:
            return res.body[0]
        return res.body

    # Internal Callbacks

    def _parse_msg(self, message: Message):
        if message.message_type != MessageType.SIGNAL:
            return

        logger.debug(
            "received D-Bus signal: {0}.{1} ({2}): {3}".format(
                message.interface, message.member, message.path, message.body
            )
        )
                    
        if message.member == "InterfacesAdded":
            path, interfaces = message.body
                         
            if defs.GATT_SERVICE_INTERFACE in interfaces:
                obj = de_variate(interfaces[defs.GATT_SERVICE_INTERFACE])
                # if this assert fails, it means our match rules are probably wrong
                assert obj["Device"] == self._device_path
                self.services.add_service(BleakGATTService(obj, path))

            if defs.GATT_CHARACTERISTIC_INTERFACE in interfaces:
                obj = de_variate(interfaces[defs.GATT_CHARACTERISTIC_INTERFACE])
                service = next(
                    x
                    for x in self.services.services.values()
                    if x.path == obj["Service"]
                )
                self.services.add_characteristic(
                    BleakGATTCharacteristic(obj, path, service.uuid)
                )

            if defs.GATT_DESCRIPTOR_INTERFACE in interfaces:
                obj = de_variate(interfaces[defs.GATT_DESCRIPTOR_INTERFACE])
                handle = int(obj["Characteristic"][-4:], 16)
                characteristic = self.services.characteristics[handle]
                self.services.add_descriptor(
                    BleakGATTDescriptor(obj, path, characteristic.uuid, handle)
                )
        elif message.member == "InterfacesRemoved":
            path, interfaces = message.body

        elif message.member == "PropertiesChanged":
            interface, changed, _ = message.body
            changed = de_variate(changed)

            if interface == defs.GATT_CHARACTERISTIC_INTERFACE:
                if message.path in self._notification_callbacks and "Value" in changed:
                    handle = int(message.path[-4:], 16)
                    self._notification_callbacks[message.path](handle, changed["Value"])
            elif interface == defs.DEVICE_INTERFACE:
                self._properties.update(changed)

                if "ServicesResolved" in changed:
                    if changed["ServicesResolved"]:
                        if self._services_resolved_event:
                            self._services_resolved_event.set()
                    else:
                        self._services_resolved = False

                if "Connected" in changed and not changed["Connected"]:
                    logger.debug(f"Device disconnected ({self._device_path})")

                    if self._disconnect_monitor_event:
                        self._disconnect_monitor_event.set()
                        self._disconnect_monitor_event = None
                    else:
                        raise BleakError(f"Device disconnected ({self._device_path})")


def _data_notification_wrapper(func, char_map):
    @wraps(func)
    def args_parser(sender, data):
        if "Value" in data:
            # Do a conversion from {'Value': [...]} to bytearray.
            return func(char_map.get(sender, sender), bytearray(data.get("Value")))

    return args_parser


def _regular_notification_wrapper(func, char_map):
    @wraps(func)
    def args_parser(sender, data):
        return func(char_map.get(sender, sender), data)

    return args_parser
