# -*- coding: utf-8 -*-
import re
from asyncdbus.constants import MessageType
from asyncdbus import Message
from asyncdbus.signature import Variant

from ...uuids import uuidstr_to_str
from . import defs

_mac_address_regex = re.compile("^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$")


def validate_mac_address(address):
    return _mac_address_regex.match(address) is not None


async def get_managed_objects(bus, object_path_filter=None):
    objects = (await bus.call(Message(
            path="/",
            member="GetManagedObjects",
            interface="org.freedesktop.DBus.ObjectManager",
            destination="org.bluez",
        ))).body[0]
    objects = de_variate(objects)
    if object_path_filter:
        return dict(
            filter(lambda i: i[0].startswith(object_path_filter), objects.items())
        )

    else:
        return objects


def format_GATT_object(object_path, interfaces):
    if defs.GATT_SERVICE_INTERFACE in interfaces:
        props = interfaces.get(defs.GATT_SERVICE_INTERFACE)
        _type = "{0} Service".format("Primary" if props.get("Primary") else "Secondary")
    elif defs.GATT_CHARACTERISTIC_INTERFACE in interfaces:
        props = interfaces.get(defs.GATT_CHARACTERISTIC_INTERFACE)
        _type = "Characteristic"
    elif defs.GATT_DESCRIPTOR_INTERFACE in interfaces:
        props = interfaces.get(defs.GATT_DESCRIPTOR_INTERFACE)
        _type = "Descriptor"
    else:
        return None

    _uuid = props.get("UUID")
    return "\n{0}\n\t{1}\n\t{2}\n\t{3}".format(
        _type, object_path, _uuid, uuidstr_to_str(_uuid)
    )


def de_variate(obj):
    if isinstance(obj,Variant):
        return de_variate(obj.value)
    elif isinstance(obj, dict):
        return { k:de_variate(v) for k,v in obj.items() }
    elif isinstance(obj, (list,tuple)):
        return [ de_variate(v) for v in obj ]
    else:
        return obj

def assert_reply(reply: Message):
    """Checks that a D-Bus message is a valid reply.

    Raises:
        BleakDBusError: if the message type is ``MessageType.ERROR``
        AssentationError: if the message type is not ``MessageType.METHOD_RETURN``
    """
    if reply.message_type != MessageType.METHOD_RETURN:
        raise RuntimeError("Wrong message type: %r" % (reply,))


