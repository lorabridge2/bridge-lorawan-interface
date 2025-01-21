LoRaWAN Interface of Bridge Unit
============================================

This repository is part of the [LoRaBridge](https://github.com/lorabridge2/lorabridge) project.

This repository contains source code for LoRaWAN interface, which acts as an interface between the bridge-automation-manager,
the bridge-forwarder and the bridge-lorawan-tx (LoRaWAN module).

Features
--------
- Forwarding of sensor data and system/user events
- Dequeueing data with priority (e.g. system events are always handled first)
- Enqueueing LB commands
- Timesync request at start-up

## License

All the LoRaBridge software components and the documentation are licensed under GNU General Public License 3.0.
