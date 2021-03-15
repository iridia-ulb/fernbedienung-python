# Python implementation of the Fernbedienung service

The python script in this repository is designed to be run by a robot that needs to be controlled remotely.

## Installation
A typical usecase involves running this python script at boot once the network comes online. For example with systemd, one could define a service file as follows:
```
[Unit]
Description=Fernbedienung
After=network.target

[Service]
ExecStart=/usr/bin/python3 /usr/lib/fernbedienung/service.py

[Install]
WantedBy=multi-user.target
```
## Protocol
The Fernbedienung protocol involves exchanging JSON messages over a TCP socket. The first four bytes of each message indicates the length of the JSON payload allowing the stream to be broken down into individual messages.

## Warnings
The protocol implements absolutely no security. Running this service means that any device on the same network can upload files and execute arbitary commands on a device running this client. If more security is required, consider running this service on localhost (127.0.0.1) and use SSH tunneling to access it.
