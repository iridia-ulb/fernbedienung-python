# Python implementation of the Fernbedienung service

The python script in this repository is designed to be ran as a Linux service on a robot that needs to be controlled remotely. By default the service listens for incoming TCP connections on port 17653 of any network interface.

## Installation
A typical usecase involves running this python script at boot once the network comes online. For example with systemd, one could define a unit file as follows:
```
[Unit]
Description=Fernbedienung
After=network.target

[Service]
ExecStart=/usr/bin/python3 /usr/lib/fernbedienung/service.py

[Install]
WantedBy=multi-user.target
```
This script and systemd configuration is installed automatically on the Pi-Puck and IRIDIA Drone images generated using the meta-drone and meta-pipuck layers in this organisation.

## Protocol
The Fernbedienung protocol involves exchanging JSON messages over a TCP socket. The first four bytes of each message indicates the length of the JSON payload allowing the stream to be broken down into individual messages. An 

### Requests
Requests are sent from a client to a robot. The following table summarizes the possible requests:
| Request | Description                                                                                       | JSON           |
|---------|---------------------------------------------------------------------------------------------------|----------------|
| Halt    | Runs the halt command on a robot                                                                  | `{ "Halt" }`   |
| Reboot  | Runs the reset command on a robot                                                                 | `{ "Reboot" }` |
| Process | Runs an program or script on the robot, streaming back its stdout, stderr, and termination status |                |
| Upload  | Uploads a single file to a robot                                                                  |                |

### Responses
Responses are sent from a robot to a client in response to a request. If a process has been started on a robot, the robot will continue to send responses whenever the process writes to the standard input or standard output until that process as terminated.

## Warnings
The protocol implements absolutely no security. Running this service means that any device on the same network can upload files and execute arbitary commands on a device running this client. If more security is required, consider running this service on localhost (127.0.0.1) and use SSH tunneling to access it.
