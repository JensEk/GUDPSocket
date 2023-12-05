# GUDPSocket

## Description

GUDPSocket is a project that extends the User Datagram Protocol (UDP) with a guaranteed delivery mechanism. This project aims to provide the speed of UDP while ensuring the reliability of TCP.

## Run
   1. Compile either transfer agent VSSend or VSRecv
   2. Use either VSSend or VSRecv to transmit/receive file with:
      - `VSSend [-d] host1:port1 [host2:port2] ... file1 [file2]`
      - `VSRecv [-d] [-o] port`
   
