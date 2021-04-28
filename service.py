# imports
import asyncio
import base64
import json
import logging
import os
import signal
import sys

# constants
FRAME_HEADER_LEN = 4

async def client_handler(rx: asyncio.StreamReader, tx: asyncio.StreamWriter):
    process_rxs = {}
    client_tx_queue = asyncio.Queue()
    buffer = bytearray()
    event_loop = asyncio.get_event_loop()
    read_task = event_loop.create_task(rx.read(1024))
    dequeue_task = event_loop.create_task(client_tx_queue.get())
    pending_tasks = {read_task, dequeue_task}
    while pending_tasks and not rx.at_eof():
        complete_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
        if dequeue_task in complete_tasks:
            response = json.dumps(await dequeue_task).encode('utf-8')
            response_length = len(response).to_bytes(4, 'big')
            # send the response
            tx.write(response_length + response)
            await tx.drain()
            # recreate the dequeue task
            dequeue_task = event_loop.create_task(client_tx_queue.get())
            pending_tasks.add(dequeue_task)
        if read_task in complete_tasks:
            buffer.extend(await read_task)
            # recreate the read task if we haven't reached EOF
            read_task = event_loop.create_task(rx.read(1024))
            pending_tasks.add(read_task)
            # parse the buffer
            while len(buffer) >= FRAME_HEADER_LEN:
                message_length = int.from_bytes(buffer[:FRAME_HEADER_LEN], byteorder='big', signed=False)
                if len(buffer) < FRAME_HEADER_LEN + message_length:
                    # more data needed, break the inner loop and await more data
                    break
                # extract message from the buffer
                message = buffer[FRAME_HEADER_LEN: FRAME_HEADER_LEN+message_length]
                buffer = buffer[FRAME_HEADER_LEN+message_length:]
                # decode message
                try:
                    message = json.loads(message)
                except JSONDecodeError:
                    logger.warning('Could not decode message')
                    continue
                # extract uuid and request
                uuid, request = message[0], message[1]
                if 'Reboot' in request:
                    # send response
                    response = json.dumps([uuid, 'Ok']).encode('utf-8')
                    response_length = len(response).to_bytes(4, 'big')
                    tx.write(response_length + response)
                    await tx.drain()
                    # execute reboot
                    reboot = await asyncio.create_subprocess_exec("reboot")
                    await reboot.wait()
                if 'Halt' in request:
                    # send response
                    response = json.dumps([uuid, 'Ok']).encode('utf-8')
                    response_length = len(response).to_bytes(4, 'big')
                    tx.write(response_length + response)
                    await tx.drain()
                    # execute halt
                    halt = await asyncio.create_subprocess_exec("halt")
                    await halt.wait()
                if 'Upload' in request:
                    upload_request = request['Upload']
                    contents = bytes(upload_request['contents'])
                    filename = upload_request['filename']
                    path = upload_request['path']
                    if path.endswith('/'):
                        filepath = path + filename
                    else:
                        filepath = path + '/' + filename
                    logger.info('uploading: %s', filepath)
                    try:
                        with open(filepath, 'wb') as upload:
                            upload.write(contents)
                    except Exception as error: 
                        response = [uuid, {'Error' : str(error)}]
                        await client_tx_queue.put(response)
                    else:
                        response = [uuid, 'Ok']
                        await client_tx_queue.put(response)
                if 'Process' in request:
                    process_request = request['Process']
                    if 'Run' in process_request:
                        run_process_request = process_request['Run']
                        # queues for communicating with this process
                        process_rx = asyncio.Queue()
                        process_tx = client_tx_queue
                        process_rxs[uuid] = process_rx
                        # run process
                        process_coroutine = process(uuid, run_process_request, process_tx, process_rx)
                        asyncio.get_event_loop().create_task(process_coroutine)
                    if 'StandardInput' in process_request or 'Terminate' in process_request:
                        if uuid in process_rxs:
                            # forward the request to the process handler
                            await process_rxs[uuid].put(process_request)
    # client handler done
    [task.cancel() for task in pending_tasks]
    if tx.can_write_eof():
        tx.write_eof()
    await tx.drain()
    tx.close()
    await tx.wait_closed()

async def process(uuid: str, run_process_request: dict, process_tx: asyncio.Queue, process_rx: asyncio.Queue):
    try:
        target = run_process_request['target']
        args = run_process_request['args']
        logger.info('running: %s %s', target, ' '.join(args))
        if 'working_dir' in run_process_request:
            working_dir = run_process_request['working_dir']
            subprocess = await asyncio.create_subprocess_exec(target, *args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=working_dir)
        else:
            subprocess = await asyncio.create_subprocess_exec(target, *args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
    except Exception as error:
        # report the error locally
        logger.warning('Could not start process: %s', str(error))
        # send the error to the client as a response
        response = [uuid, {'Error' : str(error)}]
        await process_tx.put(response)
    else:
        # notify the client that the process started
        response = [uuid, 'Ok']
        await process_tx.put(response)
        # respond to client and process events
        event_loop = asyncio.get_event_loop()
        read_stderr_task = event_loop.create_task(subprocess.stderr.read(1024))
        read_stdout_task = event_loop.create_task(subprocess.stdout.read(1024))
        request_task = event_loop.create_task(process_rx.get())
        pending_tasks = {read_stderr_task, read_stdout_task, request_task}
        while pending_tasks:
            complete_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
            if read_stderr_task in complete_tasks:
                stderr = await read_stderr_task
                if stderr:
                    stderr = base64.b64encode(stderr).decode("utf-8")
                    response = [uuid, {'Process': {'StandardError' : stderr}}]
                    await process_tx.put(response)
                    read_stderr_task = event_loop.create_task(subprocess.stderr.read(1024))
                    pending_tasks.add(read_stderr_task)
                elif request_task in pending_tasks:
                    # stderr is closing, clean up request_task if it is pending
                    request_task.cancel()
                    pending_tasks.discard(request_task)
            if read_stdout_task in complete_tasks:
                stdout = await read_stdout_task
                if stdout:
                    stdout = base64.b64encode(stdout).decode("utf-8")
                    response = [uuid, {'Process': {'StandardOutput': stdout}}]
                    await process_tx.put(response)
                    read_stdout_task = event_loop.create_task(subprocess.stdout.read(1024))
                    pending_tasks.add(read_stdout_task)
                elif request_task in pending_tasks:
                    # stdout is closing, clean up request_task if it is pending
                    request_task.cancel()
                    pending_tasks.discard(request_task)
            if request_task in complete_tasks:
                request = await request_task
                if 'Terminate' in request:
                    subprocess.terminate()
                if 'StandardInput' in request:
                    stdin = request['StandardInput']
                    subprocess.stdin.write(base64.b64decode(stdin))
                    await subprocess.stdin.drain()
                    # recreate the request task (only if stdin)
                    request_task = event_loop.create_task(process_rx.get())
                    pending_tasks.add(request_task)
        logger.info('terminated: \'%s\'', target)
        # queue the terminated message
        response = [uuid, {'Process': {'Terminated' : (await subprocess.wait()) == 0}}]
        await process_tx.put(response)

async def service_start():
    # create server instance
    server = await asyncio.start_server(client_handler, '0.0.0.0', 17653)
    logger.info('Started server on {}'.format(server.sockets[0].getsockname()))
    # run server
    await server.serve_forever()

async def service_stop():
    logger.info("cancelling remaining tasks")
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.get_event_loop().stop()

# set up the logger
logging.basicConfig(level=logging.INFO,
                    stream=sys.stderr,
                    format="[%(asctime)s %(levelname)-5s %(name)s] %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%SZ")
logger = logging.getLogger('fernbedienung')

# set up service event loop
event_loop = asyncio.get_event_loop()
# May want to catch other signals too
event_loop.add_signal_handler(signal.SIGINT,
    lambda : asyncio.get_event_loop().create_task(service_stop()))
try:
    event_loop.create_task(service_start())
    event_loop.run_forever()
finally:
    event_loop.close()
    logger.info("shutdown")

