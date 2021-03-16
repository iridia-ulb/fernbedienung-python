# imports
import asyncio
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
                    logger.warn('Could not decode message')
                    continue
                # extract uuid and request
                uuid, request = message[0], message[1]
                if 'Ping' in request:
                    response = [uuid, 'Ok']
                    await client_tx_queue.put(response)
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
                        # build a shell command for running the task
                        target = run_process_request['target']
                        working_dir = run_process_request['working_dir']
                        args = run_process_request['args']
                        # queues for communicating with this process
                        process_rx = asyncio.Queue()
                        process_tx = client_tx_queue
                        process_rxs[uuid] = process_rx
                        # run process
                        run_process_coroutine = run_process(uuid, working_dir, target, args, process_tx, process_rx)
                        asyncio.get_event_loop().create_task(run_process_coroutine)
                    if 'StandardInput' in process_request or 'Terminate' in process_request:
                        if uuid in process_rxs:
                            # forward the request to the process handler
                            await process_rxs[uuid].put(process_request)
    # client handler done
    if tx.can_write_eof():
        tx.write_eof()
    await tx.drain()
    tx.close()
    await tx.wait_closed()
        
async def run_process(uuid: str, working_dir: str, target: str, args: list[str],
                      process_tx: asyncio.Queue, process_rx: asyncio.Queue):
    prev_working_dir = os.getcwd()
    try:
        os.chdir(working_dir)
        logger.info('running: \'%s\'', target)
        subprocess = await asyncio.create_subprocess_exec(target, *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        os.chdir(prev_working_dir)
    except Exception as error:
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
                stderr = list(await read_stderr_task)
                if stderr:
                    response = [uuid, {'Process': {'StandardError' : stderr}}]
                    await process_tx.put(response)
                    read_stderr_task = event_loop.create_task(subprocess.stderr.read(1024))
                    pending_tasks.add(read_stderr_task)
            if read_stdout_task in complete_tasks:
                stdout = list(await read_stdout_task)
                if stdout:
                    response = [uuid, {'Process': {'StandardOutput' : stdout}}]
                    await process_tx.put(response)
                    read_stdout_task = event_loop.create_task(subprocess.stdout.read(1024))
                    pending_tasks.add(read_stdout_task)
            if request_task in complete_tasks:
                request = await request_task
                if 'Terminate' in request:
                    subprocess.terminate()
                if 'StandardInput' in request:
                    stdin = bytes(request['StandardInput'])
                    subprocess.stdin.write(stdin)
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

