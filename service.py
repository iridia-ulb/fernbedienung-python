# imports
import asyncio
import base64
import fcntl
import json
import logging
import mmap
import os
import signal
import sys
import v4l2

# constants
FRAME_HEADER_LEN = 4

async def client_handler(rx: asyncio.StreamReader, tx: asyncio.StreamWriter):
    process_rxs = {}
    stream_rxs = {}
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
                        # extract the process attributes
                        target = run_process_request['target']
                        working_dir = run_process_request['working_dir']
                        args = run_process_request['args']
                        # queues for communicating with this process
                        process_rx = asyncio.Queue()
                        process_tx = client_tx_queue
                        process_rxs[uuid] = process_rx
                        # run process
                        process_coroutine = process(uuid, working_dir, target, args, process_tx, process_rx)
                        asyncio.get_event_loop().create_task(process_coroutine)
                    if 'StandardInput' in process_request or 'Terminate' in process_request:
                        if uuid in process_rxs:
                            # forward the request to the process handler
                            await process_rxs[uuid].put(process_request)
                if 'Stream' in request:
                    stream_request = request['Stream']
                    if 'Start' in stream_request:
                        start_stream_request = stream_request['Start']
                        # extract the stream attributes
                        device = start_stream_request['device']
                        width = int(start_stream_request['width'])
                        height = int(start_stream_request['height'])
                        # queues for communicating with this stream
                        stream_rx = asyncio.Queue()
                        stream_tx = client_tx_queue
                        stream_rxs[uuid] = stream_rx
                        # start stream
                        stream_coroutine = stream(uuid, device, width, height, stream_tx, stream_rx)
                        asyncio.get_event_loop().create_task(stream_coroutine)
                    if 'Stop' in stream_request:
                        if uuid in stream_rxs:
                            # forward the request to the stream handler
                            await stream_rxs[uuid].put(stream_request)
    # client handler done
    [task.cancel() for task in pending_tasks]
    if tx.can_write_eof():
        tx.write_eof()
    await tx.drain()
    tx.close()
    await tx.wait_closed()

async def stream(uuid: str, device: str, width: int, height: int,
                 stream_tx: asyncio.Queue, stream_rx: asyncio.Queue):
    try:
        logger.info('starting: \'%s\'', device)
        dev = open(device, 'rb+', buffering=0)
        cap = v4l2.v4l2_capability()
        fcntl.ioctl(dev, v4l2.VIDIOC_QUERYCAP, cap)
        if not bool(cap.capabilities & v4l2.V4L2_CAP_VIDEO_CAPTURE):
            raise Exception(device + ' does not support capture')
        if not bool(cap.capabilities & v4l2.V4L2_CAP_STREAMING):
            raise Exception(device + ' does not support streaming')
        capture_format = v4l2.v4l2_format()
        capture_format.type = v4l2.V4L2_BUF_TYPE_VIDEO_CAPTURE
        # get current settings
        fcntl.ioctl(dev, v4l2.VIDIOC_G_FMT, capture_format)
        # set the pixel format to MJPEG
        capture_format.fmt.pix.pixelformat = v4l2.V4L2_PIX_FMT_MJPEG
        capture_format.fmt.pix.width = width
        capture_format.fmt.pix.height = height
        # update the settings
        fcntl.ioctl(dev, v4l2.VIDIOC_S_FMT, capture_format)
        # request a buffer
        request = v4l2.v4l2_requestbuffers()
        request.type = v4l2.V4L2_BUF_TYPE_VIDEO_CAPTURE
        request.memory = v4l2.V4L2_MEMORY_MMAP
        request.count = 1
        fcntl.ioctl(dev, v4l2.VIDIOC_REQBUFS, request)
        # query buffer for mmap
        buffer = v4l2.v4l2_buffer()
        buffer.type = v4l2.V4L2_BUF_TYPE_VIDEO_CAPTURE
        buffer.memory = v4l2.V4L2_MEMORY_MMAP
        buffer.index = 0
        fcntl.ioctl(dev, v4l2.VIDIOC_QUERYBUF, buffer)
        mmap_buffer = mmap.mmap(dev.fileno(), buffer.length, mmap.MAP_SHARED,
            mmap.PROT_READ | mmap.PROT_WRITE, offset=buffer.m.offset)
    except Exception as error:
        response = [uuid, {'Error' : str(error)}]
        await stream_tx.put(response)
    else:
        buffer_type = v4l2.v4l2_buf_type(v4l2.V4L2_BUF_TYPE_VIDEO_CAPTURE)
        fcntl.ioctl(dev, v4l2.VIDIOC_STREAMON, buffer_type)
        # enqueue the buffer
        fcntl.ioctl(dev, v4l2.VIDIOC_QBUF, buffer)
        # notify the client that the process started
        response = [uuid, 'Ok']
        await stream_tx.put(response)
        # respond to client and process events
        event_loop = asyncio.get_event_loop()
        capture_task = event_loop.create_task(asyncio.sleep(0.5))
        request_task = event_loop.create_task(stream_rx.get())
        pending_tasks = {capture_task, request_task}
        while pending_tasks:
            complete_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
            if capture_task in complete_tasks:
                await capture_task
                # dequeue the buffer
                fcntl.ioctl(dev, v4l2.VIDIOC_DQBUF, buffer)
                # read the buffer
                frame = mmap_buffer.read()
                mmap_buffer.seek(0)
                # enqueue the buffer
                fcntl.ioctl(dev, v4l2.VIDIOC_QBUF, buffer)
                frame = base64.b64encode(frame).decode("utf-8")
                response = [uuid, {'Stream': {'Frame': frame}}]
                await stream_tx.put(response)
                capture_task = event_loop.create_task(asyncio.sleep(1))
                pending_tasks.add(capture_task)
            if request_task in complete_tasks:
                request = await request_task
                if 'Stop' in request:
                    capture_task.cancel()
                    pending_tasks.discard(capture_task)
        # dequeue the buffer
        fcntl.ioctl(dev, v4l2.VIDIOC_DQBUF, buffer)
        fcntl.ioctl(dev, v4l2.VIDIOC_STREAMOFF, buffer_type)
        logger.info('stopped: \'%s\'', device)

async def process(uuid: str, working_dir: str, target: str, args: list,
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

