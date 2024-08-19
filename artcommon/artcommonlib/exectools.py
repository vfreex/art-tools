import asyncio
import contextvars
import errno
import functools
import os
import platform
import shlex
import subprocess
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from datetime import datetime

import tenacity
from fcntl import F_GETFL, F_SETFL, fcntl
from inspect import getframeinfo, stack
from multiprocessing.pool import MapResult, ThreadPool
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from urllib.request import urlopen

from future.utils import as_native_str
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import \
    TraceContextTextMapPropagator
from tenacity import stop_after_attempt, wait_fixed

from artcommonlib import logutil
from artcommonlib.format_util import green_print, yellow_print
from artcommonlib.pushd import Dir
from artcommonlib.telemetry import start_as_current_span_async

SUCCESS = 0

logger = logutil.get_logger(__name__)
TRACER = trace.get_tracer(__name__)

cmd_counter_lock = threading.Lock()
cmd_counter = 0  # Increments atomically to help search logs for command start/stop


class RetryException(Exception):
    """
    Provide a custom exception for retry failures
    """
    pass


class WrapException(Exception):
    """ https://bugs.python.org/issue13831 """
    def __init__(self):
        super(WrapException, self).__init__()
        exc_type, exc_value, exc_tb = sys.exc_info()
        self.exception = exc_value
        self.formatted = "".join(
            traceback.format_exception(exc_type, exc_value, exc_tb))

    @as_native_str()
    def __str__(self):
        return "{}\nOriginal traceback:\n{}".format(Exception.__str__(self), self.formatted)


def retry(retries, task_f, check_f=bool, wait_f=None):
    """
    Try a function up to n times.
    Raise an exception if it does not pass in time

    :param int retries: The number of times to retry
    :param func task_f: The function to be run and observed
    :param func()bool check_f: a function to check if task_f is complete
    :param func()bool wait_f: a function to run between checks
    """

    for attempt in range(retries):
        ret = task_f()
        if check_f(ret):
            return ret
        if attempt < retries - 1 and wait_f is not None:
            wait_f(attempt)
    raise RetryException("Giving up after {} failed attempt(s)".format(retries))


def urlopen_assert(url_or_req, httpcode=200, retries=3):
    """
    Retries a URL pull several times before throwing a RetryException
    :param url_or_req: The URL to open with urllib  or a customized urllib.request.Request
    :param httpcode: The http numeric code indicating success
    :param retries: The number of retries to permit
    :return: The success result of urlopen (or a RetryException will be thrown)
    """
    return retry(retries, lambda: urlopen(url_or_req),
                 check_f=lambda req: req.code == httpcode,
                 wait_f=lambda x: time.sleep(30))


def cmd_assert(cmd, realtime=False, retries=1, pollrate=60, on_retry=None, set_env=None, strip=False,
               log_stdout: bool = False, log_stderr: bool = True, timeout: Optional[int] = None,
               cwd: Optional[str] = None) -> Tuple[str, str]:
    """
    Run a command, logging (using exec_cmd) and raise an exception if the
    return code of the command indicates failure.
    Try the command multiple times if requested.

    :param <string|list> cmd: A shell command
    :param realtime: If True, output stdout and stderr in realtime instead of all at once.
    :param int retries: The number of times to try before declaring failure
    :param int pollrate: how long to sleep between tries
    :param <string|list> on_retry: A shell command to run before retrying a failure
    :param set_env: Dict of env vars to set for command (overriding existing)
    :param strip: Strip extra whitespace from stdout/err before returning.
    :param log_stdout: Whether stdout should be logged into the DEBUG log.
    :param log_stderr: Whether stderr should be logged into the DEBUG log
    :param timeout: Kill the process if it does not terminate after timeout seconds.
    :param cwd: Set current working directory
    :return: (stdout,stderr) if exit code is zero
    """

    result, stdout, stderr = -1, '', ''
    try_num = 0

    for try_num in range(0, retries):
        if try_num > 0:
            logger.debug(
                "cmd_assert: Failed {} times. Retrying in {} seconds: {}".
                format(try_num, pollrate, cmd))
            time.sleep(pollrate)
            if on_retry is not None:
                # Run the recovery command between retries. Nothing to collect or assert -- just try it.
                cmd_gather(on_retry, set_env)

        result, stdout, stderr = cmd_gather(cmd, set_env=set_env, realtime=realtime, strip=strip,
                                            log_stdout=log_stdout, log_stderr=log_stderr, timeout=timeout, cwd=cwd)
        if result == SUCCESS:
            break

    logger.debug("cmd_assert: Final result = {} in {} tries.".format(result, try_num + 1))

    if result != SUCCESS:
        msg = f"Process {cmd} exited with code {result}:\ncwd={Dir.getcwd()}\nstdout>>{stdout}<<\nstderr>>{stderr}<<\n"
        raise ChildProcessError(msg, (result, stdout, stderr))

    return stdout, stderr


def cmd_gather(cmd: Union[str, List], set_env: Optional[Dict[str, str]] = None, realtime=False, strip=False,
               log_stdout=False, log_stderr=True, timeout: Optional[int] = None,
               cwd: Optional[str] = None) -> Tuple[int, str, str]:
    """
    Runs a command and returns rc,stdout,stderr as a tuple.

    If called while the `Dir` context manager is in effect, guarantees that the
    process is executed in that directory, even if it is no longer the current
    directory of the process (i.e. it is thread-safe).

    :param cmd: The command and arguments to execute
    :param set_env: Dict of env vars to override in the current doozer environment.
    :param realtime: If True, output stdout and stderr in realtime instead of all at once.
    :param strip: Strip extra whitespace from stdout/err before returning.
    :param log_stdout: Whether stdout should be logged into the DEBUG log.
    :param log_stderr: Whether stderr should be logged into the DEBUG log
    :param timeout: Kill the process if it does not terminate after timeout seconds.
    :param cwd: Set current working directory
    :return: (rc,stdout,stderr)
    """

    global cmd_counter, cmd_counter_lock

    with cmd_counter_lock:
        my_id = cmd_counter
        cmd_counter = cmd_counter + 1

    if not isinstance(cmd, list):
        cmd_list = shlex.split(cmd)
    else:
        # convert any non-str into str
        cmd_list = [str(c) for c in cmd]

    if not cwd:
        cwd = Dir.getcwd()
    cmd_info_base = f'${my_id}: {cmd_list} - [cwd={cwd}]'
    cmd_info = cmd_info_base

    env = os.environ.copy()
    if set_env:
        cmd_info = '{} [env={}]'.format(cmd_info, set_env)
        env.update(set_env)

    # Make sure output of launched commands is utf-8
    env['LC_ALL'] = 'en_US.UTF-8'

    with timer(logger.debug, f'{cmd_info}: Executed:cmd_gather'):
        logger.info(f'{cmd_info}: Executing:cmd_gather')
        try:
            proc = subprocess.Popen(
                cmd_list, cwd=cwd, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL)
        except OSError as exc:
            description = "{}: Errored:\nException:\n{}\nIs {} installed?".format(cmd_info, exc, cmd_list[0])
            logger.error(description)
            return exc.errno, "", description

        if not realtime:
            try:
                out, err = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                out, err = proc.communicate()
            rc = proc.returncode
        else:
            out = b''
            err = b''

            # Many thanks to http://eyalarubas.com/python-subproc-nonblock.html
            # setup non-blocking read
            # set the O_NONBLOCK flag of proc.stdout file descriptor:
            flags = fcntl(proc.stdout, F_GETFL)  # get current proc.stdout flags
            fcntl(proc.stdout, F_SETFL, flags | os.O_NONBLOCK)
            # set the O_NONBLOCK flag of proc.stderr file descriptor:
            flags = fcntl(proc.stderr, F_GETFL)  # get current proc.stderr flags
            fcntl(proc.stderr, F_SETFL, flags | os.O_NONBLOCK)

            rc = None
            stdout_complete = False
            stderr_complete = False
            while not stdout_complete or not stderr_complete or rc is None:
                try:
                    output = os.read(proc.stdout.fileno(), 4096)
                    if output:
                        green_print(output.rstrip())
                        out += output
                    else:
                        stdout_complete = True
                except OSError as ose:
                    if ose.errno == errno.EAGAIN or ose.errno == errno.EWOULDBLOCK:
                        pass  # It is supposed to raise one of these exceptions
                    else:
                        raise

                try:
                    error = os.read(proc.stderr.fileno(), 4096)
                    if error:
                        yellow_print(error.rstrip())
                        out += error
                    else:
                        stderr_complete = True
                except OSError as ose:
                    if ose.errno == errno.EAGAIN or ose.errno == errno.EWOULDBLOCK:
                        pass  # It is supposed to raise one of these exceptions
                    else:
                        raise

                if rc is None:
                    rc = proc.poll()
                    time.sleep(0.5)  # reduce busy-wait

        # We read in bytes representing utf-8 output; decode so that python recognizes them as unicode strings
        out = out.decode('utf-8')
        err = err.decode('utf-8')

        log_output_stdout = out
        log_output_stderr = err
        if not log_stdout and len(out) > 200:
            log_output_stdout = f'{out[:200]}\n..truncated..'
        if not log_stderr and len(err) > 200:
            log_output_stderr = f'{err[:200]}\n..truncated..'

        if rc:
            logger.debug(
                "{}: Exited with error: {}\nstdout>>{}<<\nstderr>>{}<<\n".
                format(cmd_info, rc, log_output_stdout, log_output_stderr))
        else:
            logger.debug(
                "{}: Exited with: {}\nstdout>>{}<<\nstderr>>{}<<\n".
                format(cmd_info, rc, log_output_stdout, log_output_stderr))

    if strip:
        out = out.strip()
        err = err.strip()

    return rc, out, err


@contextmanager
def timer(out_method, msg):
    caller = getframeinfo(stack()[2][0])  # Line that called this method
    caller_caller = getframeinfo(stack()[3][0])  # Line that called the method calling this method
    start_time = datetime.now()
    try:
        yield
    finally:
        time_elapsed = datetime.now() - start_time
        entry = (f'Time elapsed (hh:mm:ss.ms) {time_elapsed} in {os.path.basename(caller.filename)}:{caller.lineno} '
                 f'from {os.path.basename(caller_caller.filename)}:{caller_caller.lineno}:'
                 f'{caller_caller.code_context[0].strip() if caller_caller.code_context else ""} : {msg}')
        out_method(entry)


async def to_thread(func, *args, **kwargs):
    """Asynchronously run function *func* in a separate thread.

    This function is a backport of asyncio.to_thread from Python 3.9.

    Any *args and **kwargs supplied for this function are directly passed
    to *func*. Also, the current :class:`contextvars.Context` is propogated,
    allowing context variables from the main thread to be accessed in the
    separate thread.

    Return a coroutine that can be awaited to get the eventual result of *func*.
    """

    loop = asyncio.get_event_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


def limit_concurrency(limit=5):
    """A decorator to limit the number of parallel tasks with asyncio.

    It should be noted that when the decorator function is executed, the created Semaphore is bound to the default event loop.
    https://stackoverflow.com/a/66289885
    """

    # use asyncio.BoundedSemaphore(5) instead of Semaphore to prevent accidentally increasing the original limit
    # (stackoverflow.com/a/48971158/6687477)
    sem = asyncio.BoundedSemaphore(limit)

    def executor(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            async with sem:
                return await func(*args, **kwargs)

        return wrapper

    return executor


def fire_and_forget(cwd, shell_cmd, quiet=True):
    """
    Executes a command in a separate process that can continue to do its work
    even after doozer terminates.
    :param cwd: Path in which to launch the command
    :param shell_cmd: A string to send to the shell
    :param quiet: Whether to sink stdout & stderr to /dev/null
    :return: N/A
    """

    if quiet:
        shell_cmd = f'{shell_cmd} > /dev/null 2>/dev/null'

    # https://stackoverflow.com/a/13256908
    kwargs = {}
    if platform.system() == 'Windows':
        # from msdn [1]
        CREATE_NEW_PROCESS_GROUP = 0x00000200  # note: could get it from subprocess
        DETACHED_PROCESS = 0x00000008  # 0x8 | 0x200 == 0x208
        kwargs.update(creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)

    elif sys.version_info < (3, 2):  # assume posix
        kwargs.update(preexec_fn=os.setsid)

    else:  # Python 3.2+ and Unix
        kwargs.update(start_new_session=True)

    p = subprocess.Popen(f'{shell_cmd}', env=os.environ.copy(), shell=True, stdin=None, stdout=None, stderr=None,
                         cwd=cwd, close_fds=True, **kwargs)
    assert not p.poll()


def parallel_exec(f, args, n_threads=None) -> MapResult:
    """
    :param f: A function to invoke for all arguments
    :param args: A list of argument tuples. Each tuple will be used to invoke the function once.
    :param n_threads: preferred number of threads to use during the work
    :return:
    """

    n_threads = n_threads if n_threads is not None else max(len(args), 1)
    terminate_event = threading.Event()
    pool = ThreadPool(n_threads)

    # Python 3 doesn't allow to unpack tuple argument in a lambdas or functions (PEP-3113).
    # `unpack_tuple_args` is a workaround that unpacks the tuple as arguments for the function
    # passed to `ThreadPool.map_async`.
    # `starmap_async` can be used in the future when we don't keep compatibility with Python 2.
    ret = pool.map_async(
        wrap_exception(unpack_tuple_args(f)),
        [(a, terminate_event) for a in args])
    pool.close()
    try:
        # `wait` without a timeout disables signal handling
        while not ret.ready():
            ret.wait(60)
    except KeyboardInterrupt:
        logger.warning('SIGINT received, signaling threads to terminate...')
        terminate_event.set()
    pool.join()
    return ret


def wrap_exception(func):
    """ Decorate a function, wrap exception if it occurs. """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            raise WrapException()
    return wrapper


def unpack_tuple_args(func):
    """ Decorate a function for unpacking the tuple argument `args`
        This is used to workaround Python 3 lambda not unpacking tuple arguments (PEP-3113)
    """
    @functools.wraps(func)
    def wrapper(args):
        return func(*args)
    return wrapper


@tenacity.retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(60))
async def manifest_tool(options, dry_run=False):
    auth_opt = ""
    if os.environ.get("XDG_RUNTIME_DIR"):
        auth_file = os.path.expandvars("${XDG_RUNTIME_DIR}/containers/auth.json")
        if Path(auth_file).is_file():
            auth_opt = f"--docker-cfg={auth_file}"

    if isinstance(options, str):
        cmd = f'manifest-tool {auth_opt} {options}'

    elif isinstance(options, list):
        cmd = ['manifest-tool', auth_opt]
        cmd.extend(options)

    else:
        raise ValueError('Invalid type for manifest-tool options provided')

    if dry_run:
        logger.warning("[DRY RUN] Would have run %s", cmd)
        return

    await cmd_assert_async(cmd)


@start_as_current_span_async(TRACER, "cmd_gather_async")
async def cmd_gather_async(cmd: Union[List[str], str], check: bool = True, **kwargs) -> Tuple[Optional[int], str, str]:
    """ Runs a command asynchronously and returns rc,stdout,stderr as a tuple
    :param cmd <string|list>: A shell command
    :param check: If check is True and the exit code was non-zero, it raises a ChildProcessError
    :param kwargs: Other arguments passing to asyncio.subprocess.create_subprocess_exec
    :return: rc, stdout, stderr
    """

    if isinstance(cmd, str):
        cmd_list = shlex.split(cmd)
    else:
        cmd_list = cmd

    span = trace.get_current_span()
    span.set_attribute("pyartcd.param.cmd", cmd_list)

    logger.info("Executing:cmd_gather_async %s", cmd_list)
    # capture stdout and stderr if they are not set in kwargs
    if "stdout" not in kwargs:
        kwargs["stdout"] = asyncio.subprocess.PIPE
    if "stderr" not in kwargs:
        kwargs["stderr"] = asyncio.subprocess.PIPE

    # Propagate trace context to subprocess
    env = kwargs.get("env", {})
    carrier = {}
    TraceContextTextMapPropagator().inject(carrier)
    if "traceparent" in carrier:
        env["TRACEPARENT"] = carrier["traceparent"]
        kwargs["env"] = env

    proc = await asyncio.subprocess.create_subprocess_exec(cmd_list[0], *cmd_list[1:], **kwargs)
    stdout, stderr = await proc.communicate()
    stdout = stdout.decode() if stdout else ""
    stderr = stderr.decode() if stderr else ""
    span.set_attribute("pyartcd.result.exit_code", str(proc.returncode))
    if proc.returncode != 0:
        msg = f"Process {cmd_list!r} exited with code {proc.returncode}.\nstdout>>{stdout}<<\nstderr>>{stderr}<<\n"
        if check:
            raise ChildProcessError(msg)
        else:
            logger.warning(msg)
    span.set_status(trace.StatusCode.OK)
    return proc.returncode, stdout, stderr


@start_as_current_span_async(TRACER, "cmd_assert_async")
async def cmd_assert_async(cmd: Union[List[str], str], check: bool = True, **kwargs) -> int:
    """ Runs a command and optionally raises an exception if the return code of the command indicates failure.
    :param cmd <string|list>: A shell command
    :param check: If check is True and the exit code was non-zero, it raises a ChildProcessError
    :param kwargs: Other arguments passing to asyncio.subprocess.create_subprocess_exec
    :return: return code of the command
    """
    if isinstance(cmd, str):
        cmd_list = shlex.split(cmd)
    else:
        cmd_list = cmd

    span = trace.get_current_span()
    span.set_attribute("pyartcd.param.cmd", cmd_list)

    # Propagate trace context to subprocess
    env = kwargs.get("env", {})
    carrier = {}
    TraceContextTextMapPropagator().inject(carrier)
    if "traceparent" in carrier:
        env["TRACEPARENT"] = carrier["traceparent"]
        logger.warning("Pass TRACEPARENT %s", env["TRACEPARENT"])
        kwargs["env"] = env

    logger.info("Executing:cmd_assert_async %s", cmd_list)
    proc = await asyncio.subprocess.create_subprocess_exec(cmd_list[0], *cmd_list[1:], **kwargs)
    returncode = await proc.wait()
    span.set_attribute("pyartcd.result.exit_code", str(returncode))
    if returncode != 0:
        msg = f"Process {cmd_list!r} exited with code {returncode}."
        if check:
            raise ChildProcessError(msg)
        else:
            logger.warning(msg)
    span.set_status(trace.StatusCode.OK)
    return returncode
