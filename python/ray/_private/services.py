import base64
import collections
import errno
import io
import json
import logging
import multiprocessing
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time

import colorama
import psutil
# Ray modules
import ray
import ray.ray_constants as ray_constants
import redis

resource = None
if sys.platform != "win32":
    import resource

EXE_SUFFIX = ".exe" if sys.platform == "win32" else ""

# True if processes are run in the valgrind profiler.
RUN_RAYLET_PROFILER = False
RUN_PLASMA_STORE_PROFILER = False

# Location of the redis server and module.
RAY_HOME = os.path.join(os.path.dirname(os.path.dirname(__file__)), "../..")
RAY_PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
REDIS_EXECUTABLE = os.path.join(
    RAY_PATH, "core/src/ray/thirdparty/redis/src/redis-server" + EXE_SUFFIX)
REDIS_MODULE = os.path.join(
    RAY_PATH, "core/src/ray/gcs/redis_module/libray_redis_module.so")

# Location of the plasma object store executable.
PLASMA_STORE_EXECUTABLE = os.path.join(
    RAY_PATH, "core/src/plasma/plasma_store_server" + EXE_SUFFIX)

# Location of the raylet executables.
RAYLET_EXECUTABLE = os.path.join(RAY_PATH,
                                 "core/src/ray/raylet/raylet" + EXE_SUFFIX)
GCS_SERVER_EXECUTABLE = os.path.join(
    RAY_PATH, "core/src/ray/gcs/gcs_server" + EXE_SUFFIX)

# Location of the cpp default worker executables.
DEFAULT_WORKER_EXECUTABLE = os.path.join(
    RAY_PATH, "core/src/ray/cpp/default_worker" + EXE_SUFFIX)

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

ProcessInfo = collections.namedtuple("ProcessInfo", [
    "process",
    "stdout_file",
    "stderr_file",
    "use_valgrind",
    "use_gdb",
    "use_valgrind_profiler",
    "use_perftools_profiler",
    "use_tmux",
])


def serialize_config(config):
    config_pairs = []
    for key, value in config.items():
        if isinstance(value, str):
            value = value.encode("utf-8")
        if isinstance(value, bytes):
            value = base64.b64encode(value).decode("utf-8")
        config_pairs.append((key, value))
    config_str = ";".join(["{},{}".format(*kv) for kv in config_pairs])
    assert " " not in config_str, (
        "Config parameters currently do not support "
        "spaces:", config_str)
    return config_str


class ConsolePopen(subprocess.Popen):
    if sys.platform == "win32":

        def terminate(self):
            if isinstance(self.stdin, io.IOBase):
                self.stdin.close()
            if self._use_signals:
                self.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                super(ConsolePopen, self).terminate()

        def __init__(self, *args, **kwargs):
            # CREATE_NEW_PROCESS_GROUP is used to send Ctrl+C on Windows:
            # https://docs.python.org/3/library/subprocess.html#subprocess.Popen.send_signal
            new_pgroup = subprocess.CREATE_NEW_PROCESS_GROUP
            flags_to_add = 0
            if ray.utils.detect_fate_sharing_support():
                # If we don't have kernel-mode fate-sharing, then don't do this
                # because our children need to be in out process group for
                # the process reaper to properly terminate them.
                flags_to_add = new_pgroup
            flags_key = "creationflags"
            if flags_to_add:
                kwargs[flags_key] = (kwargs.get(flags_key) or 0) | flags_to_add
            self._use_signals = (kwargs[flags_key] & new_pgroup)
            super(ConsolePopen, self).__init__(*args, **kwargs)


def address(ip_address, port):
    return ip_address + ":" + str(port)


def new_port():
    return random.randint(10000, 65535)


def find_redis_address(address=None):
    """
    Attempts to find all valid Ray redis addresses on this node.

    Returns:
        Set of detected Redis instances.
    """
    # Currently, this extracts the deprecated --redis-address from the command
    # that launched the raylet running on this node, if any. Anyone looking to
    # edit this function should be warned that these commands look like, for
    # example:
    # /usr/local/lib/python3.8/dist-packages/ray/core/src/ray/raylet/raylet
    # --redis_address=123.456.78.910 --node_ip_address=123.456.78.910
    # --raylet_socket_name=... --store_socket_name=... --object_manager_port=0
    # --min_worker_port=10000 --max_worker_port=10999
    # --node_manager_port=58578 --redis_port=6379 --num_initial_workers=8
    # --maximum_startup_concurrency=8
    # --static_resource_list=node:123.456.78.910,1.0,object_store_memory,66
    # --config_list=plasma_store_as_thread,True
    # --python_worker_command=/usr/bin/python
    #     /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py
    #     --redis-address=123.456.78.910:6379
    #     --node-ip-address=123.456.78.910 --node-manager-port=58578
    #     --object-store-name=... --raylet-name=...
    #     --config-list=plasma_store_as_thread,True --temp-dir=/tmp/ray
    #     --metrics-agent-port=41856 --redis-password=[MASKED]
    #     --java_worker_command= --cpp_worker_command=
    #     --redis_password=[MASKED] --temp_dir=/tmp/ray --session_dir=...
    #     --metrics-agent-port=41856 --metrics_export_port=64229
    #     --agent_command=/usr/bin/python
    #     -u /usr/local/lib/python3.8/dist-packages/ray/new_dashboard/agent.py
    #         --redis-address=123.456.78.910:6379 --metrics-export-port=64229
    #         --dashboard-agent-port=41856 --node-manager-port=58578
    #         --object-store-name=... --raylet-name=... --temp-dir=/tmp/ray
    #         --log-dir=/tmp/ray/session_2020-11-08_14-29-07_199128_278000/logs
    #         --redis-password=[MASKED] --object_store_memory=5037192806
    #         --plasma_directory=/tmp
    # Longer arguments are elided with ... but all arguments from this instance
    # are included, to provide a sense of what is in these.
    # Indeed, we had to pull --redis-address to the front of each call to make
    # this readable.
    # As you can see, this is very long and complex, which is why we can't
    # simply extract all the the arguments using regular expressions and
    # present a dict as if we never lost track of these arguments, for
    # example. Picking out --redis-address below looks like it might grab the
    # wrong thing, but double-checking that we're finding the correct process
    # by checking that the contents look like we expect would probably be prone
    # to choking in unexpected ways.
    # Notice that --redis-address appears twice. This is not a copy-paste
    # error; this is the reason why the for loop below attempts to pick out
    # every appearance of --redis-address.

    # The --redis-address here is what is now called the --address, but it
    # appears in the default_worker.py and agent.py calls as --redis-address.
    pids = psutil.pids()
    redis_addresses = set()
    for pid in pids:
        try:
            proc = psutil.Process(pid)
            # HACK: Workaround for UNIX idiosyncrasy
            # Normally, cmdline() is supposed to return the argument list.
            # But it in some cases (such as when setproctitle is called),
            # an arbitrary string resembling a command-line is stored in
            # the first argument.
            # Explanation: https://unix.stackexchange.com/a/432681
            # More info: https://github.com/giampaolo/psutil/issues/1179
            cmdline = proc.cmdline()
            # NOTE(kfstorm): To support Windows, we can't use
            # `os.path.basename(cmdline[0]) == "raylet"` here.
            if len(cmdline) > 0 and "raylet" in os.path.basename(cmdline[0]):
                for arglist in cmdline:
                    # Given we're merely seeking --redis-address, we just split
                    # every argument on spaces for now.
                    for arg in arglist.split(" "):
                        # TODO(ekl): Find a robust solution for locating Redis.
                        if arg.startswith("--redis-address="):
                            proc_addr = arg.split("=")[1]
                            if address is not None and address != proc_addr:
                                continue
                            redis_addresses.add(proc_addr)
        except psutil.AccessDenied:
            pass
        except psutil.NoSuchProcess:
            pass
    return redis_addresses


def get_ray_address_to_use_or_die():
    """
    Attempts to find an address for an existing Ray cluster if it is not
    already specified as an environment variable.
    Returns:
        A string to pass into `ray.init(address=...)`
    """
    if "RAY_ADDRESS" in os.environ:
        return "auto"  # Avoid conflict with RAY_ADDRESS env var

    return find_redis_address_or_die()


def find_redis_address_or_die():

    redis_addresses = find_redis_address()
    if len(redis_addresses) > 1:
        raise ConnectionError(
            f"Found multiple active Ray instances: {redis_addresses}. "
            "Please specify the one to connect to by setting `address`.")
        sys.exit(1)
    elif not redis_addresses:
        raise ConnectionError(
            "Could not find any running Ray instance. "
            "Please specify the one to connect to by setting `address`.")
    return redis_addresses.pop()


def get_address_info_from_redis_helper(redis_address,
                                       node_ip_address,
                                       redis_password=None):
    redis_ip_address, redis_port = redis_address.split(":")
    # Get node table from global state accessor.
    global_state = ray.state.GlobalState()
    global_state._initialize_global_state(redis_address, redis_password)
    client_table = global_state.node_table()
    if len(client_table) == 0:
        raise RuntimeError(
            "Redis has started but no raylets have registered yet.")

    relevant_client = None
    for client_info in client_table:
        client_node_ip_address = client_info["NodeManagerAddress"]
        if (client_node_ip_address == node_ip_address
                or (client_node_ip_address == "127.0.0.1"
                    and redis_ip_address == get_node_ip_address())
                or client_node_ip_address == redis_ip_address):
            relevant_client = client_info
            break
    if relevant_client is None:
        raise RuntimeError(
            f"This node has an IP address of {node_ip_address}, and Ray "
            "expects this IP address to be either the Redis address or one of"
            f" the Raylet addresses. Connected to Redis at {redis_address} and"
            " found raylets at "
            f"{', '.join(c['NodeManagerAddress'] for c in client_table)} but "
            f"none of these match this node's IP {node_ip_address}. Are any of"
            " these actually a different IP address for the same node?"
            "You might need to provide --node-ip-address to specify the IP "
            "address that the head should use when sending to this node.")

    return {
        "object_store_address": relevant_client["ObjectStoreSocketName"],
        "raylet_socket_name": relevant_client["RayletSocketName"],
        "node_manager_port": relevant_client["NodeManagerPort"],
    }


def get_address_info_from_redis(redis_address,
                                node_ip_address,
                                num_retries=5,
                                redis_password=None):
    counter = 0
    while True:
        try:
            return get_address_info_from_redis_helper(
                redis_address, node_ip_address, redis_password=redis_password)
        except Exception:
            if counter == num_retries:
                raise
            # Some of the information may not be in Redis yet, so wait a little
            # bit.
            logger.warning(
                "Some processes that the driver needs to connect to have "
                "not registered with Redis, so retrying. Have you run "
                "'ray start' on this node?")
            time.sleep(1)
        counter += 1


def get_webui_url_from_redis(redis_client):
    webui_url = redis_client.hmget("webui", "url")[0]
    return ray.utils.decode(webui_url) if webui_url is not None else None


def remaining_processes_alive():
    """See if the remaining processes are alive or not.

    Note that this ignores processes that have been explicitly killed,
    e.g., via a command like node.kill_raylet().

    Returns:
        True if the remaining processes started by ray.init() are alive and
            False otherwise.

    Raises:
        Exception: An exception is raised if the processes were not started by
            ray.init().
    """
    if ray.worker._global_node is None:
        raise RuntimeError("This process is not in a position to determine "
                           "whether all processes are alive or not.")
    return ray.worker._global_node.remaining_processes_alive()


def validate_redis_address(address):
    """Validates address parameter.

    Returns:
        redis_address: string containing the full <host:port> address.
        redis_ip: string representing the host portion of the address.
        redis_port: integer representing the port portion of the address.
    """

    if address == "auto":
        address = find_redis_address_or_die()
    redis_address = address_to_ip(address)

    redis_address_parts = redis_address.split(":")
    if len(redis_address_parts) != 2:
        raise ValueError("Malformed address. Expected '<host>:<port>'.")
    redis_ip = redis_address_parts[0]
    try:
        redis_port = int(redis_address_parts[1])
    except ValueError:
        raise ValueError("Malformed address port. Must be an integer.")
    if redis_port < 1024 or redis_port > 65535:
        raise ValueError("Invalid address port. Must "
                         "be between 1024 and 65535.")

    return redis_address, redis_ip, redis_port


def address_to_ip(address):
    """Convert a hostname to a numerical IP addresses in an address.

    This should be a no-op if address already contains an actual numerical IP
    address.

    Args:
        address: This can be either a string containing a hostname (or an IP
            address) and a port or it can be just an IP address.

    Returns:
        The same address but with the hostname replaced by a numerical IP
            address.
    """
    address_parts = address.split(":")
    ip_address = socket.gethostbyname(address_parts[0])
    # Make sure localhost isn't resolved to the loopback ip
    if ip_address == "127.0.0.1":
        ip_address = get_node_ip_address()
    return ":".join([ip_address] + address_parts[1:])


def get_node_ip_address(address="8.8.8.8:53"):
    """IP address by which the local node can be reached *from* the `address`.

    Args:
        address (str): The IP address and port of any known live service on the
            network you care about.

    Returns:
        The IP address by which the local node can be reached from the address.
    """
    if ray.worker._global_node is not None:
        return ray.worker._global_node.node_ip_address

    ip_address, port = address.split(":")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # This command will raise an exception if there is no internet
        # connection.
        s.connect((ip_address, int(port)))
        node_ip_address = s.getsockname()[0]
    except OSError as e:
        node_ip_address = "127.0.0.1"
        # [Errno 101] Network is unreachable
        if e.errno == errno.ENETUNREACH:
            try:
                # try get node ip address from host name
                host_name = socket.getfqdn(socket.gethostname())
                node_ip_address = socket.gethostbyname(host_name)
            except Exception:
                pass
    finally:
        s.close()

    return node_ip_address


def create_redis_client(redis_address, password=None):
    """Create a Redis client.

    Args:
        The IP address, port, and password of the Redis server.

    Returns:
        A Redis client.
    """
    redis_ip_address, redis_port = redis_address.split(":")
    # For this command to work, some other client (on the same machine
    # as Redis) must have run "CONFIG SET protected-mode no".
    return redis.StrictRedis(
        host=redis_ip_address, port=int(redis_port), password=password)


def start_ray_process(command,
                      process_type,
                      fate_share,
                      env_updates=None,
                      cwd=None,
                      use_valgrind=False,
                      use_gdb=False,
                      use_valgrind_profiler=False,
                      use_perftools_profiler=False,
                      use_tmux=False,
                      stdout_file=None,
                      stderr_file=None,
                      pipe_stdin=False):
    """Start one of the Ray processes.

    TODO(rkn): We need to figure out how these commands interact. For example,
    it may only make sense to start a process in gdb if we also start it in
    tmux. Similarly, certain combinations probably don't make sense, like
    simultaneously running the process in valgrind and the profiler.

    Args:
        command (List[str]): The command to use to start the Ray process.
        process_type (str): The type of the process that is being started
            (e.g., "raylet").
        fate_share: If true, the child will be killed if its parent (us) dies.
            True must only be passed after detection of this functionality.
        env_updates (dict): A dictionary of additional environment variables to
            run the command with (in addition to the caller's environment
            variables).
        cwd (str): The directory to run the process in.
        use_valgrind (bool): True if we should start the process in valgrind.
        use_gdb (bool): True if we should start the process in gdb.
        use_valgrind_profiler (bool): True if we should start the process in
            the valgrind profiler.
        use_perftools_profiler (bool): True if we should profile the process
            using perftools.
        use_tmux (bool): True if we should start the process in tmux.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        pipe_stdin: If true, subprocess.PIPE will be passed to the process as
            stdin.

    Returns:
        Information about the process that was started including a handle to
            the process that was started.
    """
    # Detect which flags are set through environment variables.
    valgrind_env_var = f"RAY_{process_type.upper()}_VALGRIND"
    if os.environ.get(valgrind_env_var) == "1":
        logger.info("Detected environment variable '%s'.", valgrind_env_var)
        use_valgrind = True
    valgrind_profiler_env_var = f"RAY_{process_type.upper()}_VALGRIND_PROFILER"
    if os.environ.get(valgrind_profiler_env_var) == "1":
        logger.info("Detected environment variable '%s'.",
                    valgrind_profiler_env_var)
        use_valgrind_profiler = True
    perftools_profiler_env_var = (f"RAY_{process_type.upper()}"
                                  "_PERFTOOLS_PROFILER")
    if os.environ.get(perftools_profiler_env_var) == "1":
        logger.info("Detected environment variable '%s'.",
                    perftools_profiler_env_var)
        use_perftools_profiler = True
    tmux_env_var = f"RAY_{process_type.upper()}_TMUX"
    if os.environ.get(tmux_env_var) == "1":
        logger.info("Detected environment variable '%s'.", tmux_env_var)
        use_tmux = True
    gdb_env_var = f"RAY_{process_type.upper()}_GDB"
    if os.environ.get(gdb_env_var) == "1":
        logger.info("Detected environment variable '%s'.", gdb_env_var)
        use_gdb = True

    if sum([
            use_gdb,
            use_valgrind,
            use_valgrind_profiler,
            use_perftools_profiler,
    ]) > 1:
        raise ValueError(
            "At most one of the 'use_gdb', 'use_valgrind', "
            "'use_valgrind_profiler', and 'use_perftools_profiler' flags can "
            "be used at a time.")
    if env_updates is None:
        env_updates = {}
    if not isinstance(env_updates, dict):
        raise ValueError("The 'env_updates' argument must be a dictionary.")

    modified_env = os.environ.copy()
    modified_env.update(env_updates)

    if use_gdb:
        if not use_tmux:
            raise ValueError(
                "If 'use_gdb' is true, then 'use_tmux' must be true as well.")

        # TODO(suquark): Any better temp file creation here?
        gdb_init_path = os.path.join(ray.utils.get_ray_temp_dir(),
                                     f"gdb_init_{process_type}_{time.time()}")
        ray_process_path = command[0]
        ray_process_args = command[1:]
        run_args = " ".join(["'{}'".format(arg) for arg in ray_process_args])
        with open(gdb_init_path, "w") as gdb_init_file:
            gdb_init_file.write(f"run {run_args}")
        command = ["gdb", ray_process_path, "-x", gdb_init_path]

    if use_valgrind:
        command = [
            "valgrind",
            "--track-origins=yes",
            "--leak-check=full",
            "--show-leak-kinds=all",
            "--leak-check-heuristics=stdstring",
            "--error-exitcode=1",
        ] + command

    if use_valgrind_profiler:
        command = ["valgrind", "--tool=callgrind"] + command

    if use_perftools_profiler:
        modified_env["LD_PRELOAD"] = os.environ["PERFTOOLS_PATH"]
        modified_env["CPUPROFILE"] = os.environ["PERFTOOLS_LOGFILE"]

    if use_tmux:
        # The command has to be created exactly as below to ensure that it
        # works on all versions of tmux. (Tested with tmux 1.8-5, travis'
        # version, and tmux 2.1)
        command = ["tmux", "new-session", "-d", f"{' '.join(command)}"]

    if fate_share:
        assert ray.utils.detect_fate_sharing_support(), (
            "kernel-level fate-sharing must only be specified if "
            "detect_fate_sharing_support() has returned True")

    def preexec_fn():
        import signal
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
        if fate_share and sys.platform.startswith("linux"):
            ray.utils.set_kill_on_parent_death_linux()

    win32_fate_sharing = fate_share and sys.platform == "win32"
    # With Windows fate-sharing, we need special care:
    # The process must be added to the job before it is allowed to execute.
    # Otherwise, there's a race condition: the process might spawn children
    # before the process itself is assigned to the job.
    # After that point, its children will not be added to the job anymore.
    CREATE_SUSPENDED = 0x00000004  # from Windows headers

    process = ConsolePopen(
        command,
        env=modified_env,
        cwd=cwd,
        stdout=stdout_file,
        stderr=stderr_file,
        stdin=subprocess.PIPE if pipe_stdin else None,
        preexec_fn=preexec_fn if sys.platform != "win32" else None,
        creationflags=CREATE_SUSPENDED if win32_fate_sharing else 0)

    if win32_fate_sharing:
        try:
            ray.utils.set_kill_child_on_death_win32(process)
            psutil.Process(process.pid).resume()
        except (psutil.Error, OSError):
            process.kill()
            raise

    def _get_stream_name(stream):
        if stream is not None:
            try:
                return stream.name
            except AttributeError:
                return str(stream)
        return None

    return ProcessInfo(
        process=process,
        stdout_file=_get_stream_name(stdout_file),
        stderr_file=_get_stream_name(stderr_file),
        use_valgrind=use_valgrind,
        use_gdb=use_gdb,
        use_valgrind_profiler=use_valgrind_profiler,
        use_perftools_profiler=use_perftools_profiler,
        use_tmux=use_tmux)


def wait_for_redis_to_start(redis_ip_address, redis_port, password=None):
    """Wait for a Redis server to be available.

    This is accomplished by creating a Redis client and sending a random
    command to the server until the command gets through.

    Args:
        redis_ip_address (str): The IP address of the redis server.
        redis_port (int): The port of the redis server.
        password (str): The password of the redis server.

    Raises:
        Exception: An exception is raised if we could not connect with Redis.
    """
    redis_client = redis.StrictRedis(
        host=redis_ip_address, port=redis_port, password=password)
    # Wait for the Redis server to start.
    num_retries = ray_constants.START_REDIS_WAIT_RETRIES
    delay = 0.001
    for i in range(num_retries):
        try:
            # Run some random command and see if it worked.
            logger.debug(
                "Waiting for redis server at {}:{} to respond...".format(
                    redis_ip_address, redis_port))
            redis_client.client_list()
        # If the Redis service is delayed getting set up for any reason, we may
        # get a redis.ConnectionError: Error 111 connecting to host:port.
        # Connection refused.
        # Unfortunately, redis.ConnectionError is also the base class of
        # redis.AuthenticationError. We *don't* want to obscure a
        # redis.AuthenticationError, because that indicates the user provided a
        # bad password. Thus a double except clause to ensure a
        # redis.AuthenticationError isn't trapped here.
        except redis.AuthenticationError as authEx:
            raise RuntimeError("Unable to connect to Redis at {}:{}.".format(
                redis_ip_address, redis_port)) from authEx
        except redis.ConnectionError as connEx:
            if i >= num_retries - 1:
                raise RuntimeError(
                    f"Unable to connect to Redis at {redis_ip_address}:"
                    f"{redis_port} after {num_retries} retries. Check that "
                    f"{redis_ip_address}:{redis_port} is reachable from this "
                    "machine. If it is not, your firewall may be blocking "
                    "this port. If the problem is a flaky connection, try "
                    "setting the environment variable "
                    "`RAY_START_REDIS_WAIT_RETRIES` to increase the number of"
                    " attempts to ping the Redis server.") from connEx
            # Wait a little bit.
            time.sleep(delay)
            delay *= 2
        else:
            break
    else:
        raise RuntimeError(
            f"Unable to connect to Redis (after {num_retries} retries). "
            "If the Redis instance is on a different machine, check that "
            "your firewall and relevant Ray ports are configured properly. "
            "You can also set the environment variable "
            "`RAY_START_REDIS_WAIT_RETRIES` to increase the number of "
            "attempts to ping the Redis server.")


def _compute_version_info():
    """Compute the versions of Python, and Ray.

    Returns:
        A tuple containing the version information.
    """
    ray_version = ray.__version__
    python_version = ".".join(map(str, sys.version_info[:3]))
    return ray_version, python_version


def _put_version_info_in_redis(redis_client):
    """Store version information in Redis.

    This will be used to detect if workers or drivers are started using
    different versions of Python, or Ray.

    Args:
        redis_client: A client for the primary Redis shard.
    """
    redis_client.set("VERSION_INFO", json.dumps(_compute_version_info()))


def check_version_info(redis_client):
    """Check if various version info of this process is correct.

    This will be used to detect if workers or drivers are started using
    different versions of Python, or Ray. If the version
    information is not present in Redis, then no check is done.

    Args:
        redis_client: A client for the primary Redis shard.

    Raises:
        Exception: An exception is raised if there is a version mismatch.
    """
    redis_reply = redis_client.get("VERSION_INFO")

    # Don't do the check if there is no version information in Redis. This
    # is to make it easier to do things like start the processes by hand.
    if redis_reply is None:
        return

    true_version_info = tuple(json.loads(ray.utils.decode(redis_reply)))
    version_info = _compute_version_info()
    if version_info != true_version_info:
        node_ip_address = get_node_ip_address()
        error_message = ("Version mismatch: The cluster was started with:\n"
                         "    Ray: " + true_version_info[0] + "\n"
                         "    Python: " + true_version_info[1] + "\n"
                         "This process on node " + node_ip_address +
                         " was started with:" + "\n"
                         "    Ray: " + version_info[0] + "\n"
                         "    Python: " + version_info[1] + "\n")
        if version_info[:2] != true_version_info[:2]:
            raise RuntimeError(error_message)
        else:
            logger.warning(error_message)


def start_reaper(fate_share=None):
    """Start the reaper process.

    This is a lightweight process that simply
    waits for its parent process to die and then terminates its own
    process group. This allows us to ensure that ray processes are always
    terminated properly so long as that process itself isn't SIGKILLed.

    Returns:
        ProcessInfo for the process that was started.
    """
    # Make ourselves a process group leader so that the reaper can clean
    # up other ray processes without killing the process group of the
    # process that started us.
    try:
        if sys.platform != "win32":
            os.setpgrp()
    except OSError as e:
        errcode = e.errno
        if errcode == errno.EPERM and os.getpgrp() == os.getpid():
            # Nothing to do; we're already a session leader.
            pass
        else:
            logger.warning("setpgrp failed, processes may not be "
                           "cleaned up properly: {}.".format(e))
            # Don't start the reaper in this case as it could result in killing
            # other user processes.
            return None

    reaper_filepath = os.path.join(RAY_PATH, "ray_process_reaper.py")
    command = [sys.executable, "-u", reaper_filepath]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_REAPER,
        pipe_stdin=True,
        fate_share=fate_share)
    return process_info


def start_redis(node_ip_address,
                redirect_files,
                resource_spec,
                port=None,
                redis_shard_ports=None,
                num_redis_shards=1,
                redis_max_clients=None,
                redirect_worker_output=False,
                password=None,
                fate_share=None):
    """Start the Redis global state store.

    Args:
        node_ip_address: The IP address of the current node. This is only used
            for recording the log filenames in Redis.
        redirect_files: The list of (stdout, stderr) file pairs.
        resource_spec (ResourceSpec): Resources for the node.
        port (int): If provided, the primary Redis shard will be started on
            this port.
        redis_shard_ports: A list of the ports to use for the non-primary Redis
            shards.
        num_redis_shards (int): If provided, the number of Redis shards to
            start, in addition to the primary one. The default value is one
            shard.
        redis_max_clients: If this is provided, Ray will attempt to configure
            Redis with this maxclients number.
        redirect_worker_output (bool): True if worker output should be
            redirected to a file and false otherwise. Workers will have access
            to this value when they start up.
        password (str): Prevents external clients without the password
            from connecting to Redis if provided.

    Returns:
        A tuple of the address for the primary Redis shard, a list of
            addresses for the remaining shards, and the processes that were
            started.
    """

    if len(redirect_files) != 1 + num_redis_shards:
        raise ValueError("The number of redirect file pairs should be equal "
                         "to the number of redis shards (including the "
                         "primary shard) we will start.")
    if redis_shard_ports is None:
        redis_shard_ports = num_redis_shards * [None]
    elif len(redis_shard_ports) != num_redis_shards:
        raise RuntimeError("The number of Redis shard ports does not match "
                           "the number of Redis shards.")

    processes = []

    redis_executable = REDIS_EXECUTABLE
    redis_modules = [REDIS_MODULE]

    redis_stdout_file, redis_stderr_file = redirect_files[0]
    # Start the primary Redis shard.
    port, p = _start_redis_instance(
        redis_executable,
        modules=redis_modules,
        port=port,
        password=password,
        redis_max_clients=redis_max_clients,
        # Below we use None to indicate no limit on the memory of the
        # primary Redis shard.
        redis_max_memory=None,
        stdout_file=redis_stdout_file,
        stderr_file=redis_stderr_file,
        fate_share=fate_share)
    processes.append(p)
    redis_address = address(node_ip_address, port)

    # Register the number of Redis shards in the primary shard, so that clients
    # know how many redis shards to expect under RedisShards.
    primary_redis_client = redis.StrictRedis(
        host=node_ip_address, port=port, password=password)
    primary_redis_client.set("NumRedisShards", str(num_redis_shards))

    # Put the redirect_worker_output bool in the Redis shard so that workers
    # can access it and know whether or not to redirect their output.
    primary_redis_client.set("RedirectOutput", 1
                             if redirect_worker_output else 0)

    # Init job counter to GCS.
    primary_redis_client.set("JobCounter", 0)

    # Store version information in the primary Redis shard.
    _put_version_info_in_redis(primary_redis_client)

    # Calculate the redis memory.
    assert resource_spec.resolved()
    redis_max_memory = resource_spec.redis_max_memory

    # Start other Redis shards. Each Redis shard logs to a separate file,
    # prefixed by "redis-<shard number>".
    redis_shards = []
    for i in range(num_redis_shards):
        redis_stdout_file, redis_stderr_file = redirect_files[i + 1]
        redis_executable = REDIS_EXECUTABLE
        redis_modules = [REDIS_MODULE]

        redis_shard_port, p = _start_redis_instance(
            redis_executable,
            modules=redis_modules,
            port=redis_shard_ports[i],
            password=password,
            redis_max_clients=redis_max_clients,
            redis_max_memory=redis_max_memory,
            stdout_file=redis_stdout_file,
            stderr_file=redis_stderr_file,
            fate_share=fate_share)
        processes.append(p)

        shard_address = address(node_ip_address, redis_shard_port)
        redis_shards.append(shard_address)
        # Store redis shard information in the primary redis shard.
        primary_redis_client.rpush("RedisShards", shard_address)

    return redis_address, redis_shards, processes


def _start_redis_instance(executable,
                          modules,
                          port=None,
                          redis_max_clients=None,
                          num_retries=20,
                          stdout_file=None,
                          stderr_file=None,
                          password=None,
                          redis_max_memory=None,
                          fate_share=None):
    """Start a single Redis server.

    Notes:
        If "port" is not None, then we will only use this port and try
        only once. Otherwise, we will first try the default redis port,
        and if it is unavailable, we will try random ports with
        maximum retries of "num_retries".

    Args:
        executable (str): Full path of the redis-server executable.
        modules (list of str): A list of pathnames, pointing to the redis
            module(s) that will be loaded in this redis server.
        port (int): If provided, start a Redis server with this port.
        redis_max_clients: If this is provided, Ray will attempt to configure
            Redis with this maxclients number.
        num_retries (int): The number of times to attempt to start Redis. If a
            port is provided, this defaults to 1.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        redis_max_memory: The max amount of memory (in bytes) to allow redis
            to use, or None for no limit. Once the limit is exceeded, redis
            will start LRU eviction of entries.

    Returns:
        A tuple of the port used by Redis and ProcessInfo for the process that
            was started. If a port is passed in, then the returned port value
            is the same.

    Raises:
        Exception: An exception is raised if Redis could not be started.
    """
    assert os.path.isfile(executable)
    for module in modules:
        assert os.path.isfile(module)
    counter = 0
    if port is not None:
        # If a port is specified, then try only once to connect.
        # This ensures that we will use the given port.
        num_retries = 1
    else:
        port = ray_constants.DEFAULT_PORT

    load_module_args = []
    for module in modules:
        load_module_args += ["--loadmodule", module]

    while counter < num_retries:
        # Construct the command to start the Redis server.
        command = [executable]
        if password:
            if " " in password:
                raise ValueError("Spaces not permitted in redis password.")
            command += ["--requirepass", password]
        command += (
            ["--port", str(port), "--loglevel", "warning"] + load_module_args)
        process_info = start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_REDIS_SERVER,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            fate_share=fate_share)
        time.sleep(0.1)
        # Check if Redis successfully started (or at least if it the executable
        # did not exit within 0.1 seconds).
        if process_info.process.poll() is None:
            break
        port = new_port()
        counter += 1
    if counter == num_retries:
        raise RuntimeError("Couldn't start Redis. "
                           "Check log files: {} {}".format(
                               stdout_file.name if stdout_file is not None else
                               "<stdout>", stderr_file.name
                               if stdout_file is not None else "<stderr>"))

    # Create a Redis client just for configuring Redis.
    redis_client = redis.StrictRedis(
        host="127.0.0.1", port=port, password=password)
    # Wait for the Redis server to start.
    wait_for_redis_to_start("127.0.0.1", port, password=password)
    # Configure Redis to generate keyspace notifications. TODO(rkn): Change
    # this to only generate notifications for the export keys.
    redis_client.config_set("notify-keyspace-events", "Kl")

    # Configure Redis to not run in protected mode so that processes on other
    # hosts can connect to it. TODO(rkn): Do this in a more secure way.
    redis_client.config_set("protected-mode", "no")

    # Discard old task and object metadata.
    if redis_max_memory is not None:
        redis_client.config_set("maxmemory", str(redis_max_memory))
        redis_client.config_set("maxmemory-policy", "allkeys-lru")
        redis_client.config_set("maxmemory-samples", "10")
        logger.debug("Starting Redis shard with {} GB max memory.".format(
            round(redis_max_memory / 1e9, 2)))

    # If redis_max_clients is provided, attempt to raise the number of maximum
    # number of Redis clients.
    if redis_max_clients is not None:
        redis_client.config_set("maxclients", str(redis_max_clients))
    elif resource is not None:
        # If redis_max_clients is not provided, determine the current ulimit.
        # We will use this to attempt to raise the maximum number of Redis
        # clients.
        current_max_clients = int(
            redis_client.config_get("maxclients")["maxclients"])
        # The below command should be the same as doing ulimit -n.
        ulimit_n = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        # The quantity redis_client_buffer appears to be the required buffer
        # between the maximum number of redis clients and ulimit -n. That is,
        # if ulimit -n returns 10000, then we can set maxclients to
        # 10000 - redis_client_buffer.
        redis_client_buffer = 32
        if current_max_clients < ulimit_n - redis_client_buffer:
            redis_client.config_set("maxclients", 10000)

    # Increase the hard and soft limits for the redis client pubsub buffer to
    # 128MB. This is a hack to make it less likely for pubsub messages to be
    # dropped and for pubsub connections to therefore be killed.
    cur_config = (redis_client.config_get("client-output-buffer-limit")[
        "client-output-buffer-limit"])
    cur_config_list = cur_config.split()
    assert len(cur_config_list) == 12
    cur_config_list[8:] = ["pubsub", "134217728", "134217728", "60"]
    redis_client.config_set("client-output-buffer-limit",
                            " ".join(cur_config_list))
    # Put a time stamp in Redis to indicate when it was started.
    redis_client.set("redis_start_time", time.time())
    return port, process_info


def start_log_monitor(redis_address,
                      logs_dir,
                      stdout_file=None,
                      stderr_file=None,
                      redis_password=None,
                      fate_share=None):
    """Start a log monitor process.

    Args:
        redis_address (str): The address of the Redis instance.
        logs_dir (str): The directory of logging files.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        redis_password (str): The password of the redis server.

    Returns:
        ProcessInfo for the process that was started.
    """
    log_monitor_filepath = os.path.join(RAY_PATH, "log_monitor.py")
    command = [
        sys.executable,
        "-u",
        log_monitor_filepath,
        f"--redis-address={redis_address}",
        f"--logs-dir={logs_dir}",
    ]
    if redis_password:
        command += ["--redis-password", redis_password]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_LOG_MONITOR,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share)
    return process_info


def start_dashboard(require_dashboard,
                    host,
                    redis_address,
                    temp_dir,
                    logdir,
                    port=ray_constants.DEFAULT_DASHBOARD_PORT,
                    stdout_file=None,
                    stderr_file=None,
                    redis_password=None,
                    fate_share=None):
    """Start a dashboard process.

    Args:
        require_dashboard (bool): If true, this will raise an exception if we
            fail to start the dashboard. Otherwise it will print a warning if
            we fail to start the dashboard.
        host (str): The host to bind the dashboard web server to.
        port (str): The port to bind the dashboard web server to.
            Defaults to 8265.
        redis_address (str): The address of the Redis instance.
        temp_dir (str): The temporary directory used for log files and
            information for this Ray session.
        logdir (str): The log directory used to generate dashboard log.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        redis_password (str): The password of the redis server.

    Returns:
        ProcessInfo for the process that was started.
    """
    port_test_socket = socket.socket()
    port_test_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if port == ray_constants.DEFAULT_DASHBOARD_PORT:
        while True:
            try:
                port_test_socket.bind(("127.0.0.1", port))
                port_test_socket.close()
                break
            except socket.error:
                port += 1
    else:
        try:
            port_test_socket.bind(("127.0.0.1", port))
            port_test_socket.close()
        except socket.error:
            raise ValueError(
                f"The given dashboard port {port} is already in use")

    dashboard_dir = "new_dashboard"
    dashboard_filepath = os.path.join(RAY_PATH, dashboard_dir, "dashboard.py")
    command = [
        sys.executable,
        "-u",
        dashboard_filepath,
        f"--host={host}",
        f"--port={port}",
        f"--redis-address={redis_address}",
        f"--temp-dir={temp_dir}",
        f"--log-dir={logdir}",
    ]

    if redis_password:
        command += ["--redis-password", redis_password]

    dashboard_dependencies_present = True
    try:
        import aiohttp  # noqa: F401
        import grpc  # noqa: F401
    except ImportError:
        dashboard_dependencies_present = False
        warning_message = (
            "Failed to start the dashboard. The dashboard requires Python 3 "
            "as well as 'pip install aiohttp grpcio'.")
        if require_dashboard:
            raise ImportError(warning_message)
        else:
            logger.warning(warning_message)
    if dashboard_dependencies_present:
        process_info = start_ray_process(
            command,
            ray_constants.PROCESS_TYPE_DASHBOARD,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            fate_share=fate_share)

        dashboard_url = (
            f"{host if host != '0.0.0.0' else get_node_ip_address()}:{port}")

        logger.info("View the Ray dashboard at {}{}http://{}{}{}".format(
            colorama.Style.BRIGHT, colorama.Fore.GREEN, dashboard_url,
            colorama.Fore.RESET, colorama.Style.NORMAL))

        return dashboard_url, process_info
    else:
        return None, None


def start_gcs_server(redis_address,
                     stdout_file=None,
                     stderr_file=None,
                     redis_password=None,
                     config=None,
                     fate_share=None,
                     gcs_server_port=None,
                     metrics_agent_port=None,
                     node_ip_address=None):
    """Start a gcs server.
    Args:
        redis_address (str): The address that the Redis server is listening on.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        redis_password (str): The password of the redis server.
        config (dict|None): Optional configuration that will
            override defaults in RayConfig.
        gcs_server_port (int): Port number of the gcs server.
        metrics_agent_port(int): The port where metrics agent is bound to.
        node_ip_address(str): IP Address of a node where gcs server starts.
    Returns:
        ProcessInfo for the process that was started.
    """
    gcs_ip_address, gcs_port = redis_address.split(":")
    redis_password = redis_password or ""
    config_str = serialize_config(config)
    if gcs_server_port is None:
        gcs_server_port = 0

    command = [
        GCS_SERVER_EXECUTABLE,
        f"--redis_address={gcs_ip_address}",
        f"--redis_port={gcs_port}",
        f"--config_list={config_str}",
        f"--gcs_server_port={gcs_server_port}",
        f"--metrics-agent-port={metrics_agent_port}",
        f"--node-ip-address={node_ip_address}",
    ]
    if redis_password:
        command += [f"--redis_password={redis_password}"]
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_GCS_SERVER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share)
    return process_info


def start_raylet(redis_address,
                 node_ip_address,
                 node_manager_port,
                 raylet_name,
                 plasma_store_name,
                 worker_path,
                 temp_dir,
                 session_dir,
                 log_dir,
                 resource_spec,
                 plasma_directory,
                 object_store_memory,
                 min_worker_port=None,
                 max_worker_port=None,
                 worker_port_list=None,
                 object_manager_port=None,
                 redis_password=None,
                 metrics_agent_port=None,
                 metrics_export_port=None,
                 use_valgrind=False,
                 use_profiler=False,
                 stdout_file=None,
                 stderr_file=None,
                 config=None,
                 java_worker_options=None,
                 load_code_from_local=False,
                 huge_pages=False,
                 fate_share=None,
                 socket_to_use=None,
                 head_node=False,
                 start_initial_python_workers_for_first_job=False,
                 code_search_path=None):
    """Start a raylet, which is a combined local scheduler and object manager.

    Args:
        redis_address (str): The address of the primary Redis server.
        node_ip_address (str): The IP address of this node.
        node_manager_port(int): The port to use for the node manager. This must
            not be 0.
        raylet_name (str): The name of the raylet socket to create.
        plasma_store_name (str): The name of the plasma store socket to connect
             to.
        worker_path (str): The path of the Python file that new worker
            processes will execute.
        temp_dir (str): The path of the temporary directory Ray will use.
        session_dir (str): The path of this session.
        log_dir (str): The path of the dir where log files are created.
        resource_spec (ResourceSpec): Resources for this raylet.
        object_manager_port: The port to use for the object manager. If this is
            None, then the object manager will choose its own port.
        min_worker_port (int): The lowest port number that workers will bind
            on. If not set, random ports will be chosen.
        max_worker_port (int): The highest port number that workers will bind
            on. If set, min_worker_port must also be set.
        redis_password: The password to use when connecting to Redis.
        metrics_agent_port(int): The port where metrics agent is bound to.
        metrics_export_port(int): The port at which metrics are exposed to.
        use_valgrind (bool): True if the raylet should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the raylet should be started inside
            a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        config (dict|None): Optional Raylet configuration that will
            override defaults in RayConfig.
        java_worker_options (list): The command options for Java worker.
        code_search_path (list): Code search path for worker. code_search_path
            is added to worker command in non-multi-tenancy mode and job_config
            in multi-tenancy mode.
    Returns:
        ProcessInfo for the process that was started.
    """
    # The caller must provide a node manager port so that we can correctly
    # populate the command to start a worker.
    assert node_manager_port is not None and node_manager_port != 0
    config_str = serialize_config(config)

    if use_valgrind and use_profiler:
        raise ValueError("Cannot use valgrind and profiler at the same time.")

    assert resource_spec.resolved()
    num_initial_workers = resource_spec.num_cpus
    static_resources = resource_spec.to_resource_dict()

    # Limit the number of workers that can be started in parallel by the
    # raylet. However, make sure it is at least 1.
    num_cpus_static = static_resources.get("CPU", 0)
    maximum_startup_concurrency = max(
        1, min(multiprocessing.cpu_count(), num_cpus_static))

    # Format the resource argument in a form like 'CPU,1.0,GPU,0,Custom,3'.
    resource_argument = ",".join(
        ["{},{}".format(*kv) for kv in static_resources.items()])

    gcs_ip_address, gcs_port = redis_address.split(":")

    has_java_command = False
    if shutil.which("java") is not None:
        has_java_command = True

    ray_java_installed = False
    try:
        jars_dir = get_ray_jars_dir()
        if os.path.exists(jars_dir):
            ray_java_installed = True
    except Exception:
        pass

    include_java = has_java_command and ray_java_installed
    if include_java is True:
        java_worker_command = build_java_worker_command(
            json.loads(java_worker_options) if java_worker_options else [],
            redis_address,
            node_manager_port,
            plasma_store_name,
            raylet_name,
            redis_password,
            session_dir,
            code_search_path,
        )
    else:
        java_worker_command = []

    if os.path.exists(DEFAULT_WORKER_EXECUTABLE):
        cpp_worker_command = build_cpp_worker_command(
            "",
            redis_address,
            node_manager_port,
            plasma_store_name,
            raylet_name,
            redis_password,
            session_dir,
        )
    else:
        cpp_worker_command = []

    # Create the command that the Raylet will use to start workers.
    start_worker_command = [
        sys.executable, worker_path, f"--node-ip-address={node_ip_address}",
        f"--node-manager-port={node_manager_port}",
        f"--object-store-name={plasma_store_name}",
        f"--raylet-name={raylet_name}", f"--redis-address={redis_address}",
        f"--config-list={config_str}", f"--temp-dir={temp_dir}",
        f"--metrics-agent-port={metrics_agent_port}"
    ]
    if code_search_path:
        start_worker_command.append(f"--code-search-path={code_search_path}")
    if redis_password:
        start_worker_command += [f"--redis-password={redis_password}"]

    # If the object manager port is None, then use 0 to cause the object
    # manager to choose its own port.
    if object_manager_port is None:
        object_manager_port = 0

    if min_worker_port is None:
        min_worker_port = 0

    if max_worker_port is None:
        max_worker_port = 0

    if code_search_path is not None and len(code_search_path) > 0:
        load_code_from_local = True

    if load_code_from_local:
        start_worker_command += ["--load-code-from-local"]

    # Create agent command
    agent_command = [
        sys.executable,
        "-u",
        os.path.join(RAY_PATH, "new_dashboard/agent.py"),
        f"--node-ip-address={node_ip_address}",
        f"--redis-address={redis_address}",
        f"--metrics-export-port={metrics_export_port}",
        f"--dashboard-agent-port={metrics_agent_port}",
        f"--node-manager-port={node_manager_port}",
        f"--object-store-name={plasma_store_name}",
        f"--raylet-name={raylet_name}",
        f"--temp-dir={temp_dir}",
        f"--log-dir={log_dir}",
    ]

    if redis_password is not None and len(redis_password) != 0:
        agent_command.append("--redis-password={}".format(redis_password))

    command = [
        RAYLET_EXECUTABLE,
        f"--raylet_socket_name={raylet_name}",
        f"--store_socket_name={plasma_store_name}",
        f"--object_manager_port={object_manager_port}",
        f"--min_worker_port={min_worker_port}",
        f"--max_worker_port={max_worker_port}",
        f"--node_manager_port={node_manager_port}",
        f"--node_ip_address={node_ip_address}",
        f"--redis_address={gcs_ip_address}",
        f"--redis_port={gcs_port}",
        f"--num_initial_workers={num_initial_workers}",
        f"--maximum_startup_concurrency={maximum_startup_concurrency}",
        f"--static_resource_list={resource_argument}",
        f"--config_list={config_str}",
        f"--python_worker_command={subprocess.list2cmdline(start_worker_command)}",  # noqa
        f"--java_worker_command={subprocess.list2cmdline(java_worker_command)}",  # noqa
        f"--cpp_worker_command={subprocess.list2cmdline(cpp_worker_command)}",  # noqa
        f"--redis_password={redis_password or ''}",
        f"--temp_dir={temp_dir}",
        f"--session_dir={session_dir}",
        f"--metrics-agent-port={metrics_agent_port}",
        f"--metrics_export_port={metrics_export_port}",
    ]
    if worker_port_list is not None:
        command.append(f"--worker_port_list={worker_port_list}")
    if start_initial_python_workers_for_first_job:
        command.append("--num_initial_python_workers_for_first_job={}".format(
            resource_spec.num_cpus))
    command.append("--agent_command={}".format(
        subprocess.list2cmdline(agent_command)))
    if config.get("plasma_store_as_thread"):
        # command related to the plasma store
        command += [
            f"--object_store_memory={object_store_memory}",
            f"--plasma_directory={plasma_directory}",
        ]
        if huge_pages:
            command.append("--huge_pages")
    if socket_to_use:
        socket_to_use.close()
    if head_node:
        command.append("--head_node")
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_RAYLET,
        use_valgrind=use_valgrind,
        use_gdb=False,
        use_valgrind_profiler=use_profiler,
        use_perftools_profiler=("RAYLET_PERFTOOLS_PATH" in os.environ),
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share)

    return process_info


def get_ray_jars_dir():
    """Return a directory where all ray-related jars and
      their dependencies locate."""
    current_dir = RAY_PATH
    jars_dir = os.path.abspath(os.path.join(current_dir, "jars"))
    if not os.path.exists(jars_dir):
        raise RuntimeError("Ray jars is not packaged into ray. "
                           "Please build ray with java enabled "
                           "(set env var RAY_INSTALL_JAVA=1)")
    return os.path.abspath(os.path.join(current_dir, "jars"))


def build_java_worker_command(java_worker_options, redis_address,
                              node_manager_port, plasma_store_name,
                              raylet_name, redis_password, session_dir,
                              code_search_path):
    """This method assembles the command used to start a Java worker.

    Args:
        java_worker_options (list): The command options for Java worker.
        redis_address (str): Redis address of GCS.
        plasma_store_name (str): The name of the plasma store socket to connect
           to.
        raylet_name (str): The name of the raylet socket to create.
        redis_password (str): The password of connect to redis.
        session_dir (str): The path of this session.
        code_search_path (list): Teh job code search path.
    Returns:
        The command string for starting Java worker.
    """
    pairs = []
    if redis_address is not None:
        pairs.append(("ray.address", redis_address))
    pairs.append(("ray.raylet.node-manager-port", node_manager_port))

    if plasma_store_name is not None:
        pairs.append(("ray.object-store.socket-name", plasma_store_name))

    if raylet_name is not None:
        pairs.append(("ray.raylet.socket-name", raylet_name))

    if redis_password is not None:
        pairs.append(("ray.redis.password", redis_password))

    pairs.append(("ray.home", RAY_HOME))
    pairs.append(("ray.logging.dir", os.path.join(session_dir, "logs")))
    pairs.append(("ray.session-dir", session_dir))
    pairs.append(("ray.job.code-search-path", code_search_path))
    command = ["java"] + ["-D{}={}".format(*pair) for pair in pairs]

    command += ["RAY_WORKER_RAYLET_CONFIG_PLACEHOLDER"]

    # Add ray jars path to java classpath
    ray_jars = os.path.join(get_ray_jars_dir(), "*")
    if java_worker_options is None:
        options = []
    else:
        assert isinstance(java_worker_options, (tuple, list))
        options = list(java_worker_options)
    cp_index = -1
    for i in range(len(options)):
        option = options[i]
        if option == "-cp" or option == "-classpath":
            cp_index = i + 1
            break
    if cp_index != -1:
        options[cp_index] = options[cp_index] + os.pathsep + ray_jars
    else:
        options = ["-cp", ray_jars] + options
    # Put `java_worker_options` in the last, so it can overwrite the
    # above options.
    command += options

    command += ["RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER"]
    command += ["io.ray.runtime.runner.worker.DefaultWorker"]

    return command


def build_cpp_worker_command(
        cpp_worker_options,
        redis_address,
        node_manager_port,
        plasma_store_name,
        raylet_name,
        redis_password,
        session_dir,
):
    """This method assembles the command used to start a CPP worker.

    Args:
        cpp_worker_options (list): The command options for CPP worker.
        redis_address (str): Redis address of GCS.
        plasma_store_name (str): The name of the plasma store socket to connect
           to.
        raylet_name (str): The name of the raylet socket to create.
        redis_password (str): The password of connect to redis.
        session_dir (str): The path of this session.
    Returns:
        The command string for starting CPP worker.
    """

    # TODO(Guyang Song): Remove the arg is_default_worker.
    # See `cluster_mode_test.cc` for why this workaround is currently needed
    # for C++ workers.
    command = [
        DEFAULT_WORKER_EXECUTABLE, plasma_store_name, raylet_name,
        str(node_manager_port), redis_address, redis_password, session_dir,
        "is_default_worker"
    ]

    return command


def determine_plasma_store_config(object_store_memory,
                                  plasma_directory=None,
                                  huge_pages=False):
    """Figure out how to configure the plasma object store.

    This will determine which directory to use for the plasma store. On Linux,
    we will try to use /dev/shm unless the shared memory file system is too
    small, in which case we will fall back to /tmp. If any of the object store
    memory or plasma directory parameters are specified by the user, then those
    values will be preserved.

    Args:
        object_store_memory (int): The objec store memory to use.
        plasma_directory (str): The user-specified plasma directory parameter.
        huge_pages (bool): The user-specified huge pages parameter.

    Returns:
        The plasma directory to use. If it is specified by the user, then that
            value will be preserved.
    """
    if not isinstance(object_store_memory, int):
        object_store_memory = int(object_store_memory)

    if huge_pages and not (sys.platform == "linux"
                           or sys.platform == "linux2"):
        raise ValueError("The huge_pages argument is only supported on "
                         "Linux.")

    system_memory = ray.utils.get_system_memory()

    # Determine which directory to use. By default, use /tmp on MacOS and
    # /dev/shm on Linux, unless the shared-memory file system is too small,
    # in which case we default to /tmp on Linux.
    if plasma_directory is None:
        if sys.platform == "linux" or sys.platform == "linux2":
            shm_avail = ray.utils.get_shared_memory_bytes()
            # Compare the requested memory size to the memory available in
            # /dev/shm.
            if shm_avail > object_store_memory:
                plasma_directory = "/dev/shm"
            else:
                plasma_directory = ray.utils.get_user_temp_dir()
                logger.warning(
                    "WARNING: The object store is using {} instead of "
                    "/dev/shm because /dev/shm has only {} bytes available. "
                    "This may slow down performance! You may be able to free "
                    "up space by deleting files in /dev/shm or terminating "
                    "any running plasma_store_server processes. If you are "
                    "inside a Docker container, you may need to pass an "
                    "argument with the flag '--shm-size' to 'docker run'.".
                    format(ray.utils.get_user_temp_dir(), shm_avail))
        else:
            plasma_directory = ray.utils.get_user_temp_dir()

        # Do some sanity checks.
        if object_store_memory > system_memory:
            raise ValueError(
                "The requested object store memory size is greater "
                "than the total available memory.")
    else:
        plasma_directory = os.path.abspath(plasma_directory)
        logger.info("object_store_memory is not verified when "
                    "plasma_directory is set.")

    if not os.path.isdir(plasma_directory):
        raise ValueError(f"The file {plasma_directory} does not "
                         "exist or is not a directory.")

    if huge_pages and plasma_directory is None:
        raise ValueError("If huge_pages is True, then the "
                         "plasma_directory argument must be provided.")

    if object_store_memory < ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES:
        raise ValueError("Attempting to cap object store memory usage at {} "
                         "bytes, but the minimum allowed is {} bytes.".format(
                             object_store_memory,
                             ray_constants.OBJECT_STORE_MINIMUM_MEMORY_BYTES))

    # Print the object store memory using two decimal places.
    logger.debug(
        "Determine to start the Plasma object store with {} GB memory "
        "using {}.".format(
            round(object_store_memory / 10**9, 2), plasma_directory))

    return plasma_directory, object_store_memory


def start_plasma_store(resource_spec,
                       plasma_directory,
                       object_store_memory,
                       plasma_store_socket_name,
                       stdout_file=None,
                       stderr_file=None,
                       keep_idle=False,
                       huge_pages=False,
                       fate_share=None,
                       use_valgrind=False):
    """This method starts an object store process.

    Args:
        resource_spec (ResourceSpec): Resources for the node.
        plasma_store_socket_name (str): The path/name of the plasma
            store socket.
        stdout_file: A file handle opened for writing to redirect stdout
            to. If no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr
            to. If no redirection should happen, then this should be None.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: Boolean flag indicating whether to start the Object
            Store with hugetlbfs support. Requires plasma_directory.
        keep_idle: If True, run the plasma store as an idle placeholder.

    Returns:
        ProcessInfo for the process that was started.
    """
    # Start the Plasma store.
    if use_valgrind and RUN_PLASMA_STORE_PROFILER:
        raise ValueError("Cannot use valgrind and profiler at the same time.")

    assert resource_spec.resolved()

    command = [
        PLASMA_STORE_EXECUTABLE,
        "-s",
        plasma_store_socket_name,
        "-m",
        str(object_store_memory),
    ]
    if plasma_directory is not None:
        command += ["-d", plasma_directory]
    if huge_pages:
        command += ["-h"]
    if keep_idle:
        command.append("-z")
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_PLASMA_STORE,
        use_valgrind=use_valgrind,
        use_valgrind_profiler=RUN_PLASMA_STORE_PROFILER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share)
    return process_info


def start_worker(node_ip_address,
                 object_store_name,
                 raylet_name,
                 redis_address,
                 worker_path,
                 temp_dir,
                 raylet_ip_address=None,
                 stdout_file=None,
                 stderr_file=None,
                 fate_share=None):
    """This method starts a worker process.

    Args:
        node_ip_address (str): The IP address of the node that this worker is
            running on.
        object_store_name (str): The socket name of the object store.
        raylet_name (str): The socket name of the raylet server.
        redis_address (str): The address that the Redis server is listening on.
        worker_path (str): The path of the source code which the worker process
            will run.
        temp_dir (str): The path of the temp dir.
        raylet_ip_address (str): The IP address of the worker's raylet. If not
            provided, it defaults to the node_ip_address.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.

    Returns:
        ProcessInfo for the process that was started.
    """
    command = [
        sys.executable,
        "-u",
        worker_path,
        "--node-ip-address=" + node_ip_address,
        "--object-store-name=" + object_store_name,
        "--raylet-name=" + raylet_name,
        "--redis-address=" + str(redis_address),
        "--temp-dir=" + temp_dir,
    ]
    if raylet_ip_address is not None:
        command.append("--raylet-ip-address=" + raylet_ip_address)
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_WORKER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share)
    return process_info


def start_monitor(redis_address,
                  logs_dir,
                  stdout_file=None,
                  stderr_file=None,
                  autoscaling_config=None,
                  redis_password=None,
                  fate_share=None):
    """Run a process to monitor the other processes.

    Args:
        redis_address (str): The address that the Redis server is listening on.
        logs_dir(str): The path to the log directory.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        autoscaling_config: path to autoscaling config file.
        redis_password (str): The password of the redis server.

    Returns:
        ProcessInfo for the process that was started.
    """
    monitor_path = os.path.join(RAY_PATH, "monitor.py")
    command = [
        sys.executable,
        "-u",
        monitor_path,
        f"--logs-dir={logs_dir}",
        "--redis-address=" + str(redis_address),
    ]
    if autoscaling_config:
        command.append("--autoscaling-config=" + str(autoscaling_config))
    if redis_password:
        command.append("--redis-password=" + redis_password)
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_MONITOR,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share)
    return process_info
