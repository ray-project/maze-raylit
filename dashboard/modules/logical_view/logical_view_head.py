import logging
import aiohttp.web
import ray.utils
import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.actor_utils as actor_utils
from ray.new_dashboard.utils import rest_response
from ray.new_dashboard.datacenter import DataOrganizer, DataSource
from ray.core.generated import core_worker_pb2
from ray.core.generated import core_worker_pb2_grpc
import subprocess
import random
from typing import Dict, Tuple
from grpc.experimental import aio as aiogrpc

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

StreamlitConnection = Tuple[int, subprocess.Popen]

class LogicalViewHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        initial_actors = DataSource.actors
        self.streamlit_servers: Dict[str, StreamlitConnection] = {}

        for actor_id, actor in initial_actors.items():
            logger.info("Starting an initial streamlit server")
            self.start_streamlit_server(actor_id, actor)

        DataSource.actors.signal.append(self._update_streamlit_actors)

    async def _update_streamlit_actors(self, change):
        logger.info("Called update_streamlit_actors in logical_view_head")
        # Indicates a deletion
        if change.old and not change.new:
            actor_id, actor = change.old
            self.stop_streamlit_server(actor_id, actor)
        # Indicates an insertion
        if change.new and not change.old:
            actor_id, actor = change.new
            self.start_streamlit_server(actor_id, actor)

    def start_streamlit_server(self, actor_id, actor):
        # TODO(hackathon) Replace with link to script
        logger.info("Starting a streamlit server")
        port_number = random.randint(49152, 65535)
        actor_name = actor["name"]
        script_path = actor["streamlitScriptPath"]
        if script_path:
            streamlit_command = [
                "streamlit", "run", "--server.port", f"{port_number}",
                actor["streamlitScriptPath"], "--",
                f"--actor-name={actor_name}"
            ]
            proc = subprocess.Popen(streamlit_command)
            # logger.info(f"PROCESS={proc.__dict__}")
            # logger.info(f"PROCESS OUTPUT LOG = {proc.stdout}")
            # logger.info(f"PROCESS OUTPUT ERR = {proc.stderr}")
            self.streamlit_servers[actor_id] = (port_number, proc)

    def stop_streamlit_server(self, actor_id, actor):
        logger.info("Killing a streamlit server")
        script_path = actor["streamlitScriptPath"]
        if script_path:
            streamlit_process = self.streamlit_servers[actor_id]
            streamlit_process.kill()

    @routes.get("/logical/actor_groups")
    async def get_actor_groups(self, req) -> aiohttp.web.Response:
        actors = await DataOrganizer.get_all_actors()
        for actor in actors.values():
            streamlit_script_path = actor["streamlitScriptPath"]
            if streamlit_script_path != "":
                conn = self.streamlit_servers.get(actor["actorId"])
                if conn:
                    port, _ = conn
                    actor["streamlitPort"] = port
        actor_creation_tasks = await DataOrganizer.get_actor_creation_tasks()
        # actor_creation_tasks have some common interface with actors,
        # and they get processed and shown in tandem in the logical view
        # hence we merge them together before constructing actor groups.
        actors.update(actor_creation_tasks)
        actor_groups = actor_utils.construct_actor_groups(actors)
        return rest_response(
            success=True,
            message="Fetched actor groups.",
            actor_groups=actor_groups)

    @routes.get("/logical/kill_actor")
    async def kill_actor(self, req) -> aiohttp.web.Response:
        try:
            actor_id = req.query["actorId"]
            ip_address = req.query["ipAddress"]
            port = req.query["port"]
        except KeyError:
            return rest_response(success=False, message="Bad Request")
        try:
            channel = aiogrpc.insecure_channel(f"{ip_address}:{port}")
            stub = core_worker_pb2_grpc.CoreWorkerServiceStub(channel)

            await stub.KillActor(
                core_worker_pb2.KillActorRequest(
                    intended_actor_id=ray.utils.hex_to_binary(actor_id)))

        except aiogrpc.AioRpcError:
            # This always throws an exception because the worker
            # is killed and the channel is closed on the worker side
            # before this handler, however it deletes the actor correctly.
            pass

        return rest_response(
            success=True, message=f"Killed actor with id {actor_id}")

    async def run(self, server):
        pass
