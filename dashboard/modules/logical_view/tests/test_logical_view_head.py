import os
import sys
import logging
import psutil
import requests
import time
import traceback
import ray
import pytest
from datetime import datetime, timedelta
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (format_web_url, wait_until_server_available,
                            wait_until_succeeded_without_exception)

logger = logging.getLogger(__name__)


def test_actor_groups(ray_start_with_dashboard):
    @ray.remote
    class Foo:
        def __init__(self, num):
            self.num = num

        def do_task(self):
            return self.num

    @ray.remote(num_gpus=1)
    class InfeasibleActor:
        pass

    foo_actors = [
        Foo.options(streamlit_script_path="/fake/path").remote(4),
        Foo.remote(5)
    ]
    infeasible_actor = InfeasibleActor.remote()  # noqa
    results = [actor.do_task.remote() for actor in foo_actors]  # noqa
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    timeout_seconds = 5
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/logical/actor_groups")
            response.raise_for_status()
            actor_groups_resp = response.json()
            assert actor_groups_resp["result"] is True, actor_groups_resp[
                "msg"]
            actor_groups = actor_groups_resp["data"]["actorGroups"]
            assert "Foo" in actor_groups
            summary = actor_groups["Foo"]["summary"]
            # 2 __init__ tasks and 2 do_task tasks
            assert summary["numExecutedTasks"] == 4
            assert summary["stateToCount"]["ALIVE"] == 2

            entries = actor_groups["Foo"]["entries"]
            assert len(entries) == 2
            assert entries[0]["streamlitScriptPath"] == "/fake/path"
            assert "InfeasibleActor" in actor_groups

            entries = actor_groups["InfeasibleActor"]["entries"]
            assert "requiredResources" in entries[0]
            assert "GPU" in entries[0]["requiredResources"]
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_kill_actor(ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self):
            ray.show_in_dashboard("test")
            return os.getpid()

    a = Actor.remote()
    worker_pid = ray.get(a.f.remote())  # noqa

    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    def actor_killed(pid):
        """Check For the existence of a unix pid."""
        try:
            os.kill(pid, 0)
        except OSError:
            return True
        else:
            return False

    def get_actor():
        resp = requests.get(f"{webui_url}/logical/actor_groups")
        resp.raise_for_status()
        actor_groups_resp = resp.json()
        assert actor_groups_resp["result"] is True, actor_groups_resp["msg"]
        actor_groups = actor_groups_resp["data"]["actorGroups"]
        actor = actor_groups["Actor"]["entries"][0]
        return actor

    def kill_actor_using_dashboard(actor):
        resp = requests.get(
            webui_url + "/logical/kill_actor",
            params={
                "actorId": actor["actorId"],
                "ipAddress": actor["ipAddress"],
                "port": actor["port"]
            })
        resp.raise_for_status()
        resp_json = resp.json()
        assert resp_json["result"] is True, "msg" in resp_json

    start = time.time()
    last_exc = None
    while time.time() - start <= 10:
        try:
            actor = get_actor()
            kill_actor_using_dashboard(actor)
            last_exc = None
            break
        except (KeyError, AssertionError) as e:
            last_exc = e
            time.sleep(.1)
    assert last_exc is None


def test_streamlit_actor(ray_start_with_dashboard):
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)

    @ray.remote
    class ActorA:
        def get_streamlit_params(self):
            return {"foo": 1, "bar": 2}

    # Starting actor A should trigger creation of a streamlit process.
    a = ActorA.options(name="ActorA").remote()  # noqa

    def check_streamlit_server_running():
        proc_names = [proc.name().lower() for proc in psutil.process_iter()]
        if any(["streamlit" in name for name in proc_names]):
            pytest.fail(f"We fail here with proc_names={proc_names}")
        else:
            raise ValueError(
                "Able to fetch processes but streamlit not present.")
    t_end = datetime.now() + timedelta(seconds=5)

    while datetime.now() < t_end:
        try:
            check_streamlit_server_running()
        except Exception as e:
            last_e = e
            
    pytest.fail(str(last_e))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
