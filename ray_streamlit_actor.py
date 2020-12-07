import ray
import os
import time
from datetime import datetime, timedelta
import streamlit as st
import argparse

ray.init(address="auto", ignore_reinit_error=True)

def fetch_actor():
    parser = argparse.ArgumentParser("Ray streamlit args parser")
    parser.add_argument("--actor-name", required=True)
    args = parser.parse_args()
    actor = retry_until_success(lambda: ray.get_actor(args.actor_name))
    return actor

def fetch_params(actor):
    params = ray.get(actor.get_streamlit_params.remote())
    return params

def main():
    actor = fetch_actor()
    placeholder = st.empty()
    
    foo = st.slider("foo", 5, 10)

    actor.set_x.remote(foo)
    params = fetch_params(actor)

    ### USER CODE TO RENDER ON EVERY REFRESH GOES HERE
    ### WE PROBABLY WANT TO INSERT THE USER'S SCRIPT
    ### INSIDE THIS SCAFFOLDING.
    st.write("x: ", params["x"])
    time.sleep(4)
    rerun()

def rerun():
    raise st.script_runner.RerunException(st.script_request_queue.RerunData(None))

def retry_until_success(f, timeout=15):
    end = datetime.now() + timedelta(seconds=timeout)
    while True:
        try:
            r = f()
            return r
        except Exception as e:
            if datetime.now() < end:
                time.sleep(1)
                continue
            raise e

if __name__ == '__main__':
    main()