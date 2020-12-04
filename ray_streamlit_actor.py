import ray
import os
import time
from datetime import datetime, timedelta
import streamlit as st
import argparse

def main():
    ray.init(address="auto", ignore_reinit_error=True)
    parser = argparse.ArgumentParser("Ray streamlit args parser")

    parser.add_argument("--actor-name", required=True)
    args = parser.parse_args()
    actor = retry_until_success(lambda: ray.get_actor(args.actor_name))
    placeholder = st.empty()

    i = 0
    while True:
        params = ray.get(actor.get_streamlit_params.remote())
        foo = st.slider("foo", 5, 10)
        with placeholder.beta_container():
            ### USER CODE TO RENDER ON EVERY REFRESH GOES HERE
            ### WE PROBABLY WANT TO INSERT THE USER'S SCRIPT
            ### INSIDE THIS SCAFFOLDING.
            st.write(f"iteration #{i}")
            st.write("foo: ", params["foo"])
            st.write("bar: ", params["bar"])
            st.write(foo)
        time.sleep(5)
        i += 1

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