import streamlit as st
import ray
import time

@ray.remote
class StreamLitActor:
    def __init__(self):
        self.foo = 6
        self.bar = 8

    def do_task(self):
        for i in range(100):
            if i % 2 == 0:
                self.foo *= 2
                self.bar *= 2.1
            else:
                self.foo /= 1.9
                self.bar /= 2
        return self.foo, self.bar
    
    def get_streamlit_params(self):
        return {"foo": self.foo,
                "bar": self.bar}

        
def main():
    ray.init()
    sta = StreamLitActor.options(name="streamlitactor").remote()
    while True:
        res = sta.do_task.remote()
        time.sleep(2)
        print(ray.get(res))

if __name__ == '__main__':
    main()