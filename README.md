aiocometd
=========

[![Docs](https://readthedocs.org/projects/aiocometd/badge/?version=latest)](http://aiocometd.readthedocs.io/en/latest/?badge=latest)
[![Licence](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

:warning: Forked project
--------

The maintainer of aiocometd, [Róbert Márki](https://github.com/robertmrk), unfortunately
was not responding to the open [Issues](https://github.com/robertmrk/aiocometd/issues) in the aiocometd
repository. Multiple people attempted to fork Róbert's project, in order to support asyncio on Python >=3.10.
But, I noticed that most of this attempts were missing:

- tests
- still using setup.py (nothing wrong about this though, but the industry is moving towards pyproject)
- removing just the `loop` keyword from the asyncio calls, without updating the whole module

I tried to resolve all of the above items:

- tests are now running compatible with `pytest`
- removed `asynctest`, nothing wrong about it, but the `unittest.mock` has the `AsyncMock`
- fixed some typos
- reformatted the code using `ruff`
- removed `setup.py`, `setup.cfg`, `tox.ini`, `_metadata.py`. In favour of `pyproject.toml`

All credits should still go to Róbert, he did a great Python package that wasn't that complex
to upgrade to a new Python version.

Description
--------

aiocometd is a [CometD](https://cometd.org/) client built using [asyncio](https://docs.python.org/3/library/asyncio.html), implementing the
[Bayeux](https://docs.cometd.org/current/reference/#_bayeux) protocol.

[CometD](https://cometd.org/) is a scalable WebSocket and HTTP based event and message routing bus.
[CometD](https://cometd.org/) makes use of WebSocket and HTTP push technologies known as [Comet](https://en.wikipedia.org/wiki/Comet_(programming)) to
provide low-latency data from the server to browsers and client applications.


Features
--------

- Supported transports:
   - ``long-polling``
   - ``websocket``
- Automatic reconnection after network failures
- Extensions

Usage
-----

```python
import asyncio

from aiocometd import Client

async def chat():
    nickname = "John"

    # connect to the server
    async with Client("http://example.com/cometd") as client:

        # subscribe to channels to receive chat messages and
        # notifications about new members
        await client.subscribe("/chat/demo")
        await client.subscribe("/members/demo")

        # send initial message
        await client.publish("/chat/demo", {
            "user": nickname,
            "membership": "join",
            "chat": nickname + " has joined"
        })
        # add the user to the chat room's members
        await client.publish("/service/members", {
            "user": nickname,
            "room": "/chat/demo"
        })

        # listen for incoming messages
        async for message in client:
            if message["channel"] == "/chat/demo":
                data = message["data"]
                print(f"{data['user']}: {data['chat']}")

if __name__ == "__main__":
    loop = asyncio.get_running_loop()
    loop.run_until_complete(chat())
```

For more detailed usage examples take a look at the
command line chat [example](https://github.com/bighelmet7/aiocometd/blob/develop/examples/chat.py) or for a more
complex example with a GUI check out the [aiocometd-chat-demo](https://github.com/bighelmet7/aiocometd-chat-demo)

Documentation
-------------

https://aiocometd.readthedocs.io/

Install
-------

```bash
pip install aiocometd
```

Requirements
------------

- Python 3.10+
- aiohttp
