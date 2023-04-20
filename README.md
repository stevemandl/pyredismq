# RedisMQ
[![PyPI version](https://badge.fury.io/py/redismq.svg)](https://badge.fury.io/py/redismq)
[![Build Status](https://travis-ci.com/stevemandl/pyredismq.svg?branch=main)](https://travis-ci.com/stevemandl/pyredismq)
[![codecov](https://codecov.io/gh/stevemandl/pyredismq/branch/main/graph/badge.svg?token=LZHN8D2FRB)](https://codecov.io/gh/stevemandl/pyredismq)
[![Documentation Status](https://readthedocs.org/projects/pyredismq/badge/?version=latest)](https://pyredismq.readthedocs.io/en/latest/?badge=latest)
## Description

RedisMQ uses the redis stream data structure to effect a message queue. The stream key name is the id of the message queue. 

It provides support for confirmed and unconfirmed guaranteed message queuing. Guaranteed messages will remain in the queue (as unread or pending messages in the redis stream) until they are read and acknowledged by exactly one consumer. Confirmed producers will get a response only when the message is received and processed by a consumer. Unconfirmed (but guaranteed) still means only a single consumer will process the message, but no response is sent. 

RedisMQ also provides support for fan-out (pub/sub) messaging when zero or more subscribers should receive the message. 

RedisMQ libraries for Python and Javascript are available.


## Requirements

The RedisMQ module requires Python 3.8 or higher.

### Prerequisites

Install Python development headers

On Ubuntu/Debian systems or Microsoft Windows:

`apt-get install python-dev`

On Redhat/Fedora systems, install them with:

`yum install python-devel`

On Mac OSX:

`xcode-select --install`

## Installation

To install RedisMQ:

```console
$ pip install redismq
```

or from source:

```console
$ python setup.py install
```

testing:

```console
pipenv install --dev
pipenv run pytest
```

debugging:

```console
export REDISMQ=DEBUG
...
```
## Getting Started

RedisMQ needs to connect to an existing redis server, so you will need the address and port of the server you want to use. RedisMQ also stores global state in the redis server. By default the namespace used for global keys is rmq:*. If you need to change this so it does not conflict with other data stored in redis, the configuration parameter redismq_namespace should be set to something different.

## Examples

Here are some examples of using the pyredismq module. See [more examples](https://github.com/stevemandl/pyredismq/tree/main/examples) here.

### Sending an unconfirmed message

From a Python shell we send an unconfirmed message:

```python
>>> import asyncio
>>> from redismq import Client
>>> async def sendAMessage():
...     mq_connection = await Client.connect('redis://127.0.0.1')
...     my_producer = await mq_connection.producer('mystream')
...     print( await my_producer.addUnconfirmedMessage('Hello there!'))
...
>>> asyncio.run(sendAMessage())
"1601460642682-0"
```

### Sending a confirmed message

From a Python shell we send a confirmed message:

```python
>>> import asyncio
>>> from redismq import Client
>>> async def sendAConfirmedMessage():
...     mq_connection = await Client.connect('redis://127.0.0.1')
...     my_producer = await mq_connection.producer('mystream')
...     response = await my_producer.addConfirmedMessage('Hello there! Let me know when you get this.')
...     print('Got confirmation', response)
...
>>> asyncio.run(sendAConfirmedMessage())
```

### Consuming a message

From a Python shell we consume a message:

```python
>>> from redismq import Client
>>> async def readAndConfirmMessage():
>>>     mq_connection = await Client.connect('redis://127.0.0.1')
>>>     my_consumer = await mq_connection.consumer('mystream', 'mygroup', 'consumer1')
>>>     payload = await my_consumer.read()
>>>     print('Got message', payload.message)
>>>     # here you can do something with the message
>>>     # the response passed to ack() is optional, 
>>>     # and ignored if the original message was unconfirmed:
>>>     resp = 'I got your message' if payload.responseChannel else ''
>>>     await payload.ack(resp)
```
## More Information

RedisMQ is free software under the New BSD license, see LICENSE.txt for
details.