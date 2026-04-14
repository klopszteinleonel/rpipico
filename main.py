# (C) Copyright Peter Hinch 2017-2019.
# Released under the MIT licence.

# This demo publishes to topic "result" and also subscribes to that topic.
# This demonstrates bidirectional TLS communication.
# You can also run the following on a PC to verify:
# mosquitto_sub -h test.mosquitto.org -t result
# To get mosquitto_sub to use a secure connection use this, offered by @gmrza:
# mosquitto_sub -h <my local mosquitto server> -t result -u <username> -P <password> -p 8883

# Public brokers https://github.com/mqtt/mqtt.github.io/wiki/public_brokers

# red LED: ON == WiFi fail
# green LED heartbeat: demonstrates scheduler is running.

from mqtt_as import MQTTClient
from mqtt_local import config
import uasyncio as asyncio
import dht, machine
import ujson
import network
import ubinascii
from led_async import LED_async

# 1. Obtenemos la MAC para el tópico (dinámico)
wlan = network.WLAN(network.STA_IF)
mac_raw = wlan.config('mac')
mac_id = ubinascii.hexlify(mac_raw).decode().upper()
print(f"MAC Address: {mac_id}")

d = dht.DHT11(machine.Pin(15))
setpoint = 10
periodo = 5
modos = ['AUTO', 'MAN']
modo = modos[0]
led_board = Pin("LED", Pin.OUT)
led = LED_async(led_board)


async def messages(client):  # Respond to incoming messages
    async for topic, msg, retained in client.queue:

        if(topic.decode() == f'{mac_id}/destello'):
            led.flash(0.7 + n/4)
        
        print(f"Guardado -> Topico: {topic.decode()}, Valor: {msg.decode()}")

async def up(client):  # Respond to connectivity being (re)established
    while True:
        await client.up.wait()  # Wait on an Event
        client.up.clear()
        await client.subscribe(f'{mac_id}/setpoint', 1)  # renew subscriptions
        await client.subscribe(f'{mac_id}/periodo', 1)
        await client.subscribe(f'{mac_id}/destello', 1)
        await client.subscribe(f'{mac_id}/modo', 1)
        await client.subscribe(f'{mac_id}/rele', 1)


async def main(client):
    await client.connect()

    for coroutine in (up, messages):
        asyncio.create_task(coroutine(client))
    
    n = 0
    await asyncio.sleep(3)  # Give broker time
    while True:
        try:
            d.measure()
            try:

                temperatura=d.temperature()

                # 2. Armamos el diccionario con los datos
                datos = {
                    'temp': d.temperature(),
                    'hum': d.humidity(),
                    'setpoint': setpoint, # Ejemplo
                    'periodo': periodo,
                    'modo': modo
                }

                # 3. Convertimos el diccionario a un string JSON
                payload = ujson.dumps(datos)

                # 4. Publicamos
                # El tópico queda: "2CCF67B72EC0/data"
                await client.publish(f'{mac_id}/data', payload, qos=1)
            except OSError as e:
                print("error al formatear datos")
        except OSError as e:
            print("sin sensor")
        await asyncio.sleep(periodo)  # Broker is slow

# Define configuration
config['ssl'] = True
config['queue_len'] = 1  # Use event interface with default queue size

# Set up client
MQTTClient.DEBUG = True  # Optional
client = MQTTClient(config)
try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()
