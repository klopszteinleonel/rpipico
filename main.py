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
import os
import network
import ubinascii
from led_async import LED_async
from machine import Pin

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
led_board.off()

rele = Pin(16, Pin.OUT)
rele.on()

DB_FILE = "config.json"

# Función para cargar los datos al arrancar
def cargar_db():
    try:
        with open(DB_FILE, "r") as f:
            return ujson.load(f)
    except (OSError, ValueError):
        # Si el archivo no existe o está roto, devolvemos un dict vacío
        return {}

# Función para guardar un dato
def guardar_dato(clave, valor):
    db = cargar_db()
    db[clave] = valor
    with open(DB_FILE, "w") as f:
        ujson.dump(db, f)

def cargar_config_inicial():
    global setpoint, modo, periodo
    
    print("Cargando configuración desde la memoria...")
    datos = cargar_db() # Usamos la función que lee el JSON
    
    if datos:
        # Usamos .get(llave, default) para evitar que el código explote si falta una llave
        setpoint = float(datos.get("setpoint", setpoint))
        modo = str(datos.get("modo", modo))
        periodo = int(datos.get("periodo", periodo))
        
        print("Configuración cargada con éxito:")
        print(f" -> Setpoint: {setpoint}")
        print(f" -> Modo: {modo}")
        print(f" -> Periodo: {periodo}")
    else:
        print("No se encontró archivo de configuración. Usando valores por defecto.")

def es_numero(v):
    try:
        float(v)
        return True
    except ValueError:
        return False

async def messages(client):  # Respond to incoming messages
    async for topic, msg, retained in client.queue:
        global setpoint, periodo, modos, modo

        if(topic.decode() == f'{mac_id}/destello'):
            led_board.on()
            await asyncio.sleep_ms(300)
            led_board.off()
            await asyncio.sleep_ms(300)
            led_board.on()
            await asyncio.sleep_ms(300)
            led_board.off()
        
        if(topic.decode() == f'{mac_id}/setpoint'):
            m = msg.decode()
            if(es_numero(m)):
                val = float(m)
                guardar_dato("setpoint", val)
                setpoint = val
                print(f"Guardado -> Topico: {topic.decode()}, Valor: {val}")
            else: 
                print("Variable no es numero")

        if(topic.decode() == f'{mac_id}/periodo'):
            m = msg.decode()
            if(es_numero(m)):
                val = float(m)
                guardar_dato("periodo", val)
                periodo = val
                print(f"Guardado -> Topico: {topic.decode()}, Valor: {val}")
                print(f"Periodo: {periodo}")
            else: 
                print("Variable no es numero")

        if(topic.decode() == f'{mac_id}/modo'):
            m = msg.decode()
            if(m == 'AUTO'):
                guardar_dato("modo", modos[0])
                modo = modos[0]
                print(f"Guardado -> Topico: {topic.decode()}, Valor: {modos[0]}")
            elif(m == 'MAN'):
                guardar_dato("modo", modos[1])
                modo = modos[1]
                print(f"Guardado -> Topico: {topic.decode()}, Valor: {modos[1]}")
            else: 
                print("Variable no es modo (AUTO, MAN)")

        if(topic.decode() == f'{mac_id}/rele'):
            m = msg.decode()
            if(modo == 'MAN'):
                rele.toggle()
                print(f"Se accionó el relé")
            else: 
                print("Modo AUTO")
                
        

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

                if(temperatura > setpoint):
                    rele.off()
                elif(temperatura < setpoint - 2): #pequeño offset
                    rele.on()
                
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
cargar_config_inicial()
try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()
