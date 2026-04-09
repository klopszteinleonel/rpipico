import network
import ubinascii

# Activamos la interfaz de red
wlan = network.WLAN(network.STA_IF)
wlan.active(True)

# Sacamos la MAC en bytes y la pasamos a un formato legible
mac_raw = wlan.config('mac')
mac_address = ubinascii.hexlify(mac_raw, ':').decode().upper()

print(f"MAC Address: {mac_address}")
