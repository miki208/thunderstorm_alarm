import websocket
import json
import threading
import requests
from time import sleep

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

slack_token = 'xoxb-1857010721059-1856857391506-UjSe5aFVEu4o3zC2qKsdOD9C'
slack_client = WebClient(token = slack_token)

def SendToSlack(msg):
    try:
        response = slack_client.chat_postMessage(channel = 'thunderstorm-alarm', text = msg)
    except SlackApiError as e:
        print(e.response['error'])

def getLocationName(lat, lon):
    url = "https://wft-geo-db.p.rapidapi.com/v1/geo/locations/{}{}/nearbyCities".format(str(lat) if lat < 0 else "%2B" + str(lat), str(lon) if lon < 0 else "%2B" + str(lon))
    querystring = {"limit":"1","distanceUnit":"KM","radius":"15"}
    
    headers = {
        'x-rapidapi-host': "wft-geo-db.p.rapidapi.com",
        'x-rapidapi-key': "HpNxUsgI8mmshsdhsrneo43dMOI2p1Y7hRajsnDLOAApiGPyBj"
    }
    
    response = requests.request("GET", url, headers=headers, params=querystring)
    response = json.loads(response.text)
    
    if "data" in response.keys() and len(response["data"]) >= 1 and "name" in response["data"][0]:
        return response["data"][0]["name"]
        
    return "Unknown"
    
def sendAlarm(lat, lon, isnear):
    locationName = getLocationName(lat, lon)

    if isnear:
        msg = "Grmljavinsko nevreme je blizu Beograda. Grom je udario blizu {} ({}, {}).".format(locationName, lat, lon)
    else:
        msg = "Grmljavinsko nevreme je u Srbiji, i potencijalno moze da dodje do Beograda. Grom je udario blizu {} ({}, {}).".format(locationName, lat, lon)

    SendToSlack(msg)

def sendPing():
    global ws

    while True:
        sleep(30)
        
        if connected:
            ws.send('{}')
        
def isNear(lat, lon):
    if lat >= 44.639027 and lat <= 45.140375 and lon >= 20.054260 and lon <= 20.821890:
        return True
        
    return False
    
def isInRange(lat, lon):
    if lat >= 44.578109 and lat <= 45.106709 and lon >= 19.151434 and lon <= 20.833877:
        return True
        
    return False

def on_message(ws, message):
    global lastInRangeAlarm
    global lastNearAlarm
    
    obj = json.loads(message)
    
    if "lat" in obj.keys():
        if isNear(float(obj["lat"]), float(obj["lon"])):
            lastNearAlarmTmp = lastNearAlarm
            lastNearAlarm = int(obj["time"])
            lastInRangeAlarm = lastNearAlarm
            
            if lastNearAlarm - lastNearAlarmTmp > 1 * 60 * 60 * 1000000000:
                print("First near: {} - ({}, {})".format(obj["time"], obj["lat"], obj["lon"]))
                
                sendAlarm(float(obj["lat"]), float(obj["lon"]), True)
            else:
                print("Near: {} - ({}, {})".format(obj["time"], obj["lat"], obj["lon"]))
        elif isInRange(float(obj["lat"]), float(obj["lon"])):
            lastInRangeAlarmTmp = lastInRangeAlarm
            lastInRangeAlarm = int(obj["time"])
            
            if lastInRangeAlarm - lastInRangeAlarmTmp > 3 * 60 * 60 * 1000000000:
                print("First in range: {} - ({}, {})".format(obj["time"], obj["lat"], obj["lon"]))
                
                sendAlarm(float(obj["lat"]), float(obj["lon"]), False)
            else:
                print("In range: {} - ({}, {})".format(obj["time"], obj["lat"], obj["lon"]))

def on_error(ws, error):
    print(error)

def on_close(ws):
    global connected

    connected = False
    
    print("### closed ###")
    
def on_open(ws):
    global connected

    connected = True
    print("### connected ###")

    ws.send('{"time":0}');

if __name__ == "__main__":
    #initialize state variables
    lastNearAlarm = 0
    lastInRangeAlarm = 0
    connected = False

    #initialize websocket
    ws = websocket.WebSocketApp("wss://ws8.blitzortung.org:3000/",
        on_message = on_message,
        on_error = on_error,
        on_close = on_close,
        on_open = on_open)
        
    #initialize and start ping thread
    x = threading.Thread(target = sendPing, daemon=True)
    x.start()
    
    #run until we explicitly say to stop
    while ws.run_forever() != False:
        pass