# Importing modules
import mysql.connector
import requests
import smtplib
import schedule
import time
from math import sin, cos, sqrt, atan2, radians
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta


def send_email(data):
    """Function to send an email using smtplib"""
    sender = 'trucking@als.group'
    password = 'Trucking@2021'
    receiver = ''

    text = """
    Hello,

    The following details have ben recorded in the last 24 hours:

    {table}

    Regards,

    """

    html = """
    <html><body><p>Hello,</p>
    <p>The following details have ben recorded in the last 24 hours:</p>
    {table}
    <p>Regards,</p>
    <p></p>
    </body></html>
    """

    # Creating message
    text = text.format(table=tabulate(data, headers=['Date', 'Vehicle Number', 'Distance From Source', 'Distance last 24 Hours', 'Average Speed', 'Location'], tablefmt="pretty"))
    html = html.format(table=tabulate(data, headers=['Date', 'Vehicle Number', 'Distance From Source', 'Distance last 24 Hours', 'Average Speed', 'Location'], tablefmt="html"))

    message = MIMEMultipart('alternative', None, [MIMEText(text), MIMEText(html, 'html')])

    # Sending mail
    message['Subject'] = "WheelsEye Update"
    message['From'] = sender
    message['To'] = receiver
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(sender, password)
    server.sendmail(sender, receiver, message.as_string())
    server.quit()


def distance_formula(lat1, lon1, lat2, lon2):
    """Calculates distance using haversine formula"""

    # Average radius of earth
    R = 6371
    lat1, lon1, lat2, lon2 = radians(lat1), radians(lon1), radians(lat2), radians(lon2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance


def calculate_distance(co_ordinates):
    """Takes an input of originLat, originLong, PreviousDayLat, PreviousDayLong, currentLat, currentLong as a tuple in the
    same specific order and returns a tuple of distance calculated from source location and last seen location"""
    orig_lat, orig_long, prev_lat, prev_long = co_ordinates[0], co_ordinates[1], co_ordinates[2], co_ordinates[3]
    current_lat, current_long = co_ordinates[4], co_ordinates[5]
    # No previous and origin data available
    if (orig_lat is None) and (orig_long is None) and (prev_lat is None) and (prev_long is None):
        distance_tuple = (0, 0)
        return distance_tuple
    # No origin data available
    elif (orig_lat is None) and (orig_long is None):
        distance_source = 0
        distance24hours = distance_formula(prev_lat, prev_long, current_lat, current_long)
        distance_tuple = (distance_source, distance24hours)
        return distance_tuple
    # Data is available
    else:
        distance_source = distance_formula(orig_lat, orig_long, current_lat, current_long)
        distance24hours = distance_formula(prev_lat, prev_long, current_lat, current_long)
        distance_tuple = (distance_source, distance24hours)
        return distance_tuple


def update(send_email_now, change_day):
    """Function for daily updates of Live tracking of vehicles"""
    # Connecting with database
    dataBase = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="Password@123"
    )

    # preparing a cursor object
    cursor = dataBase.cursor()

    # Creating database and tables if not exists
    cursor.execute("CREATE DATABASE IF NOT EXISTS WheelsEyeUpdates")
    cursor.execute("USE WheelsEyeUpdates")

    vehicleTableRecord = """CREATE TABLE IF NOT EXISTS Vehicles (
                            id INT NOT NULL AUTO_INCREMENT,
                            vehicleNum  VARCHAR(20) NOT NULL,
                            originLat FLOAT,
                            originLong FLOAT,
                            lastSeenLat FLOAT,
                            lastSeenLong FLOAT,
                            prevDayLat FLOAT,
                            prevDayLong FLOAT,
                            PRIMARY KEY (id)
                            )"""
    distanceTableRecord = """CREATE TABLE IF NOT EXISTS DistanceTravelled (
                            id INT NOT NULL AUTO_INCREMENT,
                            date DATETIME NOT NULL,
                            vehicleID  INT NOT NULL,
                            distFromSource FLOAT NOT NULL,
                            dist24Hours FLOAT NOT NULL,
                            avgSpeed FLOAT,
                            lastSeen VARCHAR(256),
                            PRIMARY KEY (id),
                            FOREIGN KEY (vehicleID) REFERENCES Vehicles(id)
                            )"""
    cursor.execute(vehicleTableRecord)
    cursor.execute(distanceTableRecord)
    dataBase.commit()

    # URL and access token
    URL = 'https://api.wheelseye.com/currentLoc'
    params = {'accessToken': 'f386ed5d-0710-4077-b12b-a4acb7c3bbc9'}

    # Requesting the API for data
    response = requests.get(URL, params=params)

    # Checking for status code
    if response.status_code == 200:
        results = response.json()['data']['list']
        data = []
        vehicleList = []
        for value in results:
            # Check existing vehicles and add new vehicles
            sql = "SELECT id FROM Vehicles WHERE vehicleNum = %s"
            val = (value['vehicleNumber'], )
            cursor.execute(sql, val)
            vehicle_id = cursor.fetchone()
            if not vehicle_id:
                # Inserting new vehicles in Vehicles table
                sql = "INSERT INTO Vehicles (vehicleNum) VALUES (%s)"
                val = (value['vehicleNumber'],)
                cursor.execute(sql, val)
                dataBase.commit()
                vehicle_id = cursor.lastrowid
            else:
                vehicle_id = vehicle_id[0]
            # fetching data from Vehicles as per ID to calculate distance travelled
            cursor.execute("SELECT originLat, originLong, prevDayLat, prevDayLong FROM Vehicles WHERE id = %s", (vehicle_id,))
            coordinates = list(cursor.fetchone())
            coordinates.append(value['latitude'])
            coordinates.append(value['longitude'])
            distances = calculate_distance(coordinates)
            vehicleList.append(value['vehicleNumber'])
            if send_email_now is True and change_day is True:
                # Sending email, thus updating prev day latitude and longitude
                cursor.execute(
                    "UPDATE Vehicles SET lastSeenLat = %s, lastSeenLong = %s, prevDayLat = %s, prevDayLong = %s WHERE id = %s",
                    (value['latitude'], value['longitude'], value['latitude'], value['longitude'], vehicle_id))
            else:
                # Updating last seen latitude and longitude
                cursor.execute(
                    "UPDATE Vehicles SET lastSeenLat = %s, lastSeenLong = %s WHERE id = %s",
                    (value['latitude'], value['longitude'], vehicle_id))
            yesterday_date = datetime.today() - timedelta(days=1)
            cursor.execute("SELECT date FROM DistanceTravelled WHERE vehicleID = %s AND date > %s ORDER BY date ASC LIMIT 1", (vehicle_id, yesterday_date))
            last_date = cursor.fetchone()

            # Calculating speed
            try:
                hours = datetime.strptime(value["dttime"], '%d %b, %Y, %I:%M %p') - last_date[0]
                hours = hours.total_seconds() / 3600
                speed = distances[1] / hours
            except:
                speed = 1
            data.append((datetime.strptime(value["dttime"], '%d %b, %Y, %I:%M %p'), vehicle_id, distances[0], distances[1], speed, value['location']))

        # Insert new entry into DistanceTravelled table
        sql = "INSERT INTO DistanceTravelled (date, vehicleID, distFromSource, dist24Hours, avgSpeed, lastSeen) values (%s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, data)

        dataBase.commit()

        # Modifying data to generate report replacing the vehicle id with vehicle number
        for num in range(len(data)):
            data[num] = list(data[num])
            data[num][1] = vehicleList[num]
        if send_email_now is True:
            send_email(data)


# Task scheduling

# Daily updates at every 09:30 am
schedule.every().day.at("09:30").do(update, send_email_now=True, change_day=True)

# Daily updates at every 06:00 pm
schedule.every().day.at("18:00").do(update, send_email_now=True, change_day=False)

# Hourly updates every 1 hour without updating distance travelled in 24 hours, while updating average speed
schedule.every(15).minutes.do(update, send_email_now=False, change_day=False)

while True:
    # Checks whether a scheduled task is pending to run or not
    schedule.run_pending()
    time.sleep(1)

