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
    receivers_mail = []
    receiver = ", ".join(receivers_mail)

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
    text = text.format(table=tabulate(data, floatfmt='.3f', headers=['Date', 'Vehicle Number', 'Distance From Source(Kms)', 'Distance last 24 Hours(Kms)', 'Average Speed(Km/hr)', 'Location'], tablefmt="pretty"))
    html = html.format(table=tabulate(data, floatfmt='.3f', headers=['Date', 'Vehicle Number', 'Distance From Source(Kms)', 'Distance last 24 Hours(Kms)', 'Average Speed(Km/hr)', 'Location'], tablefmt="html"))

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
    R = 6373
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance


def calculate_distance(co_ordinates):
    """Takes an input of originLat, originLong, LastSeenLat, LastSeenLong, currentLat, currentLong as a tuple in the
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
        distance_new = distance_formula(prev_lat, prev_long, current_lat, current_long)
        distance_tuple = (distance_source, distance_new)
        return distance_tuple
    # Data is available
    else:
        distance_source = distance_formula(orig_lat, orig_long, current_lat, current_long)
        distance_new = distance_formula(prev_lat, prev_long, current_lat, current_long)
        distance_tuple = (distance_source, distance_new)
        return distance_tuple


def update(send_email_now):
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
                            PRIMARY KEY (id)
                            )"""
    distanceTableRecord = """CREATE TABLE IF NOT EXISTS DistanceTravelled (
                            id INT NOT NULL AUTO_INCREMENT,
                            date DATETIME NOT NULL,
                            vehicleID  INT NOT NULL,
                            distFromSource FLOAT NOT NULL,
                            dist24Hours FLOAT NOT NULL,
                            distNew FLOAT NOT NULL,
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
            # Date of new entry
            new_date = datetime.strptime(value["dttime"], '%d %b, %Y, %I:%M %p')
            yesterday_date = new_date - timedelta(days=1)
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
            # Fetching data from Distance Travelled for the last 24 hours to calculate distance and average speed
            cursor.execute(
                "SELECT date, distNew FROM DistanceTravelled WHERE vehicleID = %s AND date > %s ORDER BY date ASC",
                (vehicle_id, yesterday_date))
            prev_data = list(cursor.fetchall())
            # fetching data from Vehicles as per ID to calculate distance travelled from origin and last seen location
            cursor.execute("SELECT originLat, originLong, lastSeenLat, lastSeenLong FROM Vehicles WHERE id = %s", (vehicle_id,))
            coordinates = list(cursor.fetchone())
            # Adding current latitude and longitude
            coordinates.append(value['latitude'])
            coordinates.append(value['longitude'])
            # Distance tuple received as (distance_source, distance_new)
            distances = calculate_distance(coordinates)
            distance_source = distances[0]
            distance_new = distances[1]
            # If distance travelled is less than 500m
            if (len(prev_data) > 0) and (distance_new < 0.5):
                # No action taken
                pass
            else:
                vehicleList.append(value['vehicleNumber'])
                # Updating last seen latitude and longitude
                cursor.execute(
                    "UPDATE Vehicles SET lastSeenLat = %s, lastSeenLong = %s WHERE id = %s",
                    (value['latitude'], value['longitude'], vehicle_id))
                if len(prev_data) == 0:
                    # No previous data is available
                    distance_24hrs = 0
                    speed = 1
                else:
                    time_taken = prev_data[-1][0] - prev_data[0][0]
                    time_taken = time_taken.total_seconds() / 3600
                    distance_list = [x[1] for x in prev_data]
                    distance_24hrs = float(sum(distance_list)) + float(distance_new)
                    if time_taken == 0:
                        time_taken = 1
                    speed = distance_24hrs / time_taken

                data.append((new_date, vehicle_id, distance_source, distance_24hrs, distance_new, speed, value['location']))

        # Insert new entry into DistanceTravelled table
        sql = "INSERT INTO DistanceTravelled (date, vehicleID, distFromSource, dist24Hours, distNew, avgSpeed, lastSeen) values (%s, %s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, data)

        dataBase.commit()

        # Modifying data to generate report replacing the vehicle id with vehicle number
        if send_email_now is True:
            for num in range(len(data)):
                data[num] = list(data[num])
                data[num][1] = vehicleList[num]
                del data[num][4]
            send_email(data)
    cursor.close()
    dataBase.close()


# Task scheduling

# Daily updates at every 09:30 am
schedule.every().day.at("09:30").do(update, send_email_now=True)

# Daily updates at every 06:00 pm
schedule.every().day.at("18:00").do(update, send_email_now=True)

# Hourly updates every 1 hour without updating distance travelled in 24 hours, while updating average speed
schedule.every(15).minutes.do(update, send_email_now=False)

while True:
    # Checks whether a scheduled task is pending to run or not
    schedule.run_pending()
    time.sleep(1)
