# Importing modules
import folium
import mysql.connector
import pytz
import requests
import smtplib
from branca.element import Figure
from math import sin, cos, sqrt, atan2, radians
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from datetime import datetime, timedelta


def send_email(mailing_data, cutoff_time):
    """Function to send an email using smtplib"""
    sender = 'trucking@als.group'
    password = 'Trucking@2021'
    receivers_mail = ['vaibhav.s@als.group']
    receiver = ", ".join(receivers_mail)

    html = """
        <html>
            <body>
                <div>
                    <div class="adM">
                        <div class="adm">
                            <div id="q_41" class="ajR h4" data-tooltip="Hide expanded content" aria-label="Hide expanded content" aria-expanded="true">
                                <div class="ajT">
                                </div>
                            </div>
                        </div>
                        <div class="im">
                            <p class="MsoNormal"><span>Dear All,<u></u><u></u></span></p>
                            <p class="MsoNormal"><span><u></u>&nbsp;<u></u></span></p>
                            <p class="MsoNormal"><span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Greetings for the Day!!!<u></u><u></u></span></p>
                            <p class="MsoNormal"><span><u></u>&nbsp;<u></u></span></p><p class="MsoNormal"><span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Kindly find the below Report<u></u><u></u></span></p>
                            <p class="MsoNormal"><span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <u></u><u></u></span></p>
                                                    
                            <table border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse">
                                <tbody>
                                    <tr style="height:14.4pt">
                                        <td colspan="6"
                                            style="border:solid windowtext 1.0pt;background:#00b050;padding:0cm 5.4pt 0cm 5.4pt;height:14.4pt">
                                            <p class="MsoNormal" align="center" style="text-align:center"><b><span
                                                        style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:white">MP-360
                                                        Asset Sweating Report</span></b></p>
                                        </td>
                                    </tr>
                                    <tr style="height:24.0pt">
                                        <td colspan="6"
                                            style="border-top:none;border-left:solid windowtext 1.0pt;border-bottom:none;border-right:solid windowtext 1.0pt;padding:0cm 5.4pt 0cm 5.4pt;height:24.0pt">
                                            <p class="MsoNormal" align="center" style="text-align:center"><span
                                                    style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:black">Kms operated in
                                                    last 24 hours // Cut off time is <b><mark>"""

    html += cutoff_time
    html += """</mark></b></span></p></td>
                                    </tr>
                                    <tr style="height:22.8pt">
                                        <td nowrap=""
                                            style="border-top:black;border-left:black;border-bottom:#e7e6e6;border-right:#e7e6e6;border-style:solid;border-width:1.0pt;background:#002060;padding:0cm 5.4pt 0cm 5.4pt;height:22.8pt">
                                            <p class="MsoNormal" align="center" style="text-align:center"><span
                                                    style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:white">S.No.</span></p>
                                        </td>
                                        <td nowrap=""
                                            style="border-top:solid black 1.0pt;border-left:none;border-bottom:none;border-right:solid #ededed 1.0pt;background:#002060;padding:0cm 5.4pt 0cm 5.4pt;height:22.8pt">
                                            <p class="MsoNormal" align="center" style="text-align:center"><span
                                                    style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:white">Location</span>
                                            </p>
                                        </td>
                                        <td nowrap=""
                                            style="border-top:solid black 1.0pt;border-left:none;border-bottom:none;border-right:solid #ededed 1.0pt;background:#002060;padding:0cm 5.4pt 0cm 5.4pt;height:22.8pt">
                                            <p class="MsoNormal" align="center" style="text-align:center"><span
                                                    style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:white">Vehicle Reg.
                                                    No</span></p>
                                        </td>
                                        <td
                                            style="border-top:solid black 1.0pt;border-left:none;border-bottom:none;border-right:solid #ededed 1.0pt;background:#002060;padding:0cm 5.4pt 0cm 5.4pt;height:22.8pt">
                                            <p class="MsoNormal" align="center" style="text-align:center"><span
                                                    style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:white">Average
                                                    Speed/Hour</span></p>
                                        </td>
                                        <td nowrap=""
                                            style="border-top:solid black 1.0pt;border-left:none;border-bottom:none;border-right:solid #ededed 1.0pt;background:#002060;padding:0cm 5.4pt 0cm 5.4pt;height:22.8pt">
                                            <p class="MsoNormal" align="center" style="text-align:center"><span
                                                    style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:white">Total
                                                    Kms</span></p>
                                        </td>
                                        <td nowrap=""
                                            style="border-top:solid black 1.0pt;border-left:none;border-bottom:solid #ededed 1.0pt;border-right:solid black 1.0pt;background:#002060;padding:0cm 5.4pt 0cm 5.4pt;height:22.8pt">
                                            <p class="MsoNormal" align="center" style="text-align:center"><span
                                                    style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:white;">MTD Kms</span>
                                            </p>
                                        </td>
                                    </tr>"""
    for data in mailing_data:
        html += '<tr style="height:14.4pt; border-bottom:solid #ededed 1.0pt;">'
        html += f'<td nowrap="" style="border-top:none;border-left:solid black 1.0pt;border-bottom:solid #ededed 1.0pt;border-right:none;padding:0cm 5.4pt 0cm 5.4pt;height:14.4pt"><p class="MsoNormal" align="center" style="text-align:center"><span style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:black">{data[0]}</span></p></td>'
        html += f'<td nowrap="" style="border-top:#e7e6e6;border-left:#e7e6e6;border-bottom:#ededed;border-right:#ededed;border-style:solid;border-width:1.0pt;padding:0cm 5.4pt 0cm 5.4pt;height:14.4pt"><p class="MsoNormal" align="center" style="text-align:center"><span style="font-size:10.0pt;font-family:&quot;Times New Roman&quot;,serif;color:black">{data[1]}</span></p></td>'
        html += f'<td nowrap="" style="border:solid #e7e6e6 1.0pt;border-left:none;border-bottom:solid #ededed 1.0pt;padding:0cm 5.4pt 0cm 5.4pt;height:14.4pt"><p class="MsoNormal" align="center" style="text-align:center"><span style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:black">{data[2]}</span></p></td>'
        html += f'<td nowrap="" style="border-top:solid #e7e6e6 1.0pt;border-left:none;border-bottom:solid #ededed 1.0pt;border-right:solid #ededed 1.0pt;padding:0cm 5.4pt 0cm 5.4pt;height:14.4pt"><p class="MsoNormal" align="center" style="text-align:center"><span style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:black">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; {data[3]}</span></p></td>'
        html += f'<td nowrap="" style="border-top:solid #e7e6e6 1.0pt;border-left:none;border-bottom:solid #ededed 1.0pt;border-right:solid #e7e6e6 1.0pt;padding:0cm 5.4pt 0cm 5.4pt;height:14.4pt"><p class="MsoNormal" align="center" style="text-align:center"><span style="font-size:9.0pt;font-family:&quot;Arial&quot;,sans-serif;color:black">{data[4]}</span></p></td>'
        html += f'<td nowrap="" style="border-bottom:solid #ededed 1.0pt;border-right:solid black 1.0pt;padding:0cm 5.4pt 0cm 5.4pt;height:14.4pt"><p class="MsoNormal" align="center" style="text-align:center"><span style="color:black">&nbsp; {data[5]}</span></p></td></tr>'
    html += """                     </tbody>
                                </table>
                            <p class="MsoNormal"><span><u></u>&nbsp;<u></u></span></p>
                            <p class="MsoNormal"><span>Regards,<u></u><u></u></span></p>
                            <p class="MsoNormal"><span>ALS ??? TMS<u></u><u></u></span></p>
                            <p class="MsoNormal"><span><u></u>&nbsp;<u></u></span></p>
                        </div>
                    </div>
                    <p class="MsoNormal"><span style="font-size:6.0pt">Note: This is an automated mail. Please do not reply to this mail.<u></u></span></p>
                    <p class="MsoNormal"><br></p>
                </div>
            </body>
        </html>
        """

    # Creating message
    message = MIMEMultipart()

    # Sending mail
    message['Subject'] = "Asset Sweating Report - MP360"
    message['From'] = sender
    message['To'] = receiver
    filename = "location.html"

    message.attach(MIMEText(html, "html"))

    # Attach the html file
    with open("location.html", "rb") as attachment:
        file_attachment = MIMEApplication(attachment.read())

    file_attachment.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    message.attach(file_attachment)

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(sender, password)
    server.sendmail(sender, receiver, message.as_string())
    server.quit()


def plot_map(data):

    figure = Figure(height=550, width=750)
    map_india = folium.Map()
    figure.add_child(map_india)
    coordinate_list = []
    for truck, geocode in data.items():
        time_list = list(geocode.keys())
        geocode_list = list(geocode.values())
        coordinate_list.extend(geocode_list)
        if len(geocode_list) > 1:
            time_list = [x.strftime("%d-%m-%Y, %H:%M:%S") for x in time_list]
            feature_group = folium.FeatureGroup(f"Vehicle: {truck}")
            folium.vector_layers.PolyLine(geocode_list, popup=f'<b>Path of {truck}</b>', tooltip=truck, weight=4, opacity=1).add_to(feature_group)
            marker_loc = [x for x in geocode_list]
            for x in range(len(time_list)):
                folium.Marker(location=marker_loc[x], popup=time_list[x], icon=folium.Icon(color='green', icon='truck', prefix='fa')).add_to(feature_group)
            map_india.add_child(feature_group)
    folium.LayerControl().add_to(map_india)
    sw = list(min(coordinate_list))
    ne = list(max(coordinate_list))
    map_india.fit_bounds([sw, ne])
    figure.save('location.html')


def calculate_distance(co_ordinates):
    """Takes an input of LastSeenLat, LastSeenLong, currentLat, currentLong as a tuple in the same specific order,
    and returns the distance calculated from last seen location"""
    prev_lat, prev_long, current_lat, current_long = co_ordinates[0], co_ordinates[1], co_ordinates[2], co_ordinates[3]

    # No previous data available
    if (prev_lat is None) and (prev_long is None):
        return 0
    # Data is available
    else:
        try:
            # Calculate distance with  Google Distance Matrix API
            url = "https://maps.googleapis.com/maps/api/distancematrix/json"
            params = {"origins": str(prev_lat) + " " + str(prev_long),
                      "destinations": str(current_lat) + " " + str(current_long),
                      "transit_mode": "bus",
                      "key": ""
                      }

            response = requests.get(url, params=params)
            json_response = response.json()
            distance = int(json_response['rows'][0]['elements'][0]['distance']['value']) / 1000.0
            # 10% approximation
            distance = distance * 1.10
            return distance

        except:
            # Calculating distance with haversine formula if Google Distance Matrix API fails.

            # Average radius of earth
            R = 6373
            lat1, lon1, lat2, lon2 = map(radians, [prev_lat, prev_long, current_lat, current_long])
            dlon = lon2 - lon1
            dlat = lat2 - lat1

            a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))

            distance = R * c
            # 10% approximation
            distance = distance * 1.10
        return distance


def update(send_email_now):
    """Function for daily updates of Live tracking of vehicles"""
    # Connecting with database
    dataBase = mysql.connector.connect(
        host="localhost",
        user="",
        passwd=""
    )

    # preparing a cursor object
    cursor = dataBase.cursor()

    # Creating database and tables if not exists
    cursor.execute("CREATE DATABASE IF NOT EXISTS WheelsEyeUpdates")
    cursor.execute("USE WheelsEyeUpdates")

    vehicleTableRecord = """CREATE TABLE IF NOT EXISTS Vehicles (
                            id INT NOT NULL AUTO_INCREMENT,
                            vehicleNum  VARCHAR(20) NOT NULL,
                            location VARCHAR(20),
                            lastSeenLat FLOAT,
                            lastSeenLong FLOAT,
                            PRIMARY KEY (id)
                            )"""
    distanceTableRecord = """CREATE TABLE IF NOT EXISTS DistanceTravelled (
                            id INT NOT NULL AUTO_INCREMENT,
                            date DATETIME NOT NULL,
                            vehicleID  INT NOT NULL,
                            dist24Hours FLOAT NOT NULL,
                            distNew FLOAT NOT NULL,
                            avgSpeed FLOAT,
                            currentLat FLOAT,
                            currentLong FLOAT,
                            PRIMARY KEY (id),
                            FOREIGN KEY (vehicleID) REFERENCES Vehicles(id)
                            )"""
    cursor.execute(vehicleTableRecord)
    cursor.execute(distanceTableRecord)
    dataBase.commit()

    # URL and access token
    URL = 'https://api.wheelseye.com/currentLoc'
    params = {'accessToken': ''}

    # Requesting the API for data
    response = requests.get(URL, params=params)

    # Checking for status code
    if response.status_code == 200:
        results = response.json()['data']['list']
        data = []
        mail_data = []
        map_data = {}
        for value in results:
            # Date of new entry
            new_date = datetime.strptime(value["dttime"], '%d %b, %Y, %I:%M %p')
            yesterday_date = new_date - timedelta(days=1)
            mtd_begining = new_date.replace(day=1, hour=0, minute=0)
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
                "SELECT date, distNew, currentLat, currentLong FROM DistanceTravelled WHERE vehicleID = %s AND date > %s ORDER BY date ASC",
                (vehicle_id, yesterday_date))
            received_data = list(cursor.fetchall())
            prev_data = [(x[0], x[1]) for x in received_data]
            map_data[value['vehicleNumber']] = {x[0]: (x[2], x[3]) for x in received_data}

            cursor.execute(
                "SELECT date, distNew FROM DistanceTravelled WHERE vehicleID = %s AND date > %s ORDER BY date ASC",
                (vehicle_id, mtd_begining))
            mtd_data = list(cursor.fetchall())
            # fetching data from Vehicles as per ID to calculate distance travelled from last seen location
            cursor.execute("SELECT lastSeenLat, lastSeenLong FROM Vehicles WHERE id = %s", (vehicle_id,))
            coordinates = list(cursor.fetchone())
            # Adding current latitude and longitude
            coordinates.append(value['latitude'])
            coordinates.append(value['longitude'])
            # Distance tuple received as (distance_source, distance_new)
            distance_new = calculate_distance(coordinates)
            # If distance travelled is less than 500m
            if (len(prev_data) > 0) and (distance_new < 0.5):
                # Not saving to database
                time_taken = prev_data[-1][0] - prev_data[0][0]
                time_taken = time_taken.total_seconds() / 3600
                distance_list = [x[1] for x in prev_data]
                distance_24hrs = float(sum(distance_list)) + float(distance_new)
                distance_mtd = float(sum([x[1] for x in mtd_data])) + float(distance_new)
                if time_taken == 0:
                    time_taken = 1
                speed = distance_24hrs / time_taken
            else:
                # Updating last seen latitude and longitude
                cursor.execute(
                    "UPDATE Vehicles SET lastSeenLat = %s, lastSeenLong = %s WHERE id = %s",
                    (value['latitude'], value['longitude'], vehicle_id))
                if len(prev_data) == 0:
                    # No previous data is available
                    distance_24hrs = 0
                    distance_mtd = 0
                    speed = 1
                else:
                    time_taken = prev_data[-1][0] - prev_data[0][0]
                    time_taken = time_taken.total_seconds() / 3600
                    distance_list = [x[1] for x in prev_data]
                    distance_24hrs = float(sum(distance_list)) + float(distance_new)
                    distance_mtd = float(sum([x[1] for x in mtd_data])) + float(distance_new)
                    if time_taken == 0:
                        time_taken = 1
                    speed = distance_24hrs / time_taken

                data.append((new_date, vehicle_id, distance_24hrs, distance_new, speed, value['latitude'], value['longitude']))

            # Creating mailing data
            cursor.execute("SELECT VehicleNum, location FROM Vehicles WHERE id = %s", (vehicle_id,))
            vehicle_data = cursor.fetchone()
            mail_data.append([new_date, vehicle_data[1], vehicle_data[0], round(speed, 2), round(distance_24hrs), round(distance_mtd)])

        # Insert new entry into DistanceTravelled table
        sql = "INSERT INTO DistanceTravelled (date, vehicleID, dist24Hours, distNew, avgSpeed, currentLat, currentLong) values (%s, %s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, data)

        dataBase.commit()
        # Modifying data to generate report replacing the vehicle id with vehicle number
        if send_email_now is True:
            plot_map(map_data)
            cutoff_time = max([mail_data[num][0] for num in range(len(mail_data))])
            cutoff_time = cutoff_time.strftime("%I:%M %p - %d-%b-%Y")
            for num in range(len(mail_data)):
                mail_data[num][0] = num + 1
                if mail_data[num][1] is None:
                    mail_data[num][1] = ''
            send_email(mail_data, cutoff_time)

    cursor.close()
    dataBase.close()

    del results
    del data
    del vehicle_data
    del vehicle_id
    del prev_data
    del coordinates
    del distance_new
    del distance_24hrs
    del mail_data
    del received_data
    del map_data


intz = pytz.timezone('Asia/Kolkata')

cur = datetime.now(intz)

hour = cur.hour
minute = cur.minute

if hour == 8 and (00 <= minute <= 4):
    update(send_email_now=True)
elif hour == 18 and (00 <= minute <= 4):
    update(send_email_now=True)
else:
    update(send_email_now=False)
   
