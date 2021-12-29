# requirement
# pip install mysql-connector-python
# pip install influxdb


from mysql.connector import connect, Error
from influxdb import InfluxDBClient

# variables
host = "localhost"
user = "root"
password = "admin"
db_MySQL = "SIP_Africa"
db_InfluxDB = "test_SIP_1"

# MySQL
try:
    with connect(
        host=host,
        user=user,
        password=password,
        db=db_MySQL,
    ) as connection:
        show_db_query = """select if(operators.OpName is null, SIP.calling, operators.OpName) as operators, count(SIP.calling) as
QuantityCalls  from SIP LEFT JOIN operators ON calling like concat (gt,'%') where SIP.calling like '2%' group
by operators;"""

        with connection.cursor() as cursor:
            cursor.execute(show_db_query)
            row = cursor.fetchall()
except Error as e:
    print(e)

# connect to InfluxDB
try:
    client_influx = InfluxDBClient(host=host, port=8086, )
    client_influx.switch_database(db_InfluxDB)

# input data in influxDB via write_points
    json_body = []
    for result in row:
        json_body = [
            {
                "measurement": "SIP_call",
                "tags": {
                    "calling": result[0]
                },
                "fields": {
                    "count": int(result[1])
                }
            }
        ]
        print(json_body)
        client_influx.write_points(json_body)

except Error as error:
    print(error)

finally:
    client_influx.close()
