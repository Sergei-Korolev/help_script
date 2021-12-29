from mysql.connector import connect
from influxdb import InfluxDBClient
import settings

# variables
host = settings.HOST
user = settings.USER
password = settings.PASSWORD
db_MySQL = settings.DB_MYSQL
db_InfluxDB = settings.DB_INFLUXDB

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
            my_sql_data = cursor.fetchall()

except Exception as e:
    print(e)

# connect to InfluxDB
try:
    client_influx = InfluxDBClient(host=host, port=8086)
    client_influx.switch_database(db_InfluxDB)

    # input data in influxDB via write_points
    for entry in my_sql_data:
        influx_db_data = [
            {
                "measurement": "SIP_call",
                "tags": {"calling": entry[0]},
                "fields": {"count": int(entry[1])},
            }
        ]
        print(f"Writing '{influx_db_data}' to InfluxDB")
        client_influx.write_points(influx_db_data)

except Exception as e:
    print(e)

finally:
    client_influx.close()
