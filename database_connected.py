import psycopg2
import math
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/data', methods=['POST'])
def receive_data():
    data = request.json
    print(f"Received data: {data}")
    return jsonify({"status": "success", "data_received": data}), 200

def testPostgreSQL(acceleration_x, acceleration_y, acceleration_z, gyro_x, gyro_y, gyro_z, sleep, sudden_accelerations_num, flip_num, abnormal_num):
    postgres_database = "alpaOnion"
    postgres_user = "4c91f542-752c-4e0a-a5d0-17ee6e319d4b"
    postgres_password = "kW0x83OKg5g7AfyBWXFXY7wPo"
    postgres_host = "apps-postgresql-single-8bd2ab7f-fead-40db-8b68-d635cad1b041-pub.education.wise-paas.com"
    postgres_port = 5432
    postgres_uri = f"postgres://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"

    try:
        # 데이터베이스에 연결
        conn = psycopg2.connect(postgres_uri)
        cur = conn.cursor()

        # 현재 시간을 가져옴
        current_time = str(datetime.now())
        acceleration = math.sqrt(math.pow(acceleration_x, 2) + math.pow(acceleration_y, 2) + math.pow(acceleration_z, 2))
        gyro = math.sqrt(math.pow(gyro_x, 2) + math.pow(gyro_y, 2) + math.pow(gyro_z,2))
        # 데이터를 삽입하는 쿼리 실행
        insert_query = f'''INSERT INTO busdriver.busdriver 
                        (timestamp, acceleration_x, acceleration_y, acceleration_z, gyro_x, gyro_y, gyro_z, sleep, acceleration, gyro) 
                        VALUES ('{current_time}', {acceleration_x}, {acceleration_y}, {acceleration_z}, 
                        {gyro_x}, {gyro_y}, {gyro_z}, {sleep}, {acceleration}, {gyro})'''

        cur.execute(insert_query)
        insert_query = f'''INSERT INTO busdriver.number
                                (timestamp, sudden_accelerations_num, flip_num, abnormal_num)
                                VALUES ('{current_time}', {sudden_accelerations_num}, {flip_num}, {abnormal_num})'''
        cur.execute(insert_query)
        # 변경 사항을 커밋
        conn.commit()

        # 삽입된 데이터를 확인하기 위해 선택 쿼리 실행
        cur.execute("SELECT * FROM busdriver.busdriver ORDER BY id DESC LIMIT 2")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        # 연결 종료
        cur.close()
        conn.close()

        # 결과를 딕셔너리 형태로 반환
        result = [dict(zip(colnames, row)) for row in rows]
        return result
    except psycopg2.Error as e:
        print(f"Unable to connect to PostgreSQL instance: {str(e)}")
        return {"error": f"Unable to connect to PostgreSQL instance: {str(e)}"}
    except Exception as e:
        print(f"An error occurred while connecting to PostgreSQL: {str(e)}")
        return {"error": f"An error occurred while connecting to PostgreSQL: {str(e)}"}

if __name__ == "__main__":

    app.run("0.0.0.0", debug=True)
    print(receive_data())

    acceleration_x = receive_data[1]
    acceleration_y = receive_data[2]
    acceleration_z = receive_data[3]
    gyro_x = receive_data[4]
    gyro_y = receive_data[5]
    gyro_z = receive_data[6]
    sleep = receive_data[7]
    sudden_accelerations_num = receive_data[8]
    flip_num = receive_data[9]
    abnormal_num = receive_data[10]
    result = testPostgreSQL(acceleration_x, acceleration_y, acceleration_z, gyro_x, gyro_y, gyro_z, sleep, sudden_accelerations_num, flip_num, abnormal_num)
    print(result)