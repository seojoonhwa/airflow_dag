from airflow.decorators import dag, task
from datetime import datetime
import pendulum
import json


@dag(dag_id='taskflow', start_date=datetime(2023,8,21,10,17), schedule_interval='@once')
def taskFlow():
    @task()
    def generateData():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        data_dict = json.loads(data_string)
        return data_dict

    # multiple_outputs 옵션을 true로 설정하면 반환된 값은 여러 XCom 값으로 저장됩니다. 또한 list와 tuple은 언롤링되어 저장됩니다.
    # dict의 경우엔 dict의 key를 XCom에서 key로 사용하고 값을 저장합니다.
    @task(multiple_outputs=True)
    def transform(data_dict):

        total_order_value = 0

        for value in data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value):
        with open("/shared/total.txt", 'a') as f:
            f.write(str(total_order_value))
        with open(f"/shared/touch.txt", 'r') as f:
            text = f.read()
        print(text)
        # print(total_order_value)
    
    # main flow 생성
    order_data = generateData()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

# dag start
taskFlow()
