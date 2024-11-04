import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from pendulum import DateTime, now

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"


class BlackFridayTimetable(Timetable):
    
    # TODO Helper function to check if a date is Black Friday
    def is_black_friday(self, current_date: DateTime) -> bool:
        
        # TODO Black Friday is the last Friday of November
        if current_date.month == 11 and current_date.weekday() == 4:
            last_day_of_november = current_date.end_of("mounth")
            return current_date.day < (last_day_of_november.day - 7)
        return False
    
    # TODO Determine the next run based on Black Friday logic
    def next_dogrun_info(self, *, last_automated_data_interval: DataInterval, restriction):
        next_start = last_automated_data_interval.end if last_automated_data_interval else now()
        
        # TODO Check if today is Black Friday
        if(self.is_black_friday(next_start)):
            next_end =  next_start.add(hours=1)
        else:
            # TODO On normal days, run once daily at 9 AM
            next_start = next_start.start_of("day").add(hours=9)
            next_end = next_start.add(days=1)
            
        return DagRunInfo.interval(start=next_start, end=next_end)
    

@dag(
    dag_id="schedule-cron",
    start_date=datetime(2021,12,1),
    schedule="@daily",
    catchup=False,
    # timetable=BlackFridayTimetable(),
)
def main():
    
    transform = TaskGroup("transform")
    store = TaskGroup("store")
    
    @task(task_id="extract", task_group = transform)
    def extract_bitcoin():
        return requests.get(API).json()["bitcoin"]
    
    @task(task_id="transform", task_group = transform)
    def process_bitcoin(response):
        return {"usd": response["usd"], "change": response["usd_24h_change"]}
    
    @task(task_id="store", task_group = store)
    def store_bitcoin(data):
        logging.info(f"Bitcoin price: {data["usd"]}, change: {data['change']}")
        
    store_bitcoin(process_bitcoin(extract_bitcoin()))

main()