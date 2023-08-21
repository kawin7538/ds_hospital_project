import random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import simpy

from .patient import Patient

NUM_OPD_DEPARTMENT_COUNTER=15

AVG_OPD_USAGE=15

SIM_TIME=365*24*60
START_DATETIME=datetime(year=2022,month=1,day=1)

arrival_rate_df=pd.read_parquet("output/hourly_patient_arrival_rate.parquet")

class Hospital:
    def __init__(self) -> None:
        
        self.env=simpy.Environment()

        self.patient_counter=0

        self.opd_department=simpy.Resource(self.env,capacity=NUM_OPD_DEPARTMENT_COUNTER)

    def generate_patient_arrivals(self):
        while True:
            self.patient_counter+=1
            patient=Patient(self.patient_counter)

            opd_process=self.env.process(self.attend_opd(patient=patient))

            if (START_DATETIME+timedelta(minutes=self.env.now)).weekday()>=5:
                temp_interarrival_value=arrival_rate_df[(arrival_rate_df['InDateTime_onworkday']==False)&(arrival_rate_df['InDateTime_hour']==(START_DATETIME+timedelta(minutes=self.env.now)).hour)]['counter'].values[0]
            else:
                temp_interarrival_value=arrival_rate_df[(arrival_rate_df['InDateTime_onworkday']==True)&(arrival_rate_df['InDateTime_hour']==(START_DATETIME+timedelta(minutes=self.env.now)).hour)]['counter'].values[0]
            
            sampled_interarrival=random.expovariate(1.0/temp_interarrival_value)

            yield self.env.timeout(sampled_interarrival)

    def attend_opd(self,patient:Patient):
        
        print(f"Patient {patient.patient_id} started OPD queuing at {START_DATETIME+timedelta(minutes=self.env.now)}")

        with self.opd_department.request() as req:
            yield req

            print(f"Patient {patient.patient_id} started OPD at {START_DATETIME+timedelta(minutes=self.env.now)}")

            sampled_opd_usage=random.expovariate(1.0/AVG_OPD_USAGE)

            yield self.env.timeout(sampled_opd_usage)

            print(f"Patient {patient.patient_id} finished OPD at {START_DATETIME+timedelta(minutes=self.env.now)}")

    def run(self):
        self.env.process(self.generate_patient_arrivals())
        self.env.run(until=SIM_TIME)