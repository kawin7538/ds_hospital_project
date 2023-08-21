import random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import simpy

from .patient import Patient

NUM_OPD_DEPARTMENT_COUNTER=15
NUM_LABORATORY_DEPARTMENT_WORKSTATION=15

AVG_OPD_USAGE=10
AVG_LABORATORY_USAGE=120

SIM_TIME=365*24*60
START_DATETIME=datetime(year=2022,month=1,day=1)

arrival_rate_df=pd.read_parquet("output/minutely_patient_arrival_rate_display_hourly.parquet")

class Hospital:
    def __init__(self) -> None:
        
        self.env=simpy.Environment()

        self.patient_counter=0

        self.opd_department=simpy.Resource(self.env,capacity=NUM_OPD_DEPARTMENT_COUNTER)
        self.laboratory_department=simpy.Resource(self.env,capacity=NUM_LABORATORY_DEPARTMENT_WORKSTATION)

        self.time_recorder=dict()

    def generate_patient_arrivals(self):

        while True:

            if (START_DATETIME+timedelta(minutes=self.env.now)).hour<5 or (START_DATETIME+timedelta(minutes=self.env.now)).hour>15:
                # print(f"Skipped at {(START_DATETIME+timedelta(minutes=self.env.now))}")
                yield self.env.timeout(30)
                continue;

            self.patient_counter+=1
            patient=Patient(self.patient_counter)

            self.time_recorder[patient.patient_id]=dict()

            opd_process=self.env.process(self.attend_opd(patient=patient))
            laboratory_process=self.env.process(self.attend_laboratory(patient=patient,opd_process=opd_process))

            if (START_DATETIME+timedelta(minutes=self.env.now)).weekday()>=5:
                temp_interarrival_value=arrival_rate_df[(arrival_rate_df['InDateTime_onworkday']==False)&(arrival_rate_df['InDateTime_hour']==(START_DATETIME+timedelta(minutes=self.env.now)).hour)]['counter'].values[0]
            else:
                temp_interarrival_value=arrival_rate_df[(arrival_rate_df['InDateTime_onworkday']==True)&(arrival_rate_df['InDateTime_hour']==(START_DATETIME+timedelta(minutes=self.env.now)).hour)]['counter'].values[0]
            
            sampled_interarrival=random.expovariate(temp_interarrival_value)

            yield self.env.timeout(sampled_interarrival)

    def attend_opd(self,patient:Patient):
        
        # print(f"Patient {patient.patient_id} started OPD queuing at {START_DATETIME+timedelta(minutes=self.env.now)}")
        self.time_recorder[patient.patient_id]['datetime_opd_queuing']=START_DATETIME+timedelta(minutes=self.env.now)
        self.time_recorder[patient.patient_id]['length_inqueue_opd']=len(self.opd_department.queue)

        # for wating wait for doctor in OPD
        while (START_DATETIME+timedelta(minutes=self.env.now)).hour<7:
            # print(f"Skipped at {(START_DATETIME+timedelta(minutes=self.env.now))}")
            yield self.env.timeout(30)
            continue;
        
        # for lunch
        while (START_DATETIME+timedelta(minutes=self.env.now)).hour==12:
            # print(f"Skipped at {(START_DATETIME+timedelta(minutes=self.env.now))}")
            yield self.env.timeout(15)
            continue;

        with self.opd_department.request() as req:
            
            yield req

            # print(f"Patient {patient.patient_id} started OPD at {START_DATETIME+timedelta(minutes=self.env.now)}")
            self.time_recorder[patient.patient_id]['datetime_opd']=START_DATETIME+timedelta(minutes=self.env.now)

            sampled_opd_usage=random.expovariate(1.0/AVG_OPD_USAGE)

            yield self.env.timeout(sampled_opd_usage)

            # print(f"Patient {patient.patient_id} finished OPD at {START_DATETIME+timedelta(minutes=self.env.now)}")
            self.time_recorder[patient.patient_id]['datetime_opd_finished']=START_DATETIME+timedelta(minutes=self.env.now)

    def attend_laboratory(self, patient:Patient, opd_process):
        yield opd_process

        if patient.specification['need_laboratory']==False:
            # print(f"LAB Skipped for patient {patient.patient_id} at {(START_DATETIME+timedelta(minutes=self.env.now))}")
            yield self.env.timeout(0)

        else:

            self.time_recorder[patient.patient_id]['datetime_laboratory_queuing']=START_DATETIME+timedelta(minutes=self.env.now)
            self.time_recorder[patient.patient_id]['length_inqueue_laboratory']=len(self.laboratory_department.queue)

            with self.laboratory_department.request() as req:
                
                yield req

                # print(f"Patient {patient.patient_id} started LAB at {START_DATETIME+timedelta(minutes=self.env.now)}")
                self.time_recorder[patient.patient_id]['datetime_laboratory']=START_DATETIME+timedelta(minutes=self.env.now)

                sampled_laboratory_usage=random.expovariate(1.0/AVG_LABORATORY_USAGE)

                yield self.env.timeout(sampled_laboratory_usage)

                # print(f"Patient {patient.patient_id} finished LAB at {START_DATETIME+timedelta(minutes=self.env.now)}")
                self.time_recorder[patient.patient_id]['datetime_laboratory_finished']=START_DATETIME+timedelta(minutes=self.env.now)

    def run(self):
        self.env.process(self.generate_patient_arrivals())
        self.env.run(until=SIM_TIME)

    def get_time_recorder(self)->dict[(int,dict)]:
        return self.time_recorder