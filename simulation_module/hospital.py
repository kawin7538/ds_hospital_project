import random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import simpy

from .patient import Patient

NUM_OPD_DEPARTMENT_COUNTER=30
NUM_LABORATORY_DEPARTMENT_WORKSTATION=15
NUM_RADIOLOGY_DEPARTMENT=5
NUM_ORTHOPEDIC_DEPARTMENT=1
NUM_DERMATOLOGY_DEPARTMENT=1
NUM_NEUROLOGY_DEPARTMENT=1
NUM_DENTAL_DEPARTMENT=3
NUM_OPHTHALMOLOGY_DEPARTMENT=1
NUM_OTOLARYNGOLOGY_DEPARTMENT=1
NUM_THORACIC_DEPARTMENT=1
NUM_HEMATOLOGY_DEPARTMENT=2
NUM_GI_LIVER_DEPARTMENT=2
NUM_GYNAECOLOGY_DEPARTMENT=1
NUM_NEPHROLOGY_DEPARTMENT=5
NUM_OPERATING_ROOM=3
NUM_IPD_DEPARTMENT=400
NUM_PHYSICAL_THERAPY_DEPARTMENT=1
NUM_PHARMACY_DEPARTMENT=1

AVG_OPD_USAGE=10
AVG_LABORATORY_USAGE=120
AVG_RADIOLOGY_USAGE=10
AVG_ORTHOPEDIC_USAGE=15
AVG_DERMATOLOGY_USAGE=20
AVG_NEUROLOGY_USAGE=30
AVG_DENTAL_USAGE=45
AVG_OPHTHALMOLOGY_USAGE=25
AVG_OTOLARYNGOLOGY_USAGE=15
AVG_THORACIC_USAGE=20
AVG_HEMATOLOGY_USAGE=60
AVG_GI_LIVER_USAGE=30
AVG_GYNAECOLOGY_USAGE=25
AVG_NEPHROLOGY_USAGE=60
AVG_OPERATING_ROOM_USAGE=60*5
AVG_IPD_USAGE=1440*5
AVG_PHYSICAL_THERAPY_USAGE=60*3
AVG_PHARMACY_USAGE=3

SIM_TIME=365*24*60
START_DATETIME=datetime(year=2022,month=1,day=1)

arrival_rate_df=pd.read_parquet("output/minutely_patient_arrival_rate_display_hourly.parquet")

class Hospital:
    def __init__(self) -> None:
        
        self.env=simpy.Environment()

        self.patient_counter=0

        self.opd_department=simpy.Resource(self.env,capacity=NUM_OPD_DEPARTMENT_COUNTER)
        self.laboratory_department=simpy.Resource(self.env,capacity=NUM_LABORATORY_DEPARTMENT_WORKSTATION)
        self.radiology_department=simpy.Resource(self.env,capacity=NUM_RADIOLOGY_DEPARTMENT)
        self.orthopedic_department=simpy.Resource(self.env,capacity=NUM_ORTHOPEDIC_DEPARTMENT)
        self.dermatology_department=simpy.Resource(self.env,capacity=NUM_DERMATOLOGY_DEPARTMENT)
        self.neurology_department=simpy.Resource(self.env,capacity=NUM_NEUROLOGY_DEPARTMENT)
        self.dental_department=simpy.Resource(self.env,capacity=NUM_DENTAL_DEPARTMENT)
        self.ophthalmology_department=simpy.Resource(self.env,capacity=NUM_OPHTHALMOLOGY_DEPARTMENT)
        self.otolaryngology_department=simpy.Resource(self.env,capacity=NUM_OTOLARYNGOLOGY_DEPARTMENT)
        self.thoracic_department=simpy.Resource(self.env,capacity=NUM_THORACIC_DEPARTMENT)
        self.hematology_department=simpy.Resource(self.env,capacity=NUM_HEMATOLOGY_DEPARTMENT)
        self.gi_liver_department=simpy.Resource(self.env,capacity=NUM_GI_LIVER_DEPARTMENT)
        self.gynaecology_department=simpy.Resource(self.env,capacity=NUM_GYNAECOLOGY_DEPARTMENT)
        self.nephrology_department=simpy.Resource(self.env,capacity=NUM_NEPHROLOGY_DEPARTMENT)
        self.operation_room_department=simpy.Resource(self.env,capacity=NUM_OPERATING_ROOM)
        self.ipd_department=simpy.Resource(self.env,capacity=NUM_IPD_DEPARTMENT)
        self.physical_therapy_department=simpy.Resource(self.env,capacity=NUM_PHYSICAL_THERAPY_DEPARTMENT)
        self.pharmacy_department=simpy.Resource(self.env,capacity=NUM_PHARMACY_DEPARTMENT)

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

            self.time_recorder[patient.patient_id]['VN_IN']=START_DATETIME+timedelta(minutes=self.env.now)

            opd_process=self.env.process(self.attend_opd(patient=patient))
            laboratory_process=self.env.process(self.attend_laboratory(patient=patient,opd_process=opd_process))

            attend_radiology_function=self.basic_process_template_function('need_radiology','radiology',self.radiology_department,AVG_RADIOLOGY_USAGE)
            radiology_process=self.env.process(attend_radiology_function(patient=patient,previous_process=laboratory_process))

            attend_orthopedic_function=self.basic_process_template_function('need_orthopedics','orthopedic',self.orthopedic_department,AVG_ORTHOPEDIC_USAGE)
            orthopedic_process=self.env.process(attend_orthopedic_function(patient=patient,previous_process=radiology_process))

            attend_dermatology_function=self.basic_process_template_function('need_dermatology','dermatology',self.dermatology_department,AVG_DERMATOLOGY_USAGE)
            dermatology_process=self.env.process(attend_dermatology_function(patient=patient,previous_process=orthopedic_process))

            attend_neurology_function=self.basic_process_template_function('need_neurology','neurology',self.neurology_department,AVG_NEUROLOGY_USAGE)
            neurology_process=self.env.process(attend_neurology_function(patient=patient,previous_process=dermatology_process))

            attend_dental_function=self.basic_process_template_function('need_dental','dental',self.dental_department,AVG_DENTAL_USAGE)
            dental_process=self.env.process(attend_dental_function(patient=patient,previous_process=neurology_process))

            attend_ophthalmology_function=self.basic_process_template_function('need_ophthalmology','ophthalmology',self.ophthalmology_department,AVG_OPHTHALMOLOGY_USAGE)
            ophthalmology_process=self.env.process(attend_ophthalmology_function(patient=patient,previous_process=dental_process))

            attend_otolaryngology_function=self.basic_process_template_function('need_otolaryngology','otolaryngology',self.otolaryngology_department,AVG_OTOLARYNGOLOGY_USAGE)
            otolaryngology_process=self.env.process(attend_otolaryngology_function(patient=patient,previous_process=ophthalmology_process))

            attend_thoracic_function=self.basic_process_template_function('need_thoracic','thoracic',self.thoracic_department,AVG_THORACIC_USAGE)
            thoracic_process=self.env.process(attend_thoracic_function(patient=patient,previous_process=otolaryngology_process))

            attend_hematology_function=self.basic_process_template_function('need_hematology','hematology',self.hematology_department,AVG_HEMATOLOGY_USAGE)
            hematology_process=self.env.process(attend_hematology_function(patient=patient,previous_process=thoracic_process))

            attend_gi_liver_function=self.basic_process_template_function('need_gi_liver','gi_liver',self.gi_liver_department,AVG_GI_LIVER_USAGE)
            gi_liver_process=self.env.process(attend_gi_liver_function(patient=patient,previous_process=hematology_process))

            attend_gynaecology_function=self.basic_process_template_function('need_gynaecology','gynaecology',self.gynaecology_department,AVG_GYNAECOLOGY_USAGE)
            gynaecology_process=self.env.process(attend_gynaecology_function(patient=patient,previous_process=gi_liver_process))

            attend_nephrology_function=self.basic_process_template_function('need_nephrology','nephrology',self.nephrology_department,AVG_NEPHROLOGY_USAGE)
            nephrology_process=self.env.process(attend_nephrology_function(patient=patient,previous_process=gynaecology_process))

            attend_or_function=self.basic_process_template_function('need_or','operating_room',self.operation_room_department,AVG_OPERATING_ROOM_USAGE)
            or_process=self.env.process(attend_or_function(patient=patient,previous_process=nephrology_process))

            attend_ipd_function=self.basic_process_template_function('need_ipd','ipd',self.ipd_department,AVG_IPD_USAGE)
            ipd_process=self.env.process(attend_ipd_function(patient=patient,previous_process=or_process))

            attend_physical_therapy_function=self.basic_process_template_function('need_physical_therapy','physical_therapy',self.physical_therapy_department,AVG_PHYSICAL_THERAPY_USAGE)
            physical_therapy_process=self.env.process(attend_physical_therapy_function(patient=patient,previous_process=ipd_process))

            attend_pharmacy_function=self.basic_process_template_function('need_pharmacy','pharmacy',self.pharmacy_department,AVG_PHARMACY_USAGE)
            pharmacy_process=self.env.process(attend_pharmacy_function(patient=patient,previous_process=physical_therapy_process))

            last_process=pharmacy_process
            self.env.process(self.attend_laststep(patient=patient,last_process=last_process))

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

        with self.opd_department.request() as req:

            yield req

            # for wating wait for doctor in OPD
            while (START_DATETIME+timedelta(minutes=self.env.now)).hour<7:
                # print(f"Skipped at {(START_DATETIME+timedelta(minutes=self.env.now))}")
                yield self.env.timeout(1)
                continue;

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

    def basic_process_template_function(self,specification:str,keyword:str,department:simpy.Resource, avg_time_usage):
        def basic_process_function(patient:Patient,previous_process):
            yield previous_process

            if patient.specification[specification]==False:
                # print(f"LAB Skipped for patient {patient.patient_id} at {(START_DATETIME+timedelta(minutes=self.env.now))}")
                yield self.env.timeout(0)

            else:

                self.time_recorder[patient.patient_id][f'datetime_{keyword}_queuing']=START_DATETIME+timedelta(minutes=self.env.now)
                self.time_recorder[patient.patient_id][f'length_inqueue_{keyword}']=len(department.queue)

                with department.request() as req:
                    
                    yield req

                    # print(f"Patient {patient.patient_id} started LAB at {START_DATETIME+timedelta(minutes=self.env.now)}")
                    self.time_recorder[patient.patient_id][f'datetime_{keyword}']=START_DATETIME+timedelta(minutes=self.env.now)

                    sampled_usage=random.expovariate(1.0/avg_time_usage)

                    yield self.env.timeout(sampled_usage)

                    # print(f"Patient {patient.patient_id} finished LAB at {START_DATETIME+timedelta(minutes=self.env.now)}")
                    self.time_recorder[patient.patient_id][f'datetime_{keyword}_finished']=START_DATETIME+timedelta(minutes=self.env.now)
        
        return basic_process_function

    def attend_laststep(self,patient:Patient,last_process):
        yield last_process
        self.time_recorder[patient.patient_id]['VN_OUT']=START_DATETIME+timedelta(minutes=self.env.now)

    def run(self):
        self.env.process(self.generate_patient_arrivals())
        self.env.run(until=SIM_TIME)

    def get_time_recorder(self)->dict[(int,dict)]:
        return self.time_recorder