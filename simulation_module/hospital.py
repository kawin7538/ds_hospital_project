import random
import numpy as np
import simpy

from .patient import Patient

NUM_OPD_DEPARTMENT_COUNTER=15

AVG_OPD_USAGE=15

SIM_TIME=365*24*60

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

            sampled_interarrival=random.expovariate(1.0/250*60)

            yield self.env.timeout(sampled_interarrival)

    def attend_opd(self,patient:Patient):
        
        print(f"Patient {patient.patient_id} started OPD queuing at {self.env.now:.1f}")

        with self.opd_department.request() as req:
            yield req

            print(f"Patient {patient.patient_id} started OPD at {self.env.now:.1f}")

            sampled_opd_usage=random.expovariate(1.0/AVG_OPD_USAGE)

            yield self.env.timeout(sampled_opd_usage)

            print(f"Patient {patient.patient_id} finished OPD at {self.env.now:.1f}")

    def run(self):
        self.env.process(self.generate_patient_arrivals())
        self.env.run(until=SIM_TIME)