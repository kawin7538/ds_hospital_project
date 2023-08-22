import random
import numpy as np
import pandas as pd

LIST_SPECIFICATION=[
    'need_laboratory',
    'need_radiology',
    'need_orthopedics',
    'need_dermatology',
    'need_neurology',
    'need_dental',
    'need_ophthalmology',
    'need_otolaryngology',
    'need_thoracic',
    'need_hematology',
    'need_gi_liver',
    'need_gynaecology',
    'need_nephrology',
    'need_or',
    'need_ipd',
    'need_physical_therapy',
    'need_pharmacy'
]

LIST_SPECIFICATION_IN_DF=[
    'need_lab',
    'need_radio',
    'need_orthopedics',
    'need_dermatology',
    'need_neurology',
    'need_dental',
    'need_ophthalmology',
    'need_otolaryngology',
    'need_thoracic_department',
    'need_hematology',
    'need_gi_liver_department',
    'need_gynaecology',
    'need_nephrology_department',
    'need_or_room',
    'need_ipd_stay',
    'need_physical_theraputic_room',
    'need_pharmacy'
]

opd_final_df=pd.read_parquet("input_data/opd_final_df.parquet")

class Patient:
    def __init__(self,patient_id):
        self.patient_id=patient_id
        
        self.random_patient=np.random.choice([True,False],p=[(471306-133074)/471306,133074/471306])

        self.specification=dict()

        self._define_specification()

    def _define_specification(self):
        if self.random_patient:
            for idx in LIST_SPECIFICATION:
                self.specification[idx]=False

        else:
            temp_specification=opd_final_df.sample(1)
            for idx, col_df in zip(LIST_SPECIFICATION,LIST_SPECIFICATION_IN_DF):
                self.specification[idx]=temp_specification[col_df].values[0]
                # self.specification[idx]=True