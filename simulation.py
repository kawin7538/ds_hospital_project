import json

from simulation_module.hospital import Hospital

if __name__ == '__main__':
    hospital=Hospital()
    hospital.run()

    dict_time_recorder=hospital.get_time_recorder()
    
    with open("output/base_simulation_results/base_result_regression_based.json","w") as f:
        json_string=json.dumps(dict_time_recorder, default=str)
        f.write(json_string)