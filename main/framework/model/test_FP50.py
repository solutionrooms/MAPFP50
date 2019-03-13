from ModelPrepare import ModelPrepare
import pickle
# temp code to pickle results for easier development of FP50
pickle_path = 'C:\\codes\\map_fp_50\\test_FP50_v2.pickle'
with open(pickle_path, 'rb') as infile:
    map_data_a,map_variables_v,input_vs,input_vs_logs,v_inverse_links,p_metrics,p_models,p_data,p_observations,p_dates=pickle.load(infile)
# run prepare step
mp = ModelPrepare()
models_df,variable_details = mp.prepare_models(p_metrics,p_models,p_data,p_observations,p_dates)
print('here')
print(models_df)
print(variable_details)