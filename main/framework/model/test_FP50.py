from ModelPrepare import ModelPrepare
import pickle
# temp code to pickle results for easier development of FP50
infile = open('FP50/test_FP50.pickle', 'rb')
map_data_a, map_variables_v, input_vs, v_inverse_links=pickle.load(infile)
infile.close()

# run prepare step
mp = ModelPrepare()
models_df, variable_details = mp.prepare_models(map_data_a, map_variables_v, input_vs, v_inverse_links)
print('here')
print(models_df)