import pandas as pd
from main.framework.model.ModelPrepare import ModelPrepare
from main.framework.model.ModelData import ModelData
from main.framework.model.FourierTransformer import FourierTransformer
from main.framework.model.PvalsTransformer import PvalsTransformer
from main.framework.dba.db_fourier import addFourier, getFourier
from main.models import CaseStudy
import pickle

class ModelRunner():
	def __init__(self, casestudy_id, step='start'):
		self.casestudy_id = casestudy_id
		self.step = step
		self.step1_variables = None
		self.variable_details = None

		self.step1_models = None
		self.output = {'success': False, 'message': ''}



	def run(self):
		casestudy = False

		#check if case study id exists
		try:
			casestudy = CaseStudy.objects.get(id=self.casestudy_id)
		except:
			self.output['message'] = 'No such case'
		print(casestudy)
		if casestudy:

			#variables_df = self.prepare_variables()

			if self.step == 'start':
				#get data for casestudy_id
				md=ModelData(self.casestudy_id)
				map_data_a, map_variables_v, input_vs, input_vs_log, v_inverse_links = md.prepare_models_data()
				variable_details=md.prepare_variables_data()

				#temp code to pickle results for easier development of FP50
				outfile = open('test_FP50.pickle', 'wb')
				pickle.dump((map_data_a,map_variables_v,input_vs,v_inverse_links),outfile)
				outfile.close()

				#run prepare step
				mp=ModelPrepare()
				models_df,variable_details = mp.prepare_models(map_data_a,map_variables_v,input_vs,v_inverse_links)

				pt = PvalsTransformer()
				pt.prepare_pvals_from_non_fourier(models_df, variable_details)


			elif self.step == 'pvals':

				final = getFourier(casestudy.id)
				final = pd.read_json(final, orient='split')
				pt = PvalsTransformer()
				pt.prepare_pvals_from_fourier(final, self.variable_details)



			
			

			


# start_time = time.time()
# mr = ModelRunner(55, 'start')
# mr.run()
# print("--- %s seconds ---" % (time.time() - start_time))

'''
from main.framework.ModelRunner import *

'''


'''
upload api

curl -X POST -H "Content-Type: multipart/form-data/ -F "model=variable" -F "case_id=33" -F "file=@/home/ashish/Projects/solutionroom/MAP/main/data/CM_DATA_VARIABLES.csv"

'''