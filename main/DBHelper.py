
from django.db.models import Max
from django.db import transaction

from main.models import *
from main.framework.model.helper import getDateFromUnix

import logging
LOGGER = logging.getLogger('main')

class DBHelper():
	def __init__(self):
		pass

	def getSimulationStepByScenario(self, id):
		return SimulationStep.objects.filter(scenario_id=id).order_by('id')


	def getScenario(self, id):
		scenario = False
		try:
			scenario = Scenario.objects.get(id=id)
		except:
			pass
		return scenario

	def getCasestudy(self, id):
		casestudy = False
		try:
			casestudy = CaseStudy.objects.get(id=id)
		except:
			pass
		return casestudy


	def getDataByCase(self, casestudy_id):
		data = MapData.objects.filter(casestudy_id=casestudy_id).values()
		return data

	def getDataDetailByCase(self, casestudy_id, combined=False):
		data = []
		queryset = MapDataDetail.objects.filter(map_data__casestudy_id=casestudy_id)
		if combined:
			data = queryset.extra(select={
				'product': 'map_data__product',
				'geography': 'map_data__geography',
				'segment': 'map_data__segment',
				'date': 'map_data__date',
				#'variable': 'variable__variable',
				}).values('id', 'product', 'geography', 'segment', 'date', 'variable_id', 'variable', 'variable_value' )
		else:
			data = queryset.values()
		return data


	## scenario related

	def addScenarioRun(self, scenario_id):
		sr  = ScenarioRun.objects.create(scenario_id=scenario_id, status='running')
		run_id = sr.id
		ScenarioRunStep.objects.create(run_id=run_id, name='start', order=1, data='{}')
		return run_id
		

	def getStepOrder(self, run_id):
		order = 1
		srs  = ScenarioRunStep.objects.filter(run_id=run_id)
		if srs:
			order = srs.aggregate(Max('order'))['order__max']
			order += 1
		return order


	def updateScenarioRun(self, run_id, data):

		if data['type'] == 'step':
			#df = data['df'].to_json(orient='split')
			order = self.getStepOrder(run_id)
			ScenarioRunStep.objects.create(run_id=run_id, name=data['name'], order=order, data='{}')# df)

		else:
			ScenarioRun.objects.filter(id=run_id).update(status=data['status'])
			if data['status'] == 'completed':
				order = self.getStepOrder(run_id)
				ScenarioRunStep.objects.create(run_id=run_id, name='final', order=order, data='{}')



	@transaction.atomic
	def addScenarioOutput(self, scenario_id, run_id, final_output, simulated_data, map_data_df):
		ScenarioOutput.objects.filter(scenario_id=scenario_id).delete()
		so = ScenarioOutput.objects.create(scenario_id=scenario_id, data='{}')
		scenario_output_id = so.id
		#so = ScenarioOutput.objects.get(id=8)

		# start_time = time.time()

		objects = []
		for index, row in final_output.iterrows():
			date = getDateFromUnix(row['date'], obj=True)
			objects.append(ScenarioOutputDetail(
				scenario_output_id=so.id,
				actual=row['actual'],
				predicted=row['predicted'],
				predicted_change=row['predicted_change'],
				casestudy_id=so.scenario.case.id,
				variable_id=row['variable_id'],
				product_id=row['product_id'],
				geography_id=row['geography_id'],
				segment_id=row['segment_id'],
				date=date
				)
			)

		try:
			ScenarioOutputDetail.objects.bulk_create(objects)
		except Exception as ex:
			print (ex)

		# print("--- %s seconds ---" % (time.time() - start_time))

		## add to scenariodatadetails
		objects = []
		map_data = {} ## eg. (geography_id, product_id, segment_id, data) => 100 (id)
		for index, row in simulated_data.iterrows():

			#date = datetime.strptime(str(row['date']), "%Y-%m-%d %H:%M:%S").strftime('%m/%d/%Y')
			
			objects.append(ScenarioDataDetail(
				map_data_id=row['map_data_id'],
				scenario_output_id=scenario_output_id,
				variable_value=row['variable_value'],
				variable_id=row['data_variable_id']

				)
			)


		try:
			ScenarioDataDetail.objects.bulk_create(objects)
		except Exception as ex:
			print ('ex in adding scenariodatadetail ', ex)


		order = self.getStepOrder(run_id)
		ScenarioRunStep.objects.create(run_id=run_id, name='completed', order=order, data='{}')


	def getScenarioOutput(self, scenario_id):
		so = False
		try:
			so = ScenarioOutput.objects.get(scenario_id=scenario_id)
		except:
			pass
		return so

	## scenarios