from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from main.models import MapVariables, MapVariablesDetail
from sqlalchemy import create_engine

from django.conf import settings

class ModelData():
    def __init__(self, casestudy_id):
        self.casestudy_id = casestudy_id
        self.step1_variables = None
        self.variable_details = None

        self.step1_models = None
        self.output = {'success': False, 'message': ''}
        db = settings.DATABASES['default']
        self.engine = create_engine(
            'postgresql+psycopg2://%s:%s@%s:%s/%s' % (db['USER'], db['PASSWORD'], db['HOST'], db['PORT'], db['NAME']))



    def prepare_variables_data(self):
        mv = MapVariables.objects.filter(casestudy_id=self.casestudy_id)
        mvd = MapVariablesDetail.objects.filter(map_variables_id__in=mv.values('id'))
        mvd_values = list(mvd.values('map_variables_id', 'link_variable_id', 'link_type'))
        mvd_list = list(
            map(lambda item: [item['map_variables_id'], item['link_variable_id'], item['link_type']], mvd_values))
        mvd_list_arr = np.array(mvd_list)
        self.variable_details = pd.DataFrame(mvd_list_arr, columns=['output_variable', 'link_variable', 'link'])
        return self.variable_details

    def prepare_models_data(self):
        #### Start Prepare Models for step1 #########
        map_data_a = pd.read_sql_query('''
		SELECT  md.id,
    			md.casestudy_id,
    			md.geography_id,
    			md.product_id,
    			md.segment_id,
    			md.date,
    			md.year_type,
    			mdd.variable_id,
    			mdd.variable_value
   		FROM main_mapdata as md
     	JOIN main_mapdatadetail as mdd ON mdd.map_data_id = md.id
   		join main_casestudydatainstance as csdi on csdi.id=mdd.instance_id
		where csdi.instance_function <> 'SPENDING'
		      and md.casestudy_id=%s

		''' % self.casestudy_id, con=self.engine).apply(pd.to_numeric, errors='ignore')

        map_variables_v = pd.read_sql_query('''
		SELECT  mv.casestudy_id,
    			mv.id,
    			mv.variable_transform_id,
    			mv.id AS output_variable_id,
    			mv.variable AS variable_name,
    			mv.label AS variable_label,
    			CASE mvt.description
          			WHEN 'Log'::text THEN 1
          			ELSE 0
    			END AS use_log,
    			mv.type
   		FROM main_mapvariables as mv
   		join main_mapvariabletransform as mvt on mv.variable_transform_id::integer = mvt.id
		WHERE mv.type = 'endogenous'::text
		and casestudy_id=%s
		''' % self.casestudy_id, con=self.engine).apply(pd.to_numeric, errors='ignore')

        input_vs = pd.read_sql_query('select id from main_mapvariables where casestudy_id=%s' % self.casestudy_id,
                                     con=self.engine).apply(pd.to_numeric, errors='ignore')

        # input_vs_log=pd.read_sql_query('select id from main_mapvariables where use_log=1 and casestudy_id=%s'%self.casestudy_id, con=self.engine).apply(pd.to_numeric ,errors='ignore')
        # input_vs_log=pd.read_sql_query('select id from test_main_variables where use_log=1 and casestudy_id=%s'%self.casestudy_id, con=self.engine).apply(pd.to_numeric ,errors='ignore')
        input_vs_log = pd.read_sql_query('''
		select mv.id
		FROM main_mapvariables as mv
   		join main_mapvariabletransform as mvt on mv.variable_transform_id::integer = mvt.id
		WHERE mv.type = 'endogenous'::text 
		and variable_transform_id = (select id from main_mapvariabletransform where description='Log') 
		and casestudy_id=%s
		''' % self.casestudy_id, con=self.engine).apply(pd.to_numeric, errors='ignore')

        v_inverse_links = pd.read_sql_query('''
		SELECT mv.casestudy_id,
    		mv.id AS output_variable_id,
    		mvd.link_variable_id,
    		mvd.link_type
		FROM main_mapvariables as mv
		JOIN main_mapvariablesdetail mvd ON mvd.map_variables_id = mv.id
		WHERE mv.type = 'endogenous'::text 
		and link_type=-1 and casestudy_id=%s
		''' % self.casestudy_id, con=self.engine).apply(pd.to_numeric, errors='ignore')

        return map_data_a,map_variables_v,input_vs,input_vs_log,v_inverse_links
