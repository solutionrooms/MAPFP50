import pandas as pd

class ModelPrepare():
    def __init__(self):
        self.step1_variables = None
        self.variable_details = None

        self.step1_models = None
        self.output = {'success': False, 'message': ''}



    def prepare_models(self,map_data_a,map_variables_v,input_vs,v_inverse_links):

        map_data_a_dedup = map_data_a.drop(['variable_id', 'variable_value'], 1).drop_duplicates()
        map_data_a_pivot = map_data_a.pivot(index='id', columns='variable_id', values='variable_value').reset_index()
        map_data_b = pd.merge(map_data_a_dedup, map_data_a_pivot, how='left', on='id')

        map_variables_v['mergekey'] = 1
        map_data_b['mergekey'] = 1
        model_m_1 = pd.merge(map_variables_v, map_data_b, how='outer', on='mergekey')

        model_m_1['y'] = model_m_1.apply(lambda row: row[row['output_variable_id']], axis=1)

        for input_v in input_vs.values.tolist():
            model_m_1[input_v] = model_m_1.groupby(['geography_id', 'product_id', 'segment_id'])[input_v].shift(1)

        model_m_1 = model_m_1.fillna(method='bfill')

        # #log if needed
        # for input_v in input_vs_log.values.tolist():
        #   m[input_v[0]] = m[input_v[0]].map(lambda x:log(x))

        # invert where link is -1
        for output_variable_id_to_invert, input_variable_id_to_invert in v_inverse_links[
            ['output_variable_id', 'link_variable_id']].values.tolist():
            model_m_1.loc[model_m_1['output_variable_id'] == output_variable_id_to_invert, input_variable_id_to_invert] = 1 / model_m_1.loc[
                model_m_1['output_variable_id'] == output_variable_id_to_invert, input_variable_id_to_invert]

        # Testing few variables
        # print(m[['output_variable_id',402]][m['output_variable_id']==400])
        # print(m[['geography_id','product_id','segment_id',364]].groupby(['geography_id','product_id','segment_id']).head(4))

        # m.to_csv('step1_models_test.csv', sep='\t')

        return model_m_1,self.variable_details

    #### End Prepare Model for step1 #########

