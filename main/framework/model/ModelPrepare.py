import pandas as pd
import itertools
def keep(DataFrame, keep_these):
    drop_these = list(set(list(DataFrame)) - set(keep_these))
    return DataFrame.drop(drop_these, axis=1)

def cartesian(df1, df2):
    rows = itertools.product(df1.iterrows(), df2.iterrows())
    df = pd.DataFrame(left.append(right) for (_, left), (_, right) in rows)
    return df.reset_index(drop=True)

class ModelPrepare():
    def __init__(self):
        self.step1_variables = None
        self.variable_details = None

        self.step1_models = None
        self.output = {'success': False, 'message': ''}
        
    def prepare_models(self,p_metrics,p_models,p_data,p_observations,p_dates):
        variables = p_metrics.copy(deep=True)
        variables = variables.pipe(keep, ['metric', 'variable'])
        variable_list = variables['metric'].to_list()
        # variable_list = variables['variable'].str.slice(1, len(variables['variable'])).to_list()
        variables['model'] = variables['metric']
        variables = variables.set_index('metric')

        # from pickle file
        models = p_models.copy(deep=True)
        models['link_metric'] = 'v'+models['link_metric'].astype(str)
        models = models.pivot_table(index=['casestudy', 'metric', 'label', 'type','dependence','log'],columns='link_metric', values='link_type', aggfunc='first')
        models = models.reset_index()
        variables = pd.merge(variables, models, how='left', on='metric')

        variables['lag'] = 0

        # for j in range(1, len(variable_list)+1):
        for j in variable_list:
            variables.loc[(variables['v'+str(j)]==1)&(variables['model']==j),['lag']]=1

        to_keep = ['metric', 'variable', 'model','label', 'type', 'dependence', 'log', 'lag']
        variables['variable'] = (variables['metric']).map(lambda x: f'v{x}')

        selected_variables = ['v'+str(i) for i in variable_list]
        to_keep = to_keep+selected_variables
        variables = variables.pipe(keep, to_keep)

        # from pickle file
        model = p_data.copy(deep=True)
        model = model[model['instance'].isin([0, 1])]
        model = model.pipe(keep, ['observation', 'metric', 'value'])
        model = model.set_index('observation')

        # from pickle file
        observations = p_observations.copy(deep=True)
        observations = observations.pipe(
            keep, ['observation', 'geography', 'segment', 'product', 'date'])
        observations = observations.set_index('observation')

        # Merge model and observations
        model = pd.merge(model, observations, how='left', on='observation')
        model = model.pipe(keep, ['geography', 'segment',
                                'product', 'date', 'metric', 'value'])
        model = model.set_index('metric')

        # from pickle file
        metrics = p_metrics.copy(deep=True)
        metrics = metrics.set_index('metric')
        metrics = metrics.pipe(keep, ['metric', 'variable'])
        model = pd.merge(model, metrics, how='left', on='metric')
        model = model.reset_index()
        model['variable'] = (model['metric']).map(lambda x: f'v{x}')
        model = model.pivot_table(index=['geography', 'segment', 'product', 'date'],columns='variable', values='value', aggfunc='first')
        model = model.reset_index()
        model = model.set_index('date')

        # from pickle file
        dates = p_dates.copy(deep=True)
        dates = dates.set_index('date')
        dates = dates.pipe(keep, ['date', 'year'])
        model = pd.merge(model, dates, how='left', on='date')
        model['merger'] = 'merger'
        model = model.reset_index()
        model = model[model['year'].isin(
            ['PRIMER', 'TRAINING', 'PRIOR', 'CURRENT', 'FORECAST'])]

        dates = model[['date', 'year']]
        dates['date'] = pd.to_datetime(dates.date)
        dates = dates.drop_duplicates()
        dates = dates.sort_values(by='date')
        dates['ndate'] = range(1, dates.shape[0] + 1, 1)
        model = model.drop('year', axis=1)
        model['date'] = pd.to_datetime(model['date'])
        model = pd.merge(model, dates, how='left', on='date')
        print(model.shape)
        variable_n = pd.DataFrame(dict(variable=selected_variables))
        model = cartesian(model, variable_n)
        print(model.shape)
        y = pd.Series([])
        for index, row in model.iterrows():
            y = y.append(pd.Series(row[row['variable']]))

        y = pd.DataFrame({'y': y})
        y = y.reset_index()
        y = y.pipe(keep, ['y'])
        model = pd.concat([model, y], axis=1)
        rules1 = variables[['variable', 'type']]
        rules1 = rules1.set_index('variable')
        model = pd.merge(model, rules1, how='left', on='variable')
        print(model.shape)
        model = model[~model['type'].isin(['EXOGENOUS','exogenous'])]

        to_keep = ['variable', 'geography', 'segment',
                'product', 'ndate']+selected_variables
        lags = model[to_keep]
        lags['ndate'] = lags['ndate']+1
        lags_variables = ['variable', 'geography', 'segment','product', 'ndate']+['p'+str(i) for i in variable_list]
        lags.columns = lags_variables

        model = pd.merge(model, lags, how='left', on=['variable', 'geography', 'segment', 'product', 'ndate'])
        model = model[['merger', 'variable', 'geography', 'segment', 'product','year', 'date', 'y']+selected_variables+lags_variables]
        model = model.loc[:,~model.columns.duplicated()]
        print(model.shape)

        variables = variables.reset_index()
        rules2 = variables[['variable', 'type']]
        rules2['merger'] = 'merger'
        rules2_variables = ['e'+str(i) for i in variable_list]

        for i in rules2_variables:
            rules2[i] = 0

        for j in variable_list:
            rules2.loc[(rules2['type']=='endogenous')&(rules2['variable']=='v'+str(j)),['e'+str(j)]]=1

        rules2 = rules2.groupby(['merger'])[['e'+str(i) for i in variable_list]].sum()
        model = pd.merge(model, rules2, how='left', on=['merger'])
        print(model.shape)
        rules2_z = ['z'+str(i) for i in variable_list]

        for i in rules2_z:
            model[i] = 0

        for j in variable_list:
            model.loc[(model['e'+str(j)]==1)&(model['ndate']==1),['z'+str(j)]]=model['v'+str(j)]
            model.loc[(model['e'+str(j)]==1)&(model['ndate']!=1),['z'+str(j)]]=model['p'+str(j)]
            model.loc[(model['e'+str(j)]==0),['z'+str(j)]]=model['v'+str(j)]

        for j in variable_list:
            model['v'+str(j)] = model['z'+str(j)]

        model = model.pipe(keep, ['merger', 'variable', 'geography', 'segment', 'product','year', 'date','ndate', 'y']+selected_variables)
        print(model.shape)
        rules3 = variables[['variable']+selected_variables]
        rules3_variables = ['r'+str(i) for i in variable_list]

        for i in rules3_variables:
            rules3[i] = 0

        for j in variable_list:
            rules3['r'+str(j)] = rules3['v'+str(j)]

        rules3 = rules3[['variable']+rules3_variables]
        model = pd.merge(model, rules3, how='left', on=['variable'])

        rules3_z = ['z'+str(i) for i in variable_list]
        for i in rules3_z:
            model[i] = 0

        for j in variable_list:
            model.loc[(model['r'+str(j)]== -1),['z'+str(j)]]=1/(model['v'+str(j)])
            model.loc[(model['r'+str(j)]!= -1),['z'+str(j)]]=model['v'+str(j)]

        for j in variable_list:
            model['v'+str(j)] = model['z'+str(j)]

        model = model.pipe(keep, ['merger', 'variable', 'geography', 'segment', 'product','year', 'date','y']+selected_variables+rules3_variables)
        model_agg = model[model['year'].isin(['PRIMER', 'TRAINING', 'PRIOR', 'CURRENT'])]
        dates = pd.DataFrame({'count':model_agg.groupby(('date', 'year')).size(),'percent': (model_agg.groupby(('date', 'year')).size() / len(model_agg))*100})
        dates['order'] = range(1, dates.shape[0] + 1, 1)
        model = pd.merge(model, dates, how='left', on=['date'])
        model = model[['variable','y','merger','geography','segment','product','year','date']+selected_variables+rules3_variables+['order']]
        # model.to_csv('FP50_expected_output.csv',index = False)
        print(model.shape)
        return model,selected_variables
