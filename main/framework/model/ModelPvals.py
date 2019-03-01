from __future__ import print_function

import pandas as pd

try:
    from sklearn import linear_model
    from sklearn.metrics import mean_squared_error
except:
    pass
#from main.models import *


## class for all pvals steps
class PvalsTransformer():

    # The __init__ gets called first. This sets up and loads the input file as
    # specified
    def __init__(self):
        
        self.pvals_input = pd.DataFrame()

        self.output = []

    def prepare_pvals_from_non_fourier(self, fourier_output, mdd):

        # group by output_Variable, product, geo, segment
        for (group, df_gp) in fourier_output.groupby(
                ['geography_id', 'product_id', 'segment_id', 'output_variable_id']):
            # print (df_gp)
            gpsv = group
            group_rw = {
                'geography_id': gpsv[0],
                'product_id': gpsv[1],
                'segment_id': gpsv[2],
                'output_variable_id': gpsv[3]
            }

            # go through mdd df
            mdd_df = mdd[mdd['output_variable'] == group[3]]
            dependent_var_length = len(mdd_df.index)

            ##convert y of fourier output to columns
            y_values = df_gp['y_y'] / df_gp['fy']

            # print ('mdd df :: ', mdd_df)
            for index, row in mdd_df.iterrows():
                if row['link'] == 1:

                    rw = group_rw.copy()
                    rw['dependent_variable'] = row['link_variable']

                    _rw = df_gp[row['link_variable']].tolist()  # values.reshape(1,-1)
                    rw['saturation'] = max(_rw)

                    for i, item in enumerate(_rw):
                        rw['m%s' % i] = item

                    # print (' rw :: ' , rw)
                    self.pvals_input = self.pvals_input.append(rw, ignore_index=True)
            #         break
            # break

        # print ('pvals input df :: ', self.pvals_input)
        # break
        # print (output_df)
        self.pvals_input.to_csv('pvals_input.csv', sep='\t', encoding='utf-8')
        # from main.framework.ModelRunner import *

    def prepare_pvals_from_fourier(self, fourier_output, mdd):

        # group by output_Variable, product, geo, segment
        for (group, df_gp) in fourier_output.groupby(
                ['geography_id', 'product_id', 'segment_id', 'output_variable_id']):
            # print (df_gp)
            gpsv = group
            group_rw = {
                'geography_id': gpsv[0],
                'product_id': gpsv[1],
                'segment_id': gpsv[2],
                'output_variable_id': gpsv[3]
            }

            # go through mdd df
            mdd_df = mdd[mdd['output_variable'] == group[3]]
            dependent_var_length = len(mdd_df.index)

            ##convert y of fourier output to columns
            y_values = df_gp['y_y'] / df_gp['fy']

            # print ('mdd df :: ', mdd_df)
            for index, row in mdd_df.iterrows():
                if row['link'] == 1:

                    rw = group_rw.copy()
                    rw['dependent_variable'] = row['link_variable']

                    _rw = df_gp[row['link_variable']].tolist()  # values.reshape(1,-1)
                    rw['saturation'] = max(_rw)

                    for i, item in enumerate(_rw):
                        rw['m%s' % i] = item

                    # print (' rw :: ' , rw)
                    self.pvals_input = self.pvals_input.append(rw, ignore_index=True)
            #         break
            # break

        # print ('pvals input df :: ', self.pvals_input)
        # break
        # print (output_df)
        self.pvals_input.to_csv('pvals_input.csv', sep='\t', encoding='utf-8')
        # from main.framework.ModelRunner import *

    def pvals_to_mc(self):
        #1-input
        pass


        #2-averages
        #3-applies averages
        #4-flipped data


# pt = PvalsTransformer()
# pt.prepare_pvals_from_fourier()

'''
from main.framework.PvalsTransformer import *

'''