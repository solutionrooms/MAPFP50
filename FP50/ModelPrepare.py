import pandas as pd
import re
import numpy as np
# import datacompy
# import pickle
# pd.set_option('display.max_columns', 500)
import datetime
from test_FP50 import get_data
import time


def modelprepare(SAS_metrics, SAS_models,SAS_data,SAS_observations,SAS_dates):
	print("Starting")
	variables = SAS_metrics.copy()
	variables_num = len(variables) # getting length of data
	variables["variable"] = variables["metric"].apply(lambda x: "v" + str(x)) # {creating v{metric}}
	variables =  variables[["metric","variable","label"]].copy()
	vs = list(variables.variable.unique()) #Getting variable values in list
	models = SAS_models.copy()
	models_pivot = (models.pivot_table(index=['casestudy','metric',"type","dependence","log"], columns=['link_metric'], values='link_type')
	              .add_prefix('v')
	              .reset_index()
	              .rename_axis(None, axis=1)) # getting wide format from long
	variables = variables.merge(models_pivot, how="left", on ="metric").drop(["casestudy"],axis=1)
	variables["model"] = variables["variable"].apply(lambda x: re.sub("[^0-9]", "", x))
	variables = variables.sort_values("metric")
	variables["lag"] = 0
	# The for loop bellow mimics SAS Code lines 148-167
	for i in variables.metric:  
	    name = "v" + str(i)
	    if (variables.loc[variables["metric"]==i,name] == 1).bool():
	        variables.loc[variables["metric"]==i,"lag"] = 1 

	model1 = SAS_data.copy()
	# convert the sql-like structure to represent the requirements of the sas approach. 
	model2 = model1.loc[model1.instance.apply(lambda x: x in [0,1]),].drop(["casestudy","instance"], axis=1)
	observations = SAS_observations.copy()
	model3 = model2.merge(observations, how="left", on = "observation").drop("observation", axis=1)
	metrics = SAS_metrics[["metric","variable"]].copy()
	metrics["variable"] = metrics["metric"].apply(lambda x: "v" + str(x))
	model4 = model3.merge(metrics, how="left", on= "metric").drop("metric",axis=1)
	# The code bellow mimics PROC TRANSPOSE on line 220-224
	model4_pivot = (model4.pivot_table(index=['geography','segment',"product","date"], columns=['variable'], values="value")       
	              .reset_index()
	              .rename_axis(None, axis=1))
	# incorporate the modeling year information found on the dates table ... training, prior, current, or forecast. 
	dates = SAS_dates[["date","year"]].copy()
	model6 =  model4_pivot.merge(dates, how="left", on = "date")
	model6 = model6.loc[model6.year.apply(lambda x: x.lower() in ['primer','training','prior','current','forecast']),]
	model6 = model6.sort_values(by="date")
	model6["merger"] = "merger"

	print("Model under construction")

	# Getting the minimum Date
	firstdate = min(model6.date)

	# Getting dates and according ndates
	dates = pd.DataFrame(model6.loc[model6.year.apply(lambda x: x.lower() in ['primer','training','prior','current']),
	                        "date"].drop_duplicates()).sort_values("date")
	dates_num = len(dates)
	dates["ndate"] = list(range(1,dates_num+1))

	

	# SAS codes lines 301-317
	model7 = model6.merge(dates, how="left", on = "date")
	model7_melted = pd.melt(model7,id_vars=['segment',"date","geography","product","ndate","year","merger"],var_name='variable', value_name='y')
	model8 = model7_melted.merge(model7, how='left',on=['segment',"date","geography","product","ndate","year","merger"])
	rules1 = variables[["variable","type"]].copy()
	model9 = model8.merge(rules1, how="left",on="variable")
	# designate the status of endogenous verus exogenous.
	model10 = model9.loc[model9.type.apply(lambda x: x.lower()!="exogenous"),]
	ps = [i.replace("v","p") for i in vs]  # [p1, p2 , ...]

	# generate lags by moving the time period ahead one step.
	lags = model10.rename(index=str,columns = dict(zip(vs,ps)))
	lags["ndate"]= lags["ndate"]+1
	lags = lags.drop(["date","merger","year","y","type"],axis=1)
	model11 = model10.merge(lags, how="left", on = ["variable", "geography", "segment", "product","ndate"])


	print(".")
	# Creating new columns e{metric_name}
	rules2 = variables.copy()
	for i in rules2.metric:
	    name = "e" + str(i)
	    rules2[name] = 0
	print(".")
	#SAS code lines 379-393
	for i in rules2.metric:
	    name = "e" + str(i)
	    if rules2.loc[rules2.metric ==i ,"type"].any().lower() == "endogenous":
	        rules2.loc[rules2.metric ==i ,name] = 1 
	print(".")
	# PROC MEANS SAS code 395-398
	rules2_ = rules2.loc[:,rules2.columns[-variables_num:]].sum().reset_index().T.reset_index().drop("index",axis=1)
	headers = rules2_.iloc[0]
	rules2_1 = pd.DataFrame(rules2_.values[1:], columns=headers)
	rules2_1["merger"] = "merger"

	# endogenous variables are lagged to allow for forecasting capability. 
	# SAS CODE lines 400-430
	model12 = model11.merge(rules2_1,how='left',on='merger')
	for i in variables.metric:
	    model12["v"+str(i)].update(model12.loc[~(((model12["e"+str(i)] == 1) & (model12["date"] == firstdate)) | 
	                                           (model12["e"+str(i)]==0)),"p"+str(i)])  

	print(".")

	model13 = model12[["merger","variable","geography",
	               "segment","product","year","date","ndate","y"]+vs].copy()
	rs = [i.replace("v","r") for i in vs] # [r1, r2 , r3]
	rules3 = variables.rename(index=str,columns = dict(zip(vs,rs)))[["variable"]+rs].copy()
	model14 = model13.merge(rules3,how="left", on="variable")
	# account for relationships that are meant to be negative by taking the inverse 
	# (note, only valid for continuous variables).
	for i in variables.metric:
	    model14["v"+str(i)].update(1/model14.loc[model14["r"+str(i)]==-1,"v"+str(i)])

	print(".")

	#identify order of the dates.
	dates = pd.DataFrame(model14.loc[model14.year.apply(lambda x: x.lower() in ['primer','training','prior','current']),
	                        "date"].drop_duplicates()).sort_values("date")
	dates['order'] = np.arange(len(dates))+1
	order_num = len(dates)
	model15 = model14.merge(dates,how="left",on="date")
	model15 = model15.drop(["ndate"],axis=1)
	# Getting csv file
	model15.to_csv("model_prepare_output.csv")
	print("The file *model_prepare_output.csv* was created")
	

if __name__ == '__main__':
	start = time.time()
	_, _, _, _, _, SAS_metrics, SAS_models, SAS_data, SAS_observations, SAS_dates=get_data("test_FP50_v2.pickle")
	modelprepare(SAS_metrics,SAS_models,SAS_data,SAS_observations,SAS_dates)
	end = time.time()
	print("The execution time was",end-start,"seconds")
	
