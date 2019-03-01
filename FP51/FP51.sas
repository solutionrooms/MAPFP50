

%let infile=test database 2019.01.22.xlsx;
%let library=FP51\;
%let library=C:\code\MAPversion0.801\;
%let study=SOLUTIONROOMS TEST - AIRLINES;

* comment: explain periodicity to code ... 0 = quarterly, 1 = monthly, 2 = weekly, or 3 = daily. *;
*%let periodicity=1;
* comment: control design of coefficients and steps run ... 1 = calculate models, 0 = apply models in simulation. *;
%let model=1;
%let do_nlp_part=0;
* comment: establish the level of bivariate correlation acceptable to include independent variables in the model ... values are generally between 0.05 and 0.35. *;
*%let tolerance=0.05;
* comment: name output file for scenario results. *;
*%let scenario=v23;

/*
comment: variables for simulation "zeroing out" ... see line 55 below.

v17 CABLE TELEVISION
v18 NETWORK TELEVISON
v19 MAGAZINES
v20 NEWSPAPERS
v21 OUTDOOR
v22 RADIO
v23 INTERNET
*/

* to do 1: evaluate random effects to PROC MCMC models for borrowing (shrinkage) across geographies, segments, and products ... open for PyMC3/4. *;
* to do 2: utilize previous models as priors for reaccuring coefficients ... might be tricky since model form will often change. *;
* to do 3: test Fourier transformation to forecast errors. *;
* to do 4: produce forecasts and convidence intervals for forecasts. *;

run;

* MACRO scenario: define scenarios that are processed in the body of the code. *;

%macro scenario;

/*%if &model=0 %then %do;*/
/**/
/*data model(index=(merger));*/
/*     keep merger geography segment product year date v1-v&variables;*/
/*     set model;*/
/**/
/** comment: define simulations in the form of IF-THEN-DO statements associated with the dimensions and measures. *; */
/**/
/*     &scenario=0;*/
/**/
/*%end;*/

%mend scenario;

run;

* comment: avoid clogging up log window by moving output to a permanent file. *;

%macro logoutput;

%if &model=1 %then %do;

data _null_;

     log1="model build";
     log2=right(trim(log1))||'.log';
     call symput('logfile',left(trim(log2)));

run;

%put BUILDING MODEL;

%end;

/*%if &model=0 %then %do;*/
/**/
/*data _null_;*/
/**/
/*     log1="&scenario";*/
/*     log2=right(trim(log1))||'.log';*/
/*     call symput('logfile',left(trim(log2)));*/
/**/
/*run;*/
/**/
/*%put RUNNING SCENARIO &scenario; */
/*%end;*/


* comment: supressing the LOG is removed from the SAS to SQL exercise. *;

*roc printto log="&library\&logfile";

run;

%mend logoutput;

run;

%logoutput

run;

libname library "&library\SAS Datasets";

run;

* comment: import raw data in XLSX format. *;
* comment: THIS IS NEW! *;

proc import datafile="&library\&infile" dbms=XLSX out=variables replace;
     sheet='metrics';
     getnames=yes;

run;

* comment: define the number of variables in the file. *;

data _null_;
     set variables;
     retain variables 0;

     variables+1;

     call symput('variables',left(trim(variables)));

run;

* comment: convert the sql-like structure to represent the requirements of the sas approach. *;

data variables(index=(metric));
     keep metric variable label;
	 set variables;

proc import datafile="&library\&infile" dbms=XLSX out=models replace;
     sheet='models';
     getnames=yes;

run;

data variables(index=(metric));
     keep metric variable label type dependence log v1-v&variables;
	 merge variables(in=in1) models(in=in2);
     by metric;

     if in1;

proc datasets nolist;
     delete models;

data variables;
     format model $char12. label $char144.;
     keep variable model label type dependence log lag v1-v&variables;
     set variables;

     model=substr(variable,2,length(variable)-1);

     array v(i) v1-v&variables;

     do i=1 to &variables;

        if i=model then do;

           if v=1 then lag=1; else lag=0;

        end;

     end;

run;

* comment: import raw data in XLSX format. *;
* comment: THIS IS NEW! *;

proc import datafile="&library\&infile" dbms=XLSX out=model replace;
     sheet='data';
     getnames=yes;

run;

* comment: convert the sql-like structure to represent the requirements of the sas approach. *;

data model(index=(observation));
     keep observation metric value;
     set model(where=(instance in(0,1)));

* comment: keep marketing activity (instance=1) and non-marketing activity (instance=0), but not spending (instance=2). *;

proc import datafile="&library\&infile" dbms=XLSX out=observations replace;
     sheet='observations';
     getnames=yes;

run;

data observations(index=(observation));
     keep observation geography segment product date;
     set observations;

data model(index=(metric));
     keep geography segment product date metric value;
     merge model(in=in1) observations(in=in2);
	 by observation;

	 if in1;

proc import datafile="&library\&infile" dbms=XLSX out=metrics replace;
     sheet='metrics';
     getnames=yes;

run;

data metric(index=(metric));
     keep metric variable;
     set metrics;

data model(index=(group=(geography segment product date variable)));
     keep geography segment product date variable value;
     merge model(in=in1) metrics(in=in2);
	 by metric;

	 if in1;

proc transpose data=model out=model;
     var value;
	 id variable;
	 idlabel variable;
	 by geography segment product date;

data model(index=(date));
     keep geography segment product date v1-v&variables;
     set model;

* comment: incorporate the modeling year information found on the dates table ... training, prior, current, or forecast. *;

proc import datafile="&library\&infile" dbms=XLSX out=dates replace;
     sheet='dates';
     getnames=yes;

run;

data dates(index=(date));
     keep date year;
	 set dates;

data model(index=(merger));
     format merger geography segment product year $char144. date mmddyy8. v1-v&variables best16.;
     keep merger geography segment product year date v1-v&variables;
     merge model(in=in1) dates(in=in2);
	 by date;

	 if in1;

     merger='merger';

	 if lowcase(year) in('primer','training','prior','current','forecast') then output;

proc datasets nolist;
     delete observations metrics dates;

* comment: generate changes in the marketing mix leveraging the sceanrio macro above. *;

run;

%scenario

run;

* MACRO prepare: processes all of the transformations required to both model and simulate. *;

%macro prepare;
/**/
/*proc sort data=model force;*/
/*     by date;*/
/**/
/** comment: identify the first date. *;*/
/**/
/*proc means data=model noprint;*/
/*     var date;*/
/*     output out=firstdate min=firstdate;*/
/**/
/*data _null_;*/
/*     set firstdate;*/
/**/
/*     call symput('firstdate',left(trim(firstdate)));*/
/**/
/*proc datasets nolist;*/
/*     delete firstdate;*/
/**/
/** comment: identify order of the dates. *;*/
/**/
/*proc freq data=model(where=(lowcase(year) in('primer','training','prior','current'))) noprint;*/
/*     tables date*year / out=dates;*/
/**/
/*data dates(index=(date));*/
/*     keep date year ndate;*/
/*     set dates;*/
/*     retain ndate 0;*/
/**/
/*     ndate+1;*/
/*     call symput('dates',left(trim(ndate)));*/
/**/
/*run;*/
/**/
/*data model(index=(variable));*/
/*     format variable $char6. y best16.;*/
/*     keep merger variable geography segment product year date ndate y v1-v&variables;*/
/*     merge model(in=in1) dates(in=in2);*/
/*     by date;*/
/**/
/*     if in1;*/
/**/
/*     array v(i) v1-v&variables;*/
/**/
/*     do i=1 to &variables;*/
/**/
/*        y=v;*/
/*        variable='v'||left(trim(i));*/
/*        output;*/
/**/
/*     end;*/
/**/
/** comment: designate the status of endogenous verus exogenous. *;*/
/**/
/*data rules1(index=(variable));*/
/*     keep variable type;*/
/*     set variables;*/
/**/
/*data model(index=(group=(variable geography segment product ndate)));*/
/*     keep merger variable type geography segment product year date ndate y v1-v&variables;*/
/*     merge model(in=in1) rules1(in=in2);*/
/*     by variable;*/
/**/
/*     if in1;*/
/**/
/*     if lowcase(type)='exogenous' then delete;*/
/**/
/*proc datasets nolist;*/
/*     delete rules2;*/
/**/
/** comment: generate lags by moving the time period ahead one step. *;*/
/**/
/*data lags(index=(group=(variable geography segment product ndate)));*/
/*     keep variable geography segment product ndate p1-p&variables;*/
/*     set model;*/
/**/
/*     ndate=ndate+1;*/
/**/
/*     array v(i) v1-v&variables;*/
/*     array p(i) p1-p&variables;*/
/**/
/*     do i=1 to &variables;*/
/**/
/*        p=v;*/
/**/
/*     end;*/
/**/
/*data model(index=(merger));*/
/*     keep merger variable geography segment product year date ndate y v1-v&variables p1-p&variables;*/
/*     merge model(in=in1) lags(in=in2);*/
/*     by variable geography segment product ndate;*/
/**/
/*     if in1;*/
/**/
/** comment: fill in first observation with lag. *;*/
/**/
/*     array v(i) v1-v&variables;*/
/*     array p(i) p1-p&variables;*/
/*     array z(i) z1-z&variables;*/
/**/
/*     do i=1 to &variables;*/
/**/
/*        if date=&firstdate then z=.; else z=p;*/
/*        p=z;*/
/**/
/*     end;*/
/**/
/*proc datasets nolist;*/
/*     delete lags;*/
/**/
/** comment: designate the status of endogenous verus exogenous. *;*/
/**/
/*data rules2(index=(merger));*/
/*     format variablei best16.;*/
/*     keep merger variable type e1-e&variables;*/
/*     set variables;*/
/**/
/*     merger='merger';*/
/*     variablei=substr(variable,2,length(variable)-1);*/
/**/
/*     array e(i) e1-e&variables;*/
/**/
/*     do i=1 to &variables;*/
/**/
/*        if i=variablei and lowcase(type)='endogenous' then e=1; else e=0;*/
/**/
/*     end;*/
/**/
/*proc means data=rules2 noprint;*/
/*     var e1-e&variables;*/
/*     output out=rules2 sum=e1-e&variables;*/
/*     by merger;*/
/**/
/*data model(index=(group=(variable geography segment product)));*/
/*     keep merger variable geography segment product year date ndate y v1-v&variables;*/
/*     merge model(in=in1) rules2(in=in2);*/
/*     by merger;*/
/**/
/*     if in1;*/
/**/
/** comment: endogenous variables are lagged to allow for forecasting capability. *;*/
/**/
/*     array v(i) v1-v&variables;*/
/*     array p(i) p1-p&variables;*/
/*     array e(i) e1-e&variables;*/
/*     array t(i) z1-z&variables;*/
/**/
/*     do i=1 to &variables;*/
/**/
/*        if e=1 then do;*/
/**/
/*           if date=&firstdate then z=v; else z=p;*/
/**/
/*        end;*/
/**/
/*        if e=0 then do;*/
/**/
/*           z=v;*/
/**/
/*        end;*/
/**/
/*        v=z;*/
/**/
/*     end;*/
/**/
/*proc datasets nolist;*/
/*     delete rules2;*/
/**/
/** comment: capture the modeling rules for the endogenous variables. *;*/
/**/
/*data rules3(index=(variable));*/
/*     keep variable r1-r&variables;*/
/*     set variables;*/
/**/
/*     array v(i) v1-v&variables;*/
/*     array r(i) r1-r&variables;*/
/**/
/*     do i=1 to &variables;*/
/**/
/*        r=v;*/
/**/
/*     end;*/
/**/
/*data model(index=(date));*/
/*     keep merger variable geography segment product year date y v1-v&variables r1-r&variables;*/
/*     merge model(in=in1) rules3(in=in2);*/
/*     by variable;*/
/**/
/*     if in1;*/
/**/
/** comment: account for relationships that are meant to be negative by taking the inverse (note, only valid for continuous variables). *;*/
/**/
/*     array v(i) v1-v&variables;*/
/*     array r(i) r1-r&variables;*/
/*     array z(i) z1-z&variables;*/
/**/
/*     do i=1 to &variables;*/
/**/
/*        if r=-1 then z=1/v; else z=v;*/
/*        v=z;*/
/**/
/*     end;*/
/**/
/** comment: identify order of the dates. *;*/
/**/
/*proc freq data=model(where=(lowcase(year) in('primer','training','prior','current'))) noprint;*/
/*     tables date*year / out=dates;*/
/**/
/*data dates(index=(date));*/
/*     keep order date year;*/
/*     set dates;*/
/*     retain order 0;*/
/**/
/*     order+1;*/
/*     call symput('dates',left(trim(order)));*/
/**/
/*data model(index=(group=(variable geography segment product date)));*/
/*     keep merger variable geography segment product year date order y v1-v&variables r1-r&variables;*/
/*     merge model(in=in1) dates(in=in2);*/
/*     by date;*/
/**/
/*     if in1;*/
/**/
/*data _null_;*/
/*     set dates;*/
/**/
/*     first=2;*/
/*     last=&dates;*/
/*     total=(last-first)+1;*/
/**/
/*     call symput('first',left(trim(first)));*/
/*     call symput('last',left(trim(last)));*/
/*     call symput('total',left(trim(total)));*/
/**/
/*run;*/
/**/
/*data library.keepmodelFP50;set model;run;*/

* comment: generate carry-over effect with build and decay parameters based on the Jim Friedman PVAL approach. *;

%macro pval;
*retrieve data from fp50;
data model; set library.keepmodelFP50;run;
proc sort data=model;
by variable geography segment product;
run;

*same code as in rules2 for FP50;
data rules2(index=(merger));
     format variablei best16.;
     keep merger variable type e1-e&variables;
     set variables;

     merger='merger';
     variablei=substr(variable,2,length(variable)-1);

     array e(i) e1-e&variables;

     do i=1 to &variables;

        if i=variablei and lowcase(type)='endogenous' then e=1; else e=0;

     end;
*same code as in rules for FP50;
data rules3(index=(variable));
     keep variable r1-r&variables;
     set variables;

     array v(i) v1-v&variables;
     array r(i) r1-r&variables;

     do i=1 to &variables;

        r=v;

     end;


%if &model=1 %then %do;

proc transpose data=rules3 out=pval;
     var r1-r&variables;
     by variable;

data pval;
     format variable independent $char6.;
     keep variable independent;
     set pval;

     independent='v'||left(substr(_NAME_,2,length(_NAME_)-1));
     if abs(COL1)=1 then output;

proc freq data=model noprint;
     tables geography*segment*product / out=geographies;

data geographies(index=(order));
     format geography segment product $char144.;
     keep geography segment product order;
     set geographies;
     retain order 0;

     order+1;
     call symput('geographies',left(trim(order)));

run;

data pval(index=(order));
     keep variable independent order;
     set pval;

     do order=1 to &geographies;

        output;

     end;

data pval(index=(group=(variable geography segment product)));
     keep variable geography segment product independent;
     merge pval(in=in1) geographies(in=in2);
     by order;

     if in1;

proc transpose data=model out=response prefix=y; 
     var y;
     by variable geography segment product;

data pval(index=(group=(variable geography segment product independent)));
     keep pval variable independent geography segment product y1-y&dates;
     merge pval(in=in1) response(in=in2);
     by variable geography segment product;
     retain pval 0;

     if in1;

     pval+1;
     call symput('pvals',left(trim(pval)));

run;

%macro independents;

%do pval=1 %to &pvals;

data _null_;
     set pval(where=(pval=&pval));

     call symput('independent',left(trim(independent)));
     call symput('ivariable',left(trim(variable)));
     call symput('igeography',left(trim(geography)));
     call symput('isegment',left(trim(segment)));
     call symput('iproduct',left(trim(product)));

run;

proc transpose data=model(where=(variable="&ivariable" and geography="&igeography" and segment="&isegment" and product="&iproduct")) out=i&pval prefix=m; 
     var &independent;
     by variable geography segment product;

data i&pval;
     format variable $char6. geography segment product $char144. independent $char6. pval m1-m&dates best16.;
     keep pval variable independent geography segment product m1-m&dates;
     set i&pval;

     pval=&pval;
     independent="&independent";

%if &pval=1 %then %do;

data independent;
     keep pval variable independent geography segment product m1-m&dates;
     set i&pval;

proc datasets nolist;
     delete i&pval;

%end;

%if &pval>1 %then %do;

data independent;
     keep pval variable independent geography segment product m1-m&dates;
     set independent i&pval;

proc datasets nolist;
     delete i&pval;

%end;

%if &pval=&pvals %then %do;

proc sort data=independent force;
     by variable geography segment product independent;

run;

* comment: set the PVAL index to the a set percentile of activity instead of the original 100 GRPs. *;

data pval(index=(group=(variable independent geography segment product)));
     keep pval variable independent geography segment product y1-y&dates saturation m1-m&dates;
     merge pval(in=in1) independent(in=in2);
     by variable geography segment product independent;

     if in1;

     saturation=max(of m1-m&dates);

proc datasets nolist;
     delete independent;

%end;

%end;

%mend independents;

run;

%independents

run;


%if &do_nlp_part=1 %then
%do;

/*proc nlp data=pval out=correlations noprint;*/
/*     by variable geography segment product independent;*/
/**/
/*     array m[&dates] m1-m&dates;*/
/*     array x[&dates] x1-x&dates;*/
/**/
/*     array y[&dates] y1-y&dates;*/
/*     array xs[&dates] xs1-xs&dates;*/
/*     array ys[&dates] ys1-ys&dates;*/
/*     array xy[&dates] xy1-xy&dates;*/
/**/
/*     max correlation;*/
/**/
/*     parms learn=0.30, decay=0.30, reference=1.00;*/
/*     bounds 0.10<=learn<=0.60, 0.10<=decay<=0.60, 0.2<=reference<=5.0;*/
/**/
/*     do i=1 to &dates;*/
/**/
/*        if i=1 then x[i]=0; else x[i]=1-(1-x[i-1]*exp(-decay))/exp((m[i]/(saturation*reference))*learn);*/
/**/
/*        xs[i]=x[i]*x[i];*/
/*        ys[i]=y[i]*y[i];*/
/*        xy[i]=x[i]*y[i];*/
/**/
/*     end;*/
/**/
/*     n1=sum(of xy&first-xy&last); */
/*     n2=(sum(of x&first-x&last)*sum(of y&first-y&last))/&total; */
/*     n=n1-n2;*/
/*     d1=sum(of xs&first-xs&last)-((sum(of x&first-x&last)*sum(of x&first-x&last))/&total);*/
/*     d2=sum(of ys&first-ys&last)-((sum(of y&first-y&last)*sum(of y&first-y&last))/&total);*/
/*     d=sqrt(d1*d2);*/
/*     correlation=n/d;*/
/**/
/*run;*/
%end;
%else 
%do;
data correlations;set LIBRARY.CORRELATIONS_NLP;run;
%end;
* added these 2 sorts = maybe a wps thing;
proc sort data=pval force;
by variable geography segment product independent;
run;
proc sort data=correlations force;
by variable geography segment product independent;
run;

data correlations(index=(group=(variable geography segment product independent)));
     keep variable geography segment product independent learn decay reference correlation;
     set correlations;

data pval(index=(group=(variable independent)));
     keep pval variable geography segment product independent saturation reference learn decay correlation;
     merge pval(in=in1) correlations(in=in2);
     by variable geography segment product independent;

     if in1;

proc means data=pval(where=(correlation>0)) noprint;
     var saturation reference learn decay;
     output out=means mean=msaturation mreference mlearn mdecay;
     by variable independent;

data pval(index=(group=(variable independent geography segment product)));
     keep pval variable geography segment product independent saturation reference learn decay correlation;
     merge pval(in=in1) means(in=in2);
     by variable independent;

     if in1;

     if correlation<0 or saturation=0 then do;

        saturation=msaturation;
        reference=mreference;
        learn=mlearn;
        decay=mdecay;

     end;

proc datasets nolist;
     delete means;

* comment: flip pval learn and decay measures so they directly can be applied to the data stream. *;

data pval(index=(group=(variable geography segment product)));
     format variable $char6. geography segment product $char144. independenti c1-c&variables l1-l&variables d1-d&variables m1-m&variables best16.;
     keep variable geography segment product c1-c&variables l1-l&variables d1-d&variables m1-m&variables;
     set pval;

     independenti=substr(independent,2,length(independent)-1);

     array c(i) c1-c&variables;
     array l(i) l1-l&variables;
     array d(i) d1-d&variables;
     array m(i) m1-m&variables;

     do i=1 to &variables;

        if i=independenti then do;       

           c=correlation;
           l=learn;
           d=decay;
           m=(saturation*reference);

        end;

    end;

proc means data=pval noprint;
     var c1-c&variables l1-l&variables d1-d&variables m1-m&variables;
     output out=pval sum=c1-c&variables l1-l&variables d1-d&variables m1-m&variables;
     by variable geography segment product;

data library.pvals;
     keep variable geography segment product c1-c&variables l1-l&variables d1-d&variables m1-m&variables;
     set pval;

     order=substr(variable,2,length(variable)-1);

     array c(i) c1-c&variables;
     array l(i) l1-l&variables;
     array d(i) d1-d&variables;
     array m(i) m1-m&variables;

     do i=1 to &variables;

        if c=. or c<=0 then c=0; 
        if l=. then l=0;
        if d=. then d=0;
        if m=. then m=0;

    end;

proc sort data=library.pvals force;
     by variable geography segment product;

proc export data=library.pvals outfile="&library\sas datasets\pvals.csv" dbms=CSV replace;

proc datasets nolist;
     delete pval;

run;

%end;

%mend pval;

run;

%pval

run;

proc datasets nolist;
     delete dates rules3;

data model(index=(date));
     keep merger variable geography segment product year date y v1-v&variables r1-r&variables c1-c&variables l1-l&variables d1-d&variables m1-m&variables;
     merge model(in=in1) library.pvals(in=in2);
     by variable geography segment product;

     if in1;

data _null_;

     variablesplusone=&variables+1;
     variablesplustwo=&variables+2;

     call symput('variablesplusone',left(trim(variablesplusone)));
     call symput('variablesplustwo',left(trim(variablesplustwo)));

run;

%macro timeseries;

proc freq data=model noprint;
     tables date / out=timeseries;

data _null_;
 
     if &periodicity=0 then timeseries=5; 
     if &periodicity=1 then timeseries=13; 
     if &periodicity=2 then timeseries=13;
     if &periodicity=3 then timeseries=20;

     variablesstretch=&variables+timeseries;

     call symput('timeseries',left(trim(timeseries)));
     call symput('variablesstretch',left(trim(variablesstretch)));

run;

%if &timeseries=5 %then %do; 

data timeseries(index=(date));
     keep date v&variablesplusone-v&variablesstretch;
     set timeseries;
     retain v&variablesplusone 0;

     v&variablesplusone+1;

     array t(i) v&variablesplustwo-v&variablesstretch;

     do i=1 to 4;

        if i=qtr(date) then t=1; else t=0;

     end;

%end;

%if &timeseries=13 %then %do; 

data timeseries(index=(date));
     keep date v&variablesplusone-v&variablesstretch;
     set timeseries;
     retain v&variablesplusone 0;

     v&variablesplusone+1;

     array t(i) v&variablesplustwo-v&variablesstretch;

     do i=1 to 12;

        if i=month(date) then t=1; else t=0;

     end;

%end;

%if &timeseries=20 %then %do;

data _null_;

      timeseriesstageone=&variables+13;
      timeseriesstagetwo=&variables+14;

      call symput('timeseriesstageone',left(trim(timeseriesstageone)));
      call symput('timeseriesstagetwo',left(trim(timeseriesstagetwo)));

run;

data timeseries(index=(date));
     keep date v&variablesplusone-v&variablesstretch;
     set timeseries;
     retain v&variablesplusone 0;

     v&variablesplusone+1;

     array ti(i) v&variablesplustwo-v&timeseriesstageone;

     do i=1 to 12;

        if i=month(date) then ti=1; else ti=0;

     end;

     array tj(j) v&timeseriesstagetwo-v&variablesstretch;

     do j=1 to 7;

        if j=weekday(date) then tj=1; else tj=0;

     end;

%end;

run;

%mend timeseries;

run;

%timeseries

run;

data model;
     keep merger variable geography segment product year date y v1-v&variablesstretch r1-r&variables c1-c&variables l1-l&variables d1-d&variables m1-m&variables;
     merge model(in=in1) timeseries(in=in2);
     by date;

     if in1;

proc datasets nolist; 
     delete timeseries;

run;

%mend prepare;

run;

%prepare;
proc export data=work.model outfile="&library\sas datasets\model_output_prepare.csv" dbms=CSV replace;

