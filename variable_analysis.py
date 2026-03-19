import numpy as np
import pandas as pd
import time
from datetime import datetime
import random
from pandas.api.types import CategoricalDtype
import matplotlib.pylab as plt
from joblib import load,dump
from sklearn.tree import DecisionTreeClassifier 
from sklearn import tree
from sklearn.model_selection import cross_val_score

def get_bins(val):
    global bin_cuts
    
    for i in range(len(bin_cuts)):
        pre=''
        if i+1<10:
            pre='0'
        if val<=bin_cuts[i]:
            if i == 0:
                return pre + str(i+1) + ': <=' + str(bin_cuts[i])
            else:
            
                return pre + str(i+1) + ': (' + str(bin_cuts[i-1]) + ',' + str(bin_cuts[i]) + ']'
    pre=''
    if i+2<10:
        pre='0'
    return pre + str(i+2) + ': >' + str(bin_cuts[-1])
	
def cal_stats(df,var,target,float_bin_cuts=[],float_bin_num=5,char_order=[],miss_loc='Right',method='dt',max_bin=5,cv_splits=3):
    global bin_cuts
    total_bad=df[target].sum()
    total_good=len(df)-total_bad
    
    var_df=df[[var,target]]
    
    if str in set([type(i) for i in var_df[var]]):
        var_type='str'
    else:
        var_type='float'
    
    #所有值都相同，且没有missing
    mask = (var_df[var].nunique() == 1) and (var_df[var].isna().sum()==0)
    if mask ==True:
        d1=pd.DataFrame(var_df.groupby(var).size())
        d1.rename(columns={0:'n'},inplace=True)
        d2=pd.DataFrame(var_df.groupby(var)[target].sum())
        d=d1.merge(d2,left_index=True,right_index=True)
        d.reset_index(inplace=True)
        d.columns = ['bin','n','bad']
        
        
    #所有值都是missing
    elif var_df[var].isna().mean() == 1:
        var_df[var]='Missing'
        d1=pd.DataFrame(var_df.groupby(var).size())
        d1.rename(columns={0:'n'},inplace=True)
        d2=pd.DataFrame(var_df.groupby(var)[target].sum())
        d=d1.merge(d2,left_index=True,right_index=True)
        d.reset_index(inplace=True)
        d.columns = ['bin','n','bad']
            
    
    else:
        if var_type=='float':
            nonmissing=var_df[~var_df[var].isna()]
            if method == 'qcut':
                
                if len(float_bin_cuts)==0:
                    q_list=[]
                    for i in range(1,float_bin_num):
                        q_list.append(i/float_bin_num)

                        bin_cuts=list(set(np.quantile(nonmissing[var],q_list)))
                else:
                    bin_cuts=float_bin_cuts

                bin_cuts.sort()
                #bin_cuts=[round(i,3) for i in bin_cuts]
                bin_cuts=sorted(set(round(i, 3) for i in bin_cuts))



                pd.options.mode.chained_assignment=None
                nonmissing['bin'] = list(map(get_bins,nonmissing[var]))
                pd.options.mode.chained_assignment='warn'


                d1=pd.DataFrame(nonmissing.groupby('bin').size())
                d1.rename(columns={0:'n'},inplace=True)
                d2=pd.DataFrame(nonmissing.groupby('bin')[target].sum())
                d=d1.merge(d2,left_index=True,right_index=True)
                d.reset_index(inplace=True)
                d['bin']=list(map(lambda x:str(x),d['bin']))
            
            if method == 'dt':
                #d = cut_dt(nonmissing,var, max_bin=max_bin,cv_splits=3,target='bad')
                var_tree = var + '_tree'
                score_ls = []     # here I will store the roc auc
                score_std_ls = [] # here I will store the standard deviation of the roc_auc
                #mask = data[var].notna()
                #nonmissing = nonmissing[mask][[col, target]]
                ###超参
                if len(nonmissing)<cv_splits:
                        d = pd.concat( [ \
                        nonmissing.groupby([var], dropna=False)[target].sum().to_frame('bad'), \
                        nonmissing.groupby([var], dropna=False)[target].count().to_frame('n'), \
                                              #],axis=1).sort_values(by=['保单数'], ascending=False)
                                              ],axis=1).sort_values(by=['n'], ascending=False)
                        d['bin'] = d.index
                        d.reset_index(drop=True,inplace=True)
                        d = d.reindex(columns=['bin',"n","bad"])
                else:        
                    for max_leaf_nodes in range(2,max_bin):
                        tree_model = DecisionTreeClassifier(max_leaf_nodes=max_leaf_nodes)
                        scores = cross_val_score(tree_model, nonmissing[[var]], nonmissing[target], cv=cv_splits, scoring='roc_auc')
                        score_ls.append(np.mean(scores))
                        score_std_ls.append(np.std(scores))
                    temp = pd.concat([pd.Series(range(2,max_bin)), pd.Series(score_ls), pd.Series(score_std_ls)], axis=1)
                    temp.columns = ['max_leaf_nodes', 'roc_auc_mean', 'roc_auc_std']
                    max_leaf_nodes = temp.sort_values(by=['roc_auc_mean','roc_auc_std'],ascending=[False,True]).iloc[0,0]
                    ### 参数
                    clf = DecisionTreeClassifier( \
                    max_leaf_nodes = max_leaf_nodes,
                    random_state=111, \
                    )
                    clf.fit(nonmissing[[var]], nonmissing[target])
                    ### predict
                    prob = pd.Series(1 - clf.predict_proba(nonmissing[[var]])[:,0],index=nonmissing.index)
                    leaf_id = clf.apply(nonmissing[[var]])
                    ### nonmissing.reindex(prob)
                    ###nonmissing[var_tree] = nonmissing.index.map(prob)
                    #nonmissing.loc[:,var_tree] = prob
                    nonmissing.loc[:,var_tree] = leaf_id
                    ### 分组统计
                    d = pd.concat( [ \
                                nonmissing.groupby([var_tree], dropna=False)[var].min().to_frame('左边界'), \
                                nonmissing.groupby([var_tree], dropna=False)[var].max().to_frame('右边界'), \
                                nonmissing.groupby([var_tree], dropna=False)[target].sum().to_frame('bad'), \
                                nonmissing.groupby([var_tree], dropna=False)[target].count().to_frame('n'), \
                              ],axis=1).sort_values(by=['左边界'])
                    d['bin'] = \
                        d.apply(lambda r:(str(r['左边界']) + '__' + str(r['右边界']) if r['左边界'] < r['右边界'] else r['左边界']), axis=1)
                    del d['左边界'],d['右边界']
                    d.reset_index(drop=True,inplace=True)
                    d = d.reindex(columns=["bin","n","bad"])

            missing=var_df[var_df[var].isna()]
            if len(missing)>0:
                i=len(d)
                d.loc[i,'bin']='Missing'
                d.loc[i,'n']=len(missing)
                d.loc[i,'bad']=missing[target].sum()
                
        else:
            var_df=var_df.fillna('Missing')
            
            if len(char_order)>0:
                var_df[var]=var_df[var].astype(CategoricalDtype(categories=char_order,ordered=True))
            
            d1=pd.DataFrame(var_df.groupby(var).size())
            d1.rename(columns={0:'n'},inplace=True)
            d2=pd.DataFrame(var_df.groupby(var)[target].sum())
            d=d1.merge(d2,left_index=True,right_index=True)
            d = d.reset_index()
            d.columns  = ['bin','n','bad']

            
    d['pct']=d['n']/len(df)*100
    d['bad_rate']=d['bad']/d['n']*100
    d['good']=d['n']-d['bad']
    d['bad_pct']=(d['bad']+0.0001)/total_bad*100
    d['good_pct']=(d['good']+0.0001)/total_good*100
    d['woe']=list(map(lambda x,y:np.log(x/y),d['bad_pct'],d['good_pct']))
    d['iv']=(d['bad_pct']/100-d['good_pct']/100)*d['woe']
    d['var_name'] = var
    d=d[["var_name","bin",'n','bad','bad_rate','pct','bad_pct','good_pct','woe','iv']]
		
    if miss_loc=='Left':
        d['rank']=range(len(d))
        #d.loc['Missing','rank']=-1
        d.loc[d['bin']=='Missing', 'rank']=-1
        d=d.sort_values('rank')
        del d['rank']
        
    return d
		

def cal_iv(df,var_list,target,float_bin_num=5,threshold=0,method='dt',max_bin=5,cv_splits=3):

    iv_list=[]
    iv_table_l = []
    
    if type(var_list)==str:
        v=[]
        v.append(var_list)
        var_list=v
    for var in var_list:
        print(var)
        stat=cal_stats(df,var,target,float_bin_num=float_bin_num,method=method,max_bin=max_bin,cv_splits=cv_splits)
        if type(stat)==str:
            print('输出的iv表格是这字符串，需要看看代码！！！',var)
            iv_list.append([var,0])
        else:
            iv_list.append([var,stat['iv'].sum()]) 
            iv_table_l.append(stat)
   
    iv_df=pd.DataFrame(iv_list)
    iv_df.rename(columns={0:'var',1:'iv'},inplace=True)
    iv_df.sort_values(by='iv',ascending=False,inplace=True)
    iv_df=iv_df[iv_df['iv']>threshold]
    iv_df.index=range(len(iv_df))
    
    iv_table = pd.concat(iv_table_l)
            
    return iv_df,iv_table