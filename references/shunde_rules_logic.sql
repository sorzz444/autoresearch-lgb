--par1
--666552
--select count(1) from SASSJLIB.td003649_y1rc2_dfzh_info;
drop table if exists SASSJLIB.td003649_y1rc2_dfzh_info;
create table SASSJLIB.td003649_y1rc2_dfzh_info
as
SELECT T1.AR_NO
,T.ORI_DEAL_NUM --流水号
,T.SUB_DEAL_NUM --子流水号
,T.TX_DATE --交易日期
,T1.TX_TIME --交易时间
,T.DETAIL_NUM --流水序号
,A.CUST_ID --客户号
,A.ACCOUNT_NAME --客户名称
,T.ACCOUNT_ID --实体账号
,T.CURRENCY_CD --交易币种
,T.TX_AMT --交易金额
,T.TX_BAL --交易后余额
,T.DEBIT_CREDIT_CD --借贷方向
,T.CASH_TRAN_TYPE_CD --1-现金,2-转账
,CASE
WHEN JR.TX_CODE in ('CUPI203','CUPI404','CUPI405','CUPI501','CUPI502') THEN '010601'
WHEN JR.TX_CODE in ('CUPI101','CUPI201','CUPI206','CUPI209') THEN '010602'
ELSE T.CHANNEL_TYPE_CD
END CHANNEL_TYPE_CD --渠道号
,T.TX_ORG_ID --交易机构
,T.TX_TELLER_ID --交易柜员
,T.ORI_TX_CD --交易码
,T.ABSTRACT_CD --交易摘要代码
,T.ABSTRACT_DESC --交易摘要描述
,T.REMARK --交易备注
,CASE WHEN T.DEBIT_CREDIT_CD='C' AND T.SPON_SYS_CD='NGOATS' THEN JR.DEBIT_ACCT_ID WHEN T.DEBIT_CREDIT_CD='D' AND T.SPON_SYS_CD='NGOATS' THEN JR.CREDIT_ACCT_ID ELSE T1.TRGT_ACCT_NO END AS TRGT_ACCT_NO1 --对手账号
,CASE WHEN T.DEBIT_CREDIT_CD='C' AND T.SPON_SYS_CD='NGOATS' THEN JR.DEBIT_ACCT_NAME WHEN T.DEBIT_CREDIT_CD='D' AND T.SPON_SYS_CD='NGOATS' THEN JR.CREDIT_ACCT_NAME ELSE T1.TRGT_ACCT_NM END AS TRGT_ACCT_NM1 --对手名称
,CASE WHEN T.DEBIT_CREDIT_CD='C' AND T.SPON_SYS_CD='NGOATS' THEN JR.DEBIT_BANK_NO WHEN T.DEBIT_CREDIT_CD='D' AND T.SPON_SYS_CD='NGOATS' THEN JR.CREDIT_BANK_NO ELSE T1.TRGT_ACCT_BANK_NO END AS TRGT_ACCT_BANK_NO --对手行号
,CASE WHEN T.DEBIT_CREDIT_CD='C' AND T.SPON_SYS_CD='NGOATS' THEN JR.DEBIT_BANK_NAME WHEN T.DEBIT_CREDIT_CD='D' AND T.SPON_SYS_CD='NGOATS' THEN JR.CREDIT_BANK_NAME ELSE T1.TRGT_ACCT_BANK_NM END AS TRGT_ACCT_BANK_NM --对手行名
,CASE
WHEN ORG.BANK_ORG_NO IS NOT NULL THEN 1
WHEN T1.TRGT_ACCT_BANK_NM LIKE '%顺德农%商%' THEN 1
WHEN LEFT(T1.TRGT_ACCT_NO,1)='3' AND LENGTH(T1.TRGT_ACCT_NO)=22 THEN 1
WHEN LEFT(NVL(T1.TRGT_CUST_ID,''),1) NOT IN ('1','3','6','7') AND NVL(T1.TRGT_ACCT_NO,'')<>'' THEN 0
WHEN (NVL(T1.TRGT_ACCT_BANK_NO,'')<>'' OR NVL(T1.TRGT_ACCT_BANK_NM,'')<>'') THEN 0
ELSE 1
END TRGT_IS_MYBANK --对手是否我行 1-是，0-否
,CASE
WHEN LEFT(T1.TRGT_CUST_ID,1) IN ('1','3') THEN '单位'
WHEN LEFT(T1.TRGT_CUST_ID,1) IN ('6','7') THEN '个人'
WHEN TRIM(TRGT_ACCT_NM1) ~'^\d: |-' THEN '单位'
WHEN TRIM(TRGT_ACCT_NM1) ~'^' AND TRIM(TRGT_ACCT_NM1) ~'^[A-Z][A-Z]' THEN '单位'
WHEN BIT_LENGTH(TRGT_ACCT_NM1)/24>4 AND TRGT_ACCT_NM1 ~ '公司|店|部|[a-z]|[A-Z]|顺德|湘|粤|村|包|火锅|面|煲|粥|饭|麻辣|小吃|海鲜|菠萝包|菜|茶|肉|潮汕|风味|烧|烤|糖|汤|粉|餐|所|社|冰舍|铺|馆|清真|自助|时光|食都|酒家|料理|定制|超市|咖啡|手作|记|寿司|\+' THEN '个人'
WHEN BIT_LENGTH(TRGT_ACCT_NM1)/24>3 AND REGEXP_LIKE(TRGT_ACCT_NM1,'抖音零钱提现红包活动收益|今日头条|番茄小说|番茄畅听|公司|企业|商行|贸易|工厂|学校|大学|学院|中学|小学|幼儿园|少年宫|超市|公园|动物园|文化遗产|广场|电视|广播|银行|银联|保险|房地产|置业|证券|基金|信托|期货|款项付款|资金代理|合伙|独资|个体工商户|企业年金计划|慈善协会|颐养院|养老院|研究院|事务所|监测|合作社|医院|公共资源|国库|出版社|协会|学会|委员会|党|队|团|村|居|分局|派出所|中心|站|院|APPIAPPI+\ICBCILO|FTERICOFFE|CAFE|LAB|APPLE|APPLE|OFFICE|PAY|BANK|BANK|STORE|STORE|WELCOME|AMAZON|PIZZA|HUT|TOP TOY|Queeny|\.COM|\.CN|G.DUCK|Macau|MTR|inc\.') THEN '单位'
WHEN TRIM(TRGT_ACCT_NM1) ~"小红书|小店|沃尔玛|比亚迪|一点停|留一手|特来电|陌陌|哈哈|\+|啰|剪映|零钱通|群收款|钱大妈|便捷神|采蝶轩|美团|捷停车|广场|佛通|抖音|肯德基|饿了么|丰巢|快团团|百果园|代收电费|货拉拉|羊城通|惠迪|大黄鹅|喜茶|奈雪|古茗|沪上阿姨|蜜雪冰城|沙县|兰州拉面|驴充充|春意隆|袁小饺|放心借|快手|华为|冯不记|通行宝|益禾堂|菜可道|上盈鲜|好又多|勤阿姨|抖音零钱|爱奇艺|骑驿充|加油站|华莱士|KKV|煲金珠|益万家|尹赛光|茶百道|奈雪的茶|瑞幸|麦当劳|星巴克|必胜客" THEN '个人'
WHEN LENGTH(TRIM(TRGT_ACCT_NM1))>4 AND TRIM(TRGT_ACCT_NM1) ~ '.*厂$|.*站$|.*局$|.*基地$|.*办公室$|.*团$|.*室$|.*店$|.*馆$|.*部$|.*中心$|.*厂$|.*所$|.*会$|.*社$|.*户$|.*队$|.*场$|.*院$' THEN '单位'
WHEN T.CHANNEL_TYPE_CD ='030211' AND BIT_LENGTH(TRGT_ACCT_NM1)/24>=4 AND TRGT_ACCT_NM1 !~ '欧阳|慕容|司徒' THEN '单位'
WHEN BIT_LENGTH(TRGT_ACCT_NM1)/24>4 THEN '单位'
ELSE '个人'
END TRGT_CUST_TYPE --对手客户类型 1-是，0-否

-- 行号: 61 - 103
-- 来源: 图片 (行61-103)
FROM FDMDALIB.F_EVT_CDEP_ACCT A
INNER JOIN FDMDALIB.F_AGT_CDEP_ACCT T
ON T.ACCOUNT_ID=A.ACCOUNT_ID
INNER JOIN
(SELECT *
FROM MDMDALIB.M_EZM_DEP_TRANS
WHERE TRGT_ACCT_BANK_NM!='广东顺德农村商业银行股份有限公司'
AND TRGT_ACCT_NO!='3118083303100200800101'
AND CD_FL='C'
AND TXN_CNL_CD='040113'
AND SPN_SYS_CD='NGOATS'
)T1
ON T.ORI_DEAL_NUM=T1.ORI_DEAL_NUM
AND T.SUB_DEAL_NUM=T1.SUB_DEAL_NUM
AND T.TX_DATE=T1.TX_DATE
LEFT JOIN FDMDALIB.F_EVT_JRNL_JR --
ON T.SPON_TX_DATE=JR.PLAT_DATE
AND T.SPON_DEAL_NUM=JR.PLAT_DEAL_NUM
AND T.SPON_SYS_CD IN ('ESB','NGOATS')
--AND T.SPON_SYS_CD='NGOATS'
AND JR.CHANNEL_TYPE_CD IN ('010600','040113')
LEFT JOIN (SELECT DISTINCT BANK_ORG_NO FROM FDMDALIB.F_PTY_ORG WHERE NVL(BANK_ORG_NO,'')<>'') ORG
ON ORG.BANK_ORG_NO=T1.TRGT_ACCT_BANK_NO
--WHERE A.CUST_TYPE_CD IN ('6','7')
AND T.TX_DATE BETWEEN CURRENT_DATE-1 AND CURRENT_DATE-1 --批量修改
;

666552
--select count(1) from SASSJLIB.td003649_y1rc_dfzh_info2;
drop table if exists SASSJLIB.td003649_y1rc_dfzh_info2;
create table SASSJLIB.td003649_y1rc_dfzh_info2
as
select
replace(to_char(tx_date),'-','')||'_'||ORI_DEAL_NUM||'_'||SUB_DEAL_NUM||'_'||DETAIL_NUM||'_'||ACCOUNT_ID as tradnum
,max(TX_TIME) as TX_TIME
,max(ACCOUNT_ID) as cardno
,max(TRGT_ACCT_NO1) as dfzh
,max(TRGT_ACCT_NM1) as dfhmc
,max(TRGT_ACCT_BANK_NM) as dfhmc
from SASSJLIB.td003649_y1rc2_dfzh_info
group by replace(to_char(tx_date),'-','')||'_'||ORI_DEAL_NUM||'_'||SUB_DEAL_NUM||'_'||DETAIL_NUM||'_'||ACCOUNT_ID
;

-- 行号: 104 - 154
-- 来源: 图片, (合并)
173
--select count(1) from SASSJLIB.ga_tmp3;
drop table if exists SASSJLIB.ga_tmp3;
create table SASSJLIB.ga_tmp3
as
select
CCUSACCT as cardno
,min(substr(to_char(CCRTSTAM),1,10)) as report_dt
,min(CUPDDATE) as report_dt2
,max(CINOCSNO) as CUST_ID
,max(CCUNFLNM) as custname
,max(CELTRSON) as CELTRSON
,max(CSPLSTAT) as CSPLSTAT
FROM ODMDALIB.O_ABS_BCFMCBLI
where ODS_DATA_DATE='20251218'
and CSPLC FTP='AS'
--and CUPDDATE>='20250801'
and substr(to_char(CCRTSTAM),1,10)>='2025-01-01'
--and CSPLSTAT=1
and substr(CCUSACCT,1,5)<>'62232'
and trim(CELTRSON)='涉案一级卡'
group by CCUSACCT
;
173
--select count(1) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1_1;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1_1;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1_1
as
select
ACCOUNT_ID
,max(t1.CUST_ID) as CUST_ID
,max(OPEN_ORG_ID) as OPEN_ORG_ID
,max(OPEN_DATE) as OPEN_DATE
,max(PRODUCT_ID) as PRODUCT_ID
,max(COMP_PROD_ID) as COMP_PROD_ID
,max(PROD_SUBS_ID) as PROD_SUBS_ID
,max(MEDIUM_TYPE_CDS) as MEDIUM_TYPE_CDS
,max(DEPOSIT_KIND_CD) as DEPOSIT_KIND_CD
,max(ACCT_ATTR_CD) as ACCT_ATTR_CD
from FDMDALIB.F_AGT_CDEP_ACCT t1
inner join SASSJLIB.ga_tmp3 t2 on t1.ACCOUNT_ID=t2.cardno
group by ACCOUNT_ID
;
176918
--select count(1) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1_2;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1_2;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1_2
as
select
distinct *
from MDMDALIB.M_EZM_DEP_TRANS t1
where
--t1.TX_DATE>='2021-01-01'
--#NAME?
((t1.REMARK not like '%手续费%' and t1.REMARK not like '%短信费%' and t1.REMARK not like '%利息%') or t1.REMARK is null)
and ((t1.ABSTRACT_DESC not like '%手续费%' and t1.ABSTRACT_DESC not like '%卡年费%' and t1.ABSTRACT_DESC not like '%利息%') or t1.ABSTRACT_DESC is null)
and AR_NO in (select cardno from SASSJLIB.ga_tmp3)
;

-- 行号: 163 - 212
-- 来源: 图片,,
176918
--select count(1),count(distinct tradnum) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1
as
with tmp1 as (
select
tx_date as tradnum1
,ORI_DEAL_NUM as tradnum2
,SUB_DEAL_NUM as tradnum3
,DETAIL_NUM as tradnum4
,replace(to_char(tx_date),'-','')||'_'||ORI_DEAL_NUM||'_'||SUB_DEAL_NUM||'_'||DETAIL_NUM||'_'||AR_NO as tradnum
,trim(t1.AR_NO) as cardno
,case when t3.tradnum is not null then t3.dfzh else trim(t1.TRGT_ACCT_NO) end as dfzh
,case when t1.CD_FL='C' then 1 else 0 end as dcflag
,t1.TXN_AMT as amountamt
,t1.TXN_BAL as zhye
--,trim(t1.cntpr_bank_no) as dfhh
,substr(TO_CHAR(t1.TX_TIME),1,10) as stm_dt
,substr(TO_CHAR(t1.TX_TIME),12,8) as stm_tm
,trim(t1.TXN_CNL_CD) as tranchan
,trim(t1.ABSTRACT_DESC) as abstr
,trim(t1.ACCT_NM) as custname
,case when t3.tradnum is not null then t3.dfzhmc else trim(t1.TRGT_ACCT_NM) end as dfzhmc
,trim(t1.REMARK) as bz
,cast(extract(epoch from to_timestamp(TO_CHAR(t1.TX_TIME),'yyyy-mm-dd hh24:mi:ss')) as int) as trantime
,substr(TO_CHAR(t1.TX_TIME),12,2) as hh
,case when t3.tradnum is not null then t3.dfhmc else trim(t1.TRGT_ACCT_BANK_NM) end as dfhmc
,t1.EXCH_FL as cash_exc_flag
,t1.ACCT_NO as cust_acct
,t2.cust_id as cust_id
,t2.OPEN_ORG_ID as open_org_id
,t2.ACCT_ATTR_CD as acct_attr
,t2.OPEN_DATE as OPEN_DATE
,t2.PRODUCT_ID as PRODUCT_ID
,t2.COMP_PROD_ID as COMP_PROD_ID
,t2.PROD_SUBS_ID as PROD_SUBS_ID
,t2.MEDIUM_TYPE_CDS as MEDIUM_TYPE_CDS
,t2.DEPOSIT_KIND_CD as DEPOSIT_KIND_CD
,TRGT_ACCT_BANK_NO as dfhh
,case
when t1.TXN_AMT <= 100 then 'bin_amt_le100'
when t1.TXN_AMT <= 1000 then 'bin_amt_bw_100_1k'
when t1.TXN_AMT <= 3000 then 'bin_amt_bw_1k_3k'
when t1.TXN_AMT <= 10000 then 'bin_amt_bw_3k_1w'
when t1.TXN_AMT <= 50000 then 'bin_amt_bw_1w_5w'
when t1.TXN_AMT > 50000 then 'bin_amt_gt_5w'
end as amt_bin
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1_2 t1
inner join SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1_1 t2 on t1.AR_NO=t2.ACCOUNT_ID

-- 行号: 214 - 271
-- 来源: 图片,, (合并)
left join SASSJLIB.td003649_y1rc_dfzh_info2 t3 on replace(to_char(t1.tx_date),'-','')||'_'||t1.ORI_DEAL_NUM||'_'||t1.SUB_DEAL_NUM||'_'||t1.DETAIL_NUM||'_'||t1.AR_NO=t3.tradnum
),tmp2 as (
select
t1.*
,row_number() over (partition by tradnum order by txn_time) as rn18
from tmp1 t1
)
select
*
from tmp2
where rn18=1
;
--#NAME?
1260
--select count(1) from SASSJLIB.td003649_y1rc_nwp_trainwb_gs_tmp;
drop table if exists SASSJLIB.td003649_y1rc_nwp_trainwb_gs_tmp;
create table SASSJLIB.td003649_y1rc_nwp_trainwb_gs_tmp
as
with tmp1 as (
select
cardno as cardno
,trim(dfzhmc) as cardno2
,stm_dt as stm_dt
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t1
where dfzhmc is not null and dfzhmc<>'NaN' and trim(dfzhmc)<>''
and custname<>dfzhmc
and length(trim(dfzhmc))>=2
and length(trim(dfzhmc))<=4
group by
cardno
,trim(dfzhmc)
,stm_dt
), tmp2 as (
select cardno
,cardno2
,stm_dt
from tmp1 t1
--where stm_dt>='2024-01-01'
), tmp3 as (
select
t1.cardno
,t1.cardno2
,t1.stm_dt
,t2.cardno as cardno3
from tmp2 t1
left join tmp1 t2
on t1.cardno2=t2.cardno
and t1.cardno2!=t2.cardno2
and date(t1.stm_dt)-date(t2.stm_dt)>=7
and date(t1.stm_dt)-date(t2.stm_dt)<=180
)
select
t1.cardno
,t1.cardno2
,t1.stm_dt
from tmp3 t1
where t1.cardno3 is null
;

-- 行号: 272 - 317
-- 来源: 图片,,
--1187
--select count(1) from SASSJLIB.td003649_y1rc_nwp2_trainwb_gs_tmp;
--陌生交易对手组合 select stm_dt,count(1) from SASSJLIB.td003649_y1rc_nwp_trainwb_gs_tmp group by stm_dt order by stm_dt; 1552915
drop table if exists SASSJLIB.td003649_y1rc_nwp2_trainwb_gs_tmp;
create table SASSJLIB.td003649_y1rc_nwp2_trainwb_gs_tmp
as
with tmp1 as (
select
cardno as cardno
,trim(dfzhmc) as cardno2
,stm_dt as stm_dt
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t1
where dfzhmc is not null and dfzhmc<>'NaN' and trim(dfzhmc)<>''
and custname<>dfzhmc
and length(trim(dfzhmc))>=2
and length(trim(dfzhmc))<=4
group by
cardno
,trim(dfzhmc)
,stm_dt
), tmp2 as (
select cardno
,cardno2
,stm_dt
from tmp1 t1
--where stm_dt>='2024-01-01'
), tmp3 as (
select
t1.cardno
,t1.cardno2
,t1.stm_dt
,t2.cardno as cardno3
from tmp2 t1
left join tmp1 t2
on t1.cardno=t2.cardno
and t1.cardno2=t2.cardno2
and date(t1.stm_dt)-date(t2.stm_dt)>=7
and date(t1.stm_dt)-date(t2.stm_dt)<=1095
)
select
t1.cardno
,t1.cardno2
,t1.stm_dt
from tmp3 t1
where t1.cardno3 is null
;

-- 行号: 318 - 363
-- 来源: 图片,, (合并)
211
--select count(1) from SASSJLIB.td003649_y1rc_nwabstr_trainwb_gs_tmp;
drop table if exists SASSJLIB.td003649_y1rc_nwabstr_trainwb_gs_tmp;
create table SASSJLIB.td003649_y1rc_nwabstr_trainwb_gs_tmp
as
with tmp1 as (
select
cardno
,bz as cardno2
,stm_dt
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t1
where dcflag=1
and abstr not like '%退款%'
and (bz like '%财付通%' or bz like '%数字人民币%' or bz like '%支付宝%' or bz like '%往来款%' or bz like '%跨行网银贷记%')
and custname<>dfzhmc
group by
cardno
,bz
,stm_dt
), tmp2 as (
select
cardno
,cardno2
,stm_dt
from tmp1 t1
--where stm_dt>='2024-01-01'
), tmp3 as (
select
t1.cardno
,t1.cardno2
,t1.stm_dt
,t2.cardno as cardno3
from tmp2 t1
left join tmp1 t2
on t1.cardno = t2.cardno
and t1.cardno2 = t2.cardno2
and date(t1.stm_dt)-date(t2.stm_dt)>=7
and date(t1.stm_dt)-date(t2.stm_dt)<=180
)
select
t1.cardno
,t1.cardno2
,t1.stm_dt
from tmp3 t1
where t1.cardno3 is null
;
-- 行号: 364 - 409
-- 来源: 图片,
--select count(1) from SASSJLIB.td003649_y1rc_nwtranchan_trainwb_gs_tmp;
--陌生交易渠道 474940
--1503752
drop table if exists SASSJLIB.td003649_y1rc_nwtranchan_trainwb_gs_tmp;
create table SASSJLIB.td003649_y1rc_nwtranchan_trainwb_gs_tmp
as
with tmp1 as (
select
cardno
,to_char(tranchan) as cardno2
,stm_dt
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t1
where dcflag=1
and abstr not like '%退款%'
and to_char(tranchan) in ('040113','030211','040111','040131','040106')
and custname<>dfzhmc
group by
cardno
,to_char(tranchan)
,stm_dt
), tmp2 as (
select
cardno
,cardno2
,stm_dt
from tmp1 t1
--where stm_dt>='2024-01-01'
), tmp3 as (
select
t1.cardno
,t1.cardno2
,t1.stm_dt
,t2.cardno as cardno3
from tmp2 t1
left join tmp1 t2
on t1.cardno = t2.cardno
and t1.cardno2 = t2.cardno2
and date(t1.stm_dt)-date(t2.stm_dt)>=7
and date(t1.stm_dt)-date(t2.stm_dt)<=180
)
select
t1.cardno
,t1.cardno2
,t1.stm_dt
from tmp3 t1
where t1.cardno3 is null
;

-- 行号: 411 - 455
-- 来源: 图片,
5429523
--陌生交易金额 643119
--select count(1) from SASSJLIB.td003649_y1rc_nwamt_trainwb_gs_tmp;
drop table if exists SASSJLIB.td003649_y1rc_nwamt_trainwb_gs_tmp;
create table SASSJLIB.td003649_y1rc_nwamt_trainwb_gs_tmp
as
with tmp1 as (
select
cardno
,amt_bin as cardno2
,stm_dt
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t1
where dcflag=1
and abstr not like '%退款%'
and amt_bin is not null
and custname<>dfzhmc
group by
cardno
,amt_bin
,stm_dt
), tmp2 as (
select
cardno
,cardno2
,stm_dt
from tmp1 t1
--where stm_dt>='2024-01-01'
), tmp3 as (
select
t1.cardno
,t1.cardno2
,t1.stm_dt
,t2.cardno as cardno3
from tmp2 t1
left join tmp1 t2
on t1.cardno = t2.cardno
and t1.cardno2 = t2.cardno2
and date(t1.stm_dt)-date(t2.stm_dt)>=7
and date(t1.stm_dt)-date(t2.stm_dt)<=180
)
select
t1.cardno
,t1.cardno2
,t1.stm_dt
from tmp3 t1 --注意：此处原图为select ... from tmp3 t1 where ...
-- 上方代码在454行截断，逻辑在下一页继续
-- 行号: 450 - 505 (图54，旋转图)
where t1.cardno3 is null
;
--#NAME?
--316
--select count(1) from SASSJLIB.td003649_y1rc_nwhh_trainwb_gs_tmp;
drop table if exists SASSJLIB.td003649_y1rc_nwhh_trainwb_gs_tmp;
create table SASSJLIB.td003649_y1rc_nwhh_trainwb_gs_tmp
as
with tmp1 as (
select
cardno
,hh as cardno2
,stm_dt
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t1
where dcflag=1
and abstr not like '%退款%'
and hh is not null
and custname<>dfzhmc
group by
cardno
,hh
,stm_dt
), tmp2 as (
select
cardno
,cardno2
,stm_dt
from tmp1 t1
--where stm_dt>='2024-01-01'
), tmp3 as (
select
t1.cardno
,t1.cardno2
,t1.stm_dt
,t2.cardno as cardno3
from tmp2 t1
left join tmp1 t2
on t1.cardno = t2.cardno
and t1.cardno2 = t2.cardno2
and date(t1.stm_dt)-date(t2.stm_dt)>=7
and date(t1.stm_dt)-date(t2.stm_dt)<=180
)
select
t1.cardno
,t1.cardno2
,t1.stm_dt
from tmp3 t1
where t1.cardno3 is null
;
--par2
-- 行号: 506 - 552
-- 来源: 图片
--4469704
--select count(1) from SASSJLIB.td003649_y1rc_nwdfhh_trainwb_gs_tmp;
drop table if exists SASSJLIB.td003649_y1rc_nwdfhh_trainwb_gs_tmp;
create table SASSJLIB.td003649_y1rc_nwdfhh_trainwb_gs_tmp
as
with tmp1 as (
select
cardno
,dfhmc as cardno2
,stm_dt
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t1
where dcflag=1
and abstr not like '%退款%'
and dfhmc is not null
and custname<>dfzhmc
group by
cardno
,dfhmc
,stm_dt
), tmp2 as (
select
cardno
,cardno2
,stm_dt
from tmp1 t1
--where stm_dt>='2024-01-01'
), tmp3 as (
select
t1.cardno
,t1.cardno2
,t1.stm_dt
,t2.cardno as cardno3
from tmp2 t1
left join tmp1 t2
on t1.cardno = t2.cardno
and t1.cardno2 = t2.cardno2
and date(t1.stm_dt)-date(t2.stm_dt)>=7
and date(t1.stm_dt)-date(t2.stm_dt)<=180
)
select
t1.cardno
,t1.cardno2
,t1.stm_dt
from tmp3 t1
where t1.cardno3 is null
;
#NAME?
112
--select province,count(1) from SASSJLIB.td003649_y1rc_card_location_tmp group by province;

-- 行号: 555 - 614
-- 来源: 图片,
drop table if exists SASSJLIB.td003649_y1rc_card_location_tmp;
create table SASSJLIB.td003649_y1rc_card_location_tmp
as
with tmp11 as (
select t1.dfzh as cardno
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t1
--where substr(t1.dfzh,1,2)='62' and length(t1.dfzh)<=4 and length(t1.dfzhmc)>=1
where dfzh is not null
), tmp12 as (
select
distinct cardno
from tmp11 t1
), tmp1 as (
select
t1.cardno
,t2.bank_code
,t2.bin_prefix
,row_number() over (partition by t1.cardno order by length(t2.bin_prefix) desc) as rn
from tmp12 t1
left join SASSJLIB.bank_bin t2
on substr(t1.cardno,1,length(t2.bin_prefix)) = t2.bin_prefix
and length(t1.cardno) = t2.length
), tmp2 as (
select
t1.cardno
,t1.bank_code
,t1.bin_prefix
,t2.name as bank_name
from tmp1 t1
left join SASSJLIB.bank_name t2
on t1.rn = 1
and t1.bank_code = t2.bank
),tmp3 as (
select
t1.cardno
,t1.bank_name
,t2.province
,t2.city
from tmp2 t1
left join SASSJLIB.bank_info t2
on t1.bank_name = t2.bank_name
and t1.bin_prefix = t2.bank_prefix
and substr(t1.cardno,t2.index_start+1,length(t2.district_code)) = t2.district_code
),tmp4 as (
select
t1.cardno
,t1.province
,t1.bank_name
,t1.city
,row_number() over (partition by t1.cardno order by t1.province, t1.city) as rn
from tmp3 t1
)
select
t1.cardno
,t1.province
,t1.bank_name
,t1.city
from tmp4 t1
where t1.rn=1
;

-- 行号: 615 - 655
-- 来源: 图片, (合并)
25529
--select count(1) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2
as
select
t0.*
,case when length(t0.dfzhmc)>=1 and length(t0.dfzhmc)<=4 and t0.custname<>t0.dfzhmc then 1 else 0 end as f_diff_name
,case when length(t0.dfzhmc)>=1 and length(t0.dfzhmc)<=4 and t0.custname<>t0.dfzhmc and substr(t0.dfzh,1,2)='62' then 1 else 0 end as f_diff_name_62
,case when t2.cardno is null then 0 else 1 end as fabstr
,case when t3.cardno is null then 0 else 1 end as ftranchan
,case when t4.cardno is null then 0 else 1 end as famt_bin
,case when t5.cardno is null then 0 else 1 end as ffhh
,case when t6.cardno is null then 0 else 1 end as fnwp
,case when t6.cardno is null then 0 else 1 end as fdfhh
,t7.province as province
,t7.city as city
,t7.bank_name as dfbank_name
,row_number() over (partition by t0.cardno,t0.stm_dt order by t0.trantime asc, t0.tradnum asc) as rk_ye_asc
,row_number() over (partition by t0.cardno,t0.stm_dt order by t0.trantime desc, t0.tradnum desc) as rk_ye
,t0.trantime - lag(t0.trantime) over (partition by t0.cardno,t0.stm_dt order by t0.trantime,t0.tradnum) as diff_tm
,t8.report_dt as report_dt
,date(t8.report_dt)-date(t0.stm_dt) as ga_days
,case when t8.cardno is null then 0 when date(t0.stm_dt)-date(t8.report_dt)<=0 then 1 else 2 end as label
,case when t8.cardno is null then null when date(t8.report_dt)-date(t0.stm_dt)<0 then date(t8.report_dt)-date(t0.stm_dt) else dense_rank() over (partition by t0.cardno order by case when date(t8.report_dt)-date(t0.stm_dt)>=0 then date(t8.report_dt)-date(t0.stm_dt) end nulls last) end as ga_days_rank
--如果当笔abstr是冲正，那么上一笔也标为冲正标签
,lead(t0.abstr) over (partition by t0.cardno order by t0.trantime,t0.tradnum) as lead_abstr
,case when t9.cardno is null then 0 else 1 end as fnwp2
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t0
left join SASSJLIB.td003649_y1rc_nwabstr_trainwb_gs_tmp t2 on t0.cardno = t2.cardno and t0.bz = t2.cardno2 and t0.stm_dt = t1.stm_dt -- 注意：此处 t1 应为 t0 或 t2，源码图为 t1.stm_dt 可能笔误，照抄
left join SASSJLIB.td003649_y1rc_nwtranchan_trainwb_gs_tmp t3 on t0.cardno = t3.cardno and to_char(t0.tranchan) = t3.cardno2 and t0.stm_dt = t3.stm_dt
left join SASSJLIB.td003649_y1rc_nwamt_trainwb_gs_tmp t4 on t0.cardno = t4.cardno and t0.amt_bin = t4.cardno2 and t0.stm_dt = t4.stm_dt
left join SASSJLIB.td003649_y1rc_nwhh_trainwb_gs_tmp t5 on t0.cardno = t5.cardno and t0.hh = t5.cardno2 and t0.stm_dt = t5.stm_dt
left join SASSJLIB.td003649_y1rc_nwdfhh_trainwb_gs_tmp t6 on t0.cardno = t6.cardno and t0.dfhmc = t6.cardno2 and t0.stm_dt = t6.stm_dt
left join (select * from SASSJLIB.td003649_y1rc_card_location_tmp where province is not null or city is not null or bank_name is not null) t7 on t0.dfzh = t7.cardno
left join SASSJLIB.ga_tmp3 t8 on t0.cardno = t8.cardno
left join SASSJLIB.td003649_y1rc_nwp2_trainwb_gs_tmp t9 on t0.cardno = t9.cardno and trim(t0.dfzhmc) = t9.cardno2 and t0.stm_dt = t9.stm_dt
;

-- 行号: 656 - 678
-- 来源: 图片, (合并)
#NAME?
--select count(1) from SASSJLIB.td003649_y1rc_custid_ft_gs;
--393,222
drop table if exists SASSJLIB.td003649_y1rc_custid_ft_gs;
create table SASSJLIB.td003649_y1rc_custid_ft_gs
as
select
cust_id
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2
where substr(cust_id,1,1)='6'
and custname<>dfzhmc
and dcflag=1
and length(trim(dfzhmc))<=4
and (fnwp=1 or dfzhmc=")
--and (bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%' or bz='')
--and (bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
--and (bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
or
((substr(to_char(dfzh),1,2)='62' and (length(dfzh)=16 or length(dfzh)=19)))
)
group by cust_id
;

-- 行号: 679 - 704
-- 来源: 图片
drop table if exists SASSJLIB.td003649_y1rc_trck_flw;
create table SASSJLIB.td003649_y1rc_trck_flw
as
with tmp1 as (
select
CERT_NAME,CERT_TYPE_CD,CERT_NO,CHANNEL_CD,ORG_ID
,TX_DESC,ERROR_CD,ERROR_INFO,CONSM_SUB_NUM,TX_DATE
,TX_TIME,CONSUME_TM,CUST_ID,DEVICE_ID,IP,DEV_NAME
,DEV_MANUFACTURER,MAC,LOC_LNG,LOC_LAT,OP_OS_VERSION
,COUNTRY,CITY,PROVINCE,APP_VERSIONNAME,OS,DEV_NET,INTF_CIND
from FDMDALIB.F_EVT_CHN_RT_BUSINESS t1
where TX_DESC is not null
and cust_id is not null
)
select
t1.*
from tmp1 t1
inner join SASSJLIB.td003649_y1rc_custid_ft_gs t2
on t1.cust_id=t2.cust_id
;
--select count(1) from SASSJLIB.td003649_y1rc_cardno_ft_gs;
407875
drop table if exists SASSJLIB.td003649_y1rc_cardno_ft_gs;
create table SASSJLIB.td003649_y1rc_cardno_ft_gs
as
select -- 注意：此处原图为 select ... from tmp2
-- 行号: 705 - 721 (图片,)
cardno
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2
where substr(cust_id,1,1)='6'
and custname<>dfzhmc
and dcflag=1
and length(trim(dfzhmc))<=4
and (fnwp=1 or dfzhmc=")
--and (bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%' or bz='')
--and (bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
--and (bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
or
((substr(to_char(dfzh),1,2)='62' and (length(dfzh)=16 or length(dfzh)=19)))
)
group by cardno
;

-- 行号: 722 - 747
-- 来源: 图片, (合并)
--122,396,051
--select count(1) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp3;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp3;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp3
as
select
t1.*
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2 t1
inner join SASSJLIB.td003649_y1rc_cardno_ft_gs t2
on t1.cardno=t2.cardno
;
--122,396,051
--select count(1) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_1;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_1;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_1
as
select
t1.*
,min(case when dcflag=1 then zhye-amountamt end)
over (partition by t1.cardno order by t1.trantime range between 604800 preceding and 0 preceding) as txn_adj_c_zhye_min_604800s
,min(case when dcflag=1 then zhye-amountamt end)
over (partition by t1.cardno order by t1.trantime range between 7200 preceding and 0 preceding) as txn_adj_c_zhye_min_7200s
,last_value(case when t1.dcflag=1 and (t1.abstr is null or t1.abstr not like '%冲销%') then t1.tradnum END,true) over
(partition by t1.cardno order by t1.trantime,t1.tradnum rows between unbounded preceding and current row) as lag_in_tradnum
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp3 t1
;

-- 行号: 749 - 780
-- 来源: 图片, (合并)
--122,396,051
--select count(1) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_2;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_2;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_2
as
select
t1.*
,t2.trantime as lag_in_trantime
,t2.dfzh as lag_in_dfzh
,t2.dfzhmc as lag_in_dfzhmc
,t2.custname as lag_in_custname
,t2.amountamt as lag_in_amountamt
,t2.dcflag as lag_in_dcflag
,t2.zhye as lag_in_zhye
,t2.province as lag_in_province
,t2.city as lag_in_city
,t2.dfhmc as lag_in_dfhmc
,t2.fnwp as lag_in_fnwp
,t2.fnwp2 as lag_in_fnwp2
,t2.f_diff_name_62 as lag_in_f_diff_name_62
,t2.f_diff_name as lag_in_f_diff_name
,t2.fhh as lag_in_fhh
,t2.fabstr as lag_in_fabstr
,t2.ftranchan as lag_in_ftranchan
,t2.famt_bin as lag_in_famt_bin
,t2.txn_time as lag_in_txn_time
,t2.bz as lag_in_bz
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_1 t1
left join SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_1 t2
on t1.lag_in_tradnum=t2.tradnum
;

-- 行号: 781 - 805
-- 来源: 图片,
--122,396,051
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3
as
with tmp1 as (
select
cardno
,trantime
,tradnum
,lag_in_tradnum
,row_number() over (partition by lag_in_tradnum order by trantime,tradnum) as rn_out_follow_in
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_2
where lag_in_tradnum is not null
and dcflag = 0
and (abstr not like '%冲销%' or abstr is null) and (lead_abstr not like '%冲销%' or lead_abstr is null)
and lag_in_dcflag=1
and lag_in_custname<>lag_in_dfzhmc and length(lag_in_dfzhmc)<=4 and length(lag_in_dfzhmc)>=2 and lag_in_fnwp2=1
and trantime-lag_in_trantime<=86399
and trantime-lag_in_trantime>=0
)
select t1.*
,t2.rn_out_follow_in as rn_out_follow_in
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_2 t1
left join tmp1 t2 on t1.cardno = t2.cardno and t1.trantime = t2.trantime and t1.tradnum = t2.tradnum
;

-- 行号: 806 - 820
-- 来源: 图片, (合并)
--122,396,051
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_4;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_4
as
select t1.*
,lag(tradnum) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_tradnum
,lag(fnwp) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_fnwp
,lag(fnwp2) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_fnwp2
,lag(dcflag) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_dcflag
,lag(f_diff_name) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_f_diff_name
,lag(trantime) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_trantime
,lag(province) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_province
,lag(amountamt) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_amountamt
,lag(zhye) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_zhye
,lag(bz) over (partition by t1.cardno order by t1.trantime,t1.tradnum) as lag_bz
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3 t1
;

-- 行号: 821 - 867
-- 来源: 图片,
--46,857,923
--select count(1) from SASSJLIB.td003649_y1rc_nwp3_trainwb_gs_tmp;
--陌生交易对手组合 select stm_dt,count(1) from SASSJLIB.td003649_y1rc_nwp3_trainwb_gs_tmp group by stm_dt order by stm_dt; 1552915
drop table if exists SASSJLIB.td003649_y1rc_nwp3_trainwb_gs_tmp;
create table SASSJLIB.td003649_y1rc_nwp3_trainwb_gs_tmp
as
with tmp1 as (
select
cardno as cardno
,trim(dfzhmc) as cardno2
,stm_dt as stm_dt
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp1 t1
where dfzhmc is not null and dfzhmc<>'NaN' and trim(dfzhmc)<>''
and custname<>dfzhmc
and length(trim(dfzhmc))>=2
and length(trim(dfzhmc))<=4
group by
cardno
,trim(dfzhmc)
,stm_dt
), tmp2 as (
select
cardno
,cardno2
,stm_dt
from tmp1 t1
--where stm_dt>='2024-01-01'
), tmp3 as (
select
t1.cardno
,t1.cardno2
,t1.stm_dt
,t2.cardno as cardno3
from tmp2 t1
left join tmp1 t2
on t1.cardno=t2.cardno
and t1.cardno2=t2.cardno2
and date(t1.stm_dt)-date(t2.stm_dt)>=7
and date(t1.stm_dt)-date(t2.stm_dt)<=1095
)
select
t1.cardno
,t1.cardno2
,t1.stm_dt
from tmp3 t1
where t1.cardno3 is null
;

-- 行号: 868 - 891
-- 来源: 图片, (合并)
--122,396,051
--136,715,558
--select count(1) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_5;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_5;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_5
as
select t1.*
,sum(case when f_diff_name=1 and dcflag=1 and fnwp2=1 and (substr(to_char(dfzh),1,2)='62' and (length(dfzh)=16 or length(dfzh)=19)) then 1 else 0 end)
over (partition by t1.cardno order by t1.trantime range between 7200 preceding and 1 preceding) as txn_fnm_c_nwp_bank_window_cnt_7200s
,sum(case when f_diff_name=1 and dcflag=1 and fnwp2=1 and province<>'广东' then 1 else 0 end)
over (partition by t1.cardno order by t1.trantime range between 7200 preceding and 1 preceding) as txn_fnm_c_nwp_bank_window_cnt_7200s -- 注意：此处变量名重复，根据图片35行880应为 _bank_dffloc_window_cnt_7200s
,sum(case when f_diff_name=1 and dcflag=1 and fnwp2=1 then 1 else 0 end)
over (partition by t1.cardno order by t1.trantime range between 7200 preceding and 1 preceding) as txn_fnm_c_nwp_window_cnt_7200s
--
,sum(case when f_diff_name=1 and dcflag=1 and fnwp2=1 and (substr(to_char(dfzh),1,2)='62' and (length(dfzh)=16 or length(dfzh)=19)) then 1 else 0 end)
over (partition by t1.cardno order by t1.trantime range between 86400 preceding and 1 preceding) as txn_fnm_c_nwp_bank_window_cnt_86400s
,sum(case when f_diff_name=1 and dcflag=1 and fnwp2=1 and province<>'广东' then 1 else 0 end)
over (partition by t1.cardno order by t1.trantime range between 86400 preceding and 1 preceding) as txn_fnm_c_nwp_bank_dffloc_window_cnt_86400s
,sum(case when f_diff_name=1 and dcflag=1 and fnwp2=1 then 1 else 0 end)
over (partition by t1.cardno order by t1.trantime range between 86400 preceding and 1 preceding) as txn_fnm_c_nwp_window_cnt_86400s
,case when t2.cardno is null then 0 else 1 end as fnwp3
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_4 t1
left join SASSJLIB.td003649_y1rc_nwp3_trainwb_gs_tmp t2 on t1.cardno = t2.cardno and trim(t1.dfzhmc) = t2.cardno2 and t1.stm_dt = t2.stm_dt
;

-- 行号: 892 - 912
-- 来源: 图片, (合并)
-- 532143
--select count(1) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_1_gs;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_1_gs;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_1_gs
as
select
*
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_5
where (abstr not like '%冲销%' or abstr is null)
and lag_in_dcflag=1
and trantime-lag_in_trantime<=7200
and trantime-lag_in_trantime>=0
and lag_in_custname<>lag_in_dfzhmc
and lag_in_fnwp2=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%' or bz='')
or
((substr(to_char(dfzh),1,2)='62' and (length(dfzh)=16 or length(dfzh)=19)))
)
and (rn_out_follow_in=1 or rn_out_follow_in is null)
;

-- 行号: 913 - 925
-- 来源: 图片
--229798
--select count(1),count(distinct tradnum) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_2_gs;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_2_gs;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_2_gs
as
select
*
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_5
where (abstr not like '%冲销%' or abstr is null)
and dcflag=0
and (txn_fnm_c_nwp_bank_window_cnt_86400s>0 or txn_fnm_c_nwp_bank_dffloc_window_cnt_86400s>0 or txn_fnm_c_nwp_window_cnt_86400s>0)
--and lag_dcflag=1
;

-- 行号: 926 - 934
-- 来源: 图片
--1051
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs;
drop table if exists SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs;
create table SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs
as
select * from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_1_gs
union
select * from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_2_gs
;

-- 行号: 935 - 1001
-- 来源: 图片,,, (合并)
--1051
--select count(1) from SASSJLIB.td003649_y1rc_txn_bank_cnt_3d;
drop table if exists SASSJLIB.td003649_y1rc_txn_bank_cnt_3d;
create table SASSJLIB.td003649_y1rc_txn_bank_cnt_3d
as
select
t1.tradnum as tradnum_txn_3d
,sum(case when (length(t2.dfzh)=16 or length(t2.dfzh)=19) and substr(t2.dfzh,1,2)='62' then 1 else 0 end) as txn_bank_cnt_3d
,sum(case when (length(t2.dfzh)=16 or length(t2.dfzh)=19) and substr(t2.dfzh,1,2)='62' and t2.dcflag=1 then 1 else 0 end) as txn_bank_c_cnt_3d
,sum(case when (length(t2.dfzh)=16 or length(t2.dfzh)=19) and substr(t2.dfzh,1,2)='62' and t2.dcflag=1 and t2.f_diff_name=1 then 1 else 0 end) as txn_bank_c_fnm_cnt_3d
,sum(case when t2.bz like '%数字人民币兑回%' then 1 else 0 end) as txn_szrmb_cnt_3d
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 then 1 else 0 end) as txn_fnm_c_cnt_3d
,#NAME?
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 then 1 else 0 end) as txn_fnm_c_nwp_cnt_3d
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.bz like '%支付宝%' then 1 else 0 end) as txn_fnm_c_nwp_zfb_cnt_3d
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and (substr(to_char(t2.dfzh),1,2)='62' and (length(t2.dfzh)=16 or length(t2.dfzh)=19)) then 1 else 0 end) as txn_fnm_c_nwp_bank_cnt_3d -- 原图被截断，合并修正
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.province<>'广东' then 1 else 0 end) as txn_fnm_c_nwp_bank_dffloc_cnt_3d
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.province<>'广东' then t2.amountamt else 0 end) as txn_fnm_c_nwp_bank_dffloc_amt_3d
,count(distinct case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.province<>'广东' then t2.province end) as txn_fnm_c_nwp_bank_dffloc_pro_cnt_3d
,count(distinct case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.province<>'广东' then t2.dfzhmc end) as txn_fnm_c_nwp_bank_dffloc_dfzhmc_cnt_3d
,sum(case when t2.dcflag=0 and t2.bz like '%基金%' then 1 else 0 end) as txn_fund_cnt_3d
,sum(case when t2.dcflag=0 and t2.bz like '%基金%' then t2.amountamt else 0 end) as txn_fund_amt_3d
,0
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' then 1 else 0 end) as txn_bz65_amt_3d -- 注意：变量名疑似笔误，按图为 txn_bz65_amt_3d 但逻辑是 count
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' then 1 else 0 end) as txn_bz65_cnt_3d
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' and t2.hh in ('23','00','01','02','03','04') then 1 else 0 end) as txn_bz65_zsh_cnt_3d
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' then 1 else 0 end)/nullif(sum(t2.dcflag),0) as txn_bz65_c_ratio_3d
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' and t2.hh in ('23','00','01','02','03','04') then t2.amountamt else 0 end) as txn_bz65_zsh_amt_3d
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
left join SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3 t2
on t1.cardno=t2.cardno
and t1.trantime>0
and t1.trantime-t2.trantime<=259200
group by t1.tradnum
;
-- 1051
-- select count(1) from SASSJLIB.td003649_y1rc_txn_bank_cnt_7200s;
drop table if exists SASSJLIB.td003649_y1rc_txn_bank_cnt_7200s;
create table SASSJLIB.td003649_y1rc_txn_bank_cnt_7200s
as
select
t1.tradnum as tradnum_txn_7200s
,sum(case when (length(t2.dfzh)=16 or length(t2.dfzh)=19) and substr(t2.dfzh,1,2)='62' then 1 else 0 end) as txn_bank_cnt_7200s
,sum(case when (length(t2.dfzh)=16 or length(t2.dfzh)=19) and substr(t2.dfzh,1,2)='62' and t2.dcflag=1 then 1 else 0 end) as txn_bank_c_cnt_7200s
,sum(case when (length(t2.dfzh)=16 or length(t2.dfzh)=19) and substr(t2.dfzh,1,2)='62' and t2.dcflag=1 and t2.f_diff_name=1 then 1 else 0 end) as txn_bank_c_fnm_cnt_7200s
,sum(case when t2.bz like '%数字人民币兑回%' then 1 else 0 end) as txn_szrmb_cnt_7200s
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 then 1 else 0 end) as txn_fnm_c_cnt_7200s
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 then 1 else 0 end) as txn_fnm_c_nwp_cnt_7200s
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.bz like '%支付宝%' then 1 else 0 end) as txn_fnm_c_nwp_zfb_cnt_7200s
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and (substr(to_char(t2.dfzh),1,2)='62' and (length(t2.dfzh)=16 or length(t2.dfzh)=19)) then 1 else 0 end) as txn_fnm_c_nwp_bank_cnt_7200s
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.province<>'广东' then 1 else 0 end) as txn_fnm_c_nwp_bank_dffloc_cnt_7200s
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.province<>'广东' then t2.amountamt else 0 end) as txn_fnm_c_nwp_bank_dffloc_amt_7200s
,count(distinct case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.province<>'广东' then t2.province end) as txn_fnm_c_nwp_bank_dffloc_pro_cnt_7200s
,count(distinct case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.province<>'广东' then t2.dfzhmc end) as txn_fnm_c_nwp_bank_dffloc_dfzhmc_cnt_7200s
,sum(case when t2.dcflag=0 and t2.bz like '%基金%' then 1 else 0 end) as txn_fund_cnt_7200s
,sum(case when t2.dcflag=0 and t2.bz like '%基金%' then t2.amountamt else 0 end) as txn_fund_amt_7200s
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' then 1 else 0 end) as txn_bz65_cnt_7200s
,sum(case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 and t2.bz like '%跨行网银贷记%' then 1 else 0 end) as txn_fnm_c_kh_cnt_7200s
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
left join SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3 t2
on t1.cardno=t2.cardno
and t1.trantime>=0
and t1.trantime-t2.trantime<=7200
group by t1.tradnum
;
--par3
-- 行号: 1002 - 1017
-- 来源: 图片
1051
--select count(1) from SASSJLIB.td003649_y1rc_txn_bank_cnt_1800s;
drop table if exists SASSJLIB.td003649_y1rc_txn_bank_cnt_1800s;
create table SASSJLIB.td003649_y1rc_txn_bank_cnt_1800s
as
select
t1.tradnum as tradnum_txn_1800s
1208
,count(distinct case when t2.f_diff_name=1 and t2.dcflag=1 and t2.fnwp2=1 then t2.dfzhmc end) as txn_fnm_c_nwp_dfzhmc_cnt_1800s
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
left join SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3 t2
on t1.cardno=t2.cardno
and t1.trantime-t2.trantime>0
and t1.trantime-t2.trantime<=1800
group by t1.tradnum
;

-- 行号: 1018 - 1056
-- 来源: 图片,,, (合并)
1051
--select count(1) from SASSJLIB.td003649_y1rc_txn_base_1296000s_par1;
drop table if exists SASSJLIB.td003649_y1rc_txn_base_1296000s_par1;
create table SASSJLIB.td003649_y1rc_txn_base_1296000s_par1
as
with tmp1 as (
select
t1.tradnum
,t1.trantime
,t1.cardno
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
), tmp2 as (
select *
,case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end as d_ye
,case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end as c_ye
,amountamt/(case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end) as d_ye_prop
,amountamt/(case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end) as c_ye_prop
,case when province<>'广东' then 1 when province is not null then 0 end as f_dffloc
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_4
)
select
t1.tradnum as tradnum_txn_base_1296000s_par1
--#NAME?
,max(case when dcflag=1 then c_ye end) as txn_cye_max_1296000s
--#NAME?
,stddev_pop(zhye) as txn_zhye_std_1296000s
--#NAME?
,sum(case when dcflag=1 then amountamt else 0 end)/nullif(sum(case when dcflag=0 then amountamt else 0 end),0) as txn_cd_amt_ratio_1296000s
,min(amountamt) as txn_min_amt_1296000s
--#NAME?
,avg(case when cast(hh as int) between 0 and 3 then 1 else 0 end) as txn_stsleep_prop_1296000s
--#NAME?
,avg(case when cast(hh as int) between 8 and 18 then 1 else 0 end) as txn_stwork_prop_1296000s
--#NAME?
,avg(case when cast(hh as int) in (4,5,6,7,19,20,21,22,23) then 1 else 0 end) as txn_strest_prop_1296000s
,stddev_pop(case when dcflag=0 then d_ye_prop else null end) as txn_dyeprop_std_1296000s
--#NAME?
,max(case when dcflag=1 then c_ye_prop else null end) as txn_cyeprop_max_1296000s
--#NAME?
,stddev_pop(case when dcflag=1 then c_ye_prop else null end) as txn_cyeprop_std_1296000s
--#NAME?
,sum(case when fabstr=1 and f_diff_name=1 and dcflag=1 then t2.amountamt else 0 end) as txn_c_nabstr_txn_amt_1296000s -- 注意：此处合并了右侧K列后的内容
--#NAME?
,sum(case when f_diff_name=1 then t2.amountamt else 0 end) as txn_amt_1296000s
--#NAME?
,sum(case when t2.tranchan='040113' then amountamt else 0 end) as txn_tranchan16_amt_1296000s
-- 0
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' then amountamt else 0 end) as txn_bz65_amt_1296000s
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' then 1 else 0 end) as txn_bz65_cnt_1296000s
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' and t2.hh in ('23','00','01','02','03','04') then 1 else 0 end) as txn_bz65_zsh_cnt_1296000s
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' then 1 else 0 end)/nullif(sum(t2.dcflag),0) as txn_bz65_c_ratio_1296000s
,sum(case when t2.dcflag=1 and t2.bz like '%提现%' and t2.hh in ('23','00','01','02','03','04') then t2.amountamt else 0 end) as txn_bz65_zsh_amt_1296000s
--#NAME?
,sum(case when t2.bz like '%支付宝转账%' then amountamt else 0 end) as txn_zfb0_amt_1296000s
,sum(case when mod(amountamt,1000)=0 then 1 else 0 end) as txn_intk_cnt_1296000s
,min(case when dcflag=0 then d_ye_prop else null end) as txn_dyeprop_min_1296000s
--ADD 21
,min(t2.zhye) as txn_zhye_min_1296000s
--ADD 22
,count(t2.cardno) as txn_cnt_1296000s
--#NAME?
,sum(case when t2.dcflag=0 and t2.bz like '%商品%' and t2.lag_bz like '%提现%' and t2.lag_dcflag=1 and t2.trantime-t2.lag_trantime<300 then 1 else 0 end) as txn_tx_xf_cnt_1296000s
,sum(case when t2.dcflag=0 and t2.bz like '%商品%' and t2.lag_bz like '%借款%' and t2.lag_dcflag=1 and t2.trantime-t2.lag_trantime<300 then 1 else 0 end) as txn_loan_xf_cnt_1296000s
--
,sum(case when t2.dcflag=0 and t2.bz like '%基金%' then 1 else 0 end) as txn_fund_cnt_1296000s
,sum(case when t2.dcflag=0 and t2.bz like '%基金%' then t2.amountamt else 0 end) as txn_fund_amt_1296000s
-- 1208
,avg(case when cast(hh as int) in (23,0,1,2,3) then 1 else 0 end) as txn_sttime_prop_1296000s
-- 1208
,sum(case when t2.bz like '%跨行网银贷记%' and t2.amountamt<100 then 1 else 0 end) as txn_smlprob_bank_amt_1296000s
-- 1209
,min(case when dcflag=1 then c_ye end) as txn_cye_min_1296000s
-- 1209
,sum(case when dcflag=1 and length(dfzhmc)>=2 and length(dfzhmc)<=3 then amountamt else 0 end)/nullif(sum(case when dcflag=1 then amountamt else 0 end),0) as txn_c_ind_amt_ratio_1296000s
-- 1209
,sum(amountamt) as txn_tot_amt_1296000s
-- 1209
,count(distinct case when t2.custname<>t2.dfzhmc and length(t2.dfzhmc)<=3 and length(t2.dfzhmc)>=2 then t2.dfzhmc end) as txn_fnm_dfzhmc_cnt_1296000s
-- 1209
,count(distinct case when t2.custname<>t2.dfzhmc and length(t2.dfzhmc)<=3 and length(t2.dfzhmc)>=2 and t2.province<>'广东' then t2.dfzhmc end) as txn_fnm_dfzhmc_dffloc_cnt_1296000s
-- 1100
,sum(case when dcflag=1 and (bz like '%商品%' or bz like '%商户%') then 1 else 0 end) as txn_wg_c_1296000s
from tmp1 t1
left join tmp2 t2 on t1.cardno=t2.cardno and t1.trantime-t2.trantime>0 and t1.trantime-t2.trantime<=1296000
group by t1.tradnum
;

-- 行号: 1106 - 1148
-- 来源: 图片, (合并)
1051
--select count(1) from SASSJLIB.td003649_y1rc_txn_base_259200s_par1;
drop table if exists SASSJLIB.td003649_y1rc_txn_base_259200s_par1;
create table SASSJLIB.td003649_y1rc_txn_base_259200s_par1
as
with tmp1 as (
select
t1.tradnum
,t1.trantime
,t1.cardno
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
), tmp2 as (
select *
,case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end as d_ye
,case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end as c_ye
,amountamt/(case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end) as d_ye_prop
,amountamt/(case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end) as c_ye_prop
,case when province<>'广东' then 1 when province is not null then 0 end as f_dffloc
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3
)
select
t1.tradnum as tradnum_txn_base_259200s_par1
--#NAME?
,min(zhye) as txn_zhye_min_259200s
--#NAME?
,avg(zhye) as txn_zhye_avg_259200s
--#NAME?
,stddev_pop(zhye) as txn_zhye_std_259200s
--#NAME?
,max(case when dcflag=1 then amountamt else 0 end) as txn_c_max_amt_259200s
--#NAME?
,avg(case when dcflag=1 then amountamt else null end) as txn_in_avg_259200s
--#NAME?
,min(amountamt) as txn_min_amt_259200s
,sum(case when dcflag=1 then amountamt else 0 end) as txn_in_amt_259200s
,sum(case when dcflag=0 then amountamt else 0 end) as txn_out_amt_259200s
-- 1208
,sum(case when t2.bz like '%跨行网银贷记%' and t2.amountamt<100 then 1 else 0 end) as txn_smlprob_bank_amt_259200s
from tmp1 t1
left join tmp2 t2 on t1.cardno=t2.cardno and t1.trantime-t2.trantime>0 and t1.trantime-t2.trantime<=259200
group by t1.tradnum
;

-- 行号: 1149 - 1178
-- 来源: 图片,, (合并)
1051
--select count(1) from SASSJLIB.td003649_y1rc_txn_base_10800s_par1;
drop table if exists SASSJLIB.td003649_y1rc_txn_base_10800s_par1;
create table SASSJLIB.td003649_y1rc_txn_base_10800s_par1
as
with tmp1 as (
select
t1.tradnum
,t1.trantime
,t1.cardno
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
), tmp2 as (
select *
,case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end as d_ye
,case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end as c_ye
,amountamt/(case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end) as d_ye_prop
,amountamt/(case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end) as c_ye_prop
,case when province<>'广东' then 1 when province is not null then 0 end as f_dffloc
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3
)
select
t1.tradnum as tradnum_txn_base_10800s_par1
--#NAME?
,sum(case when to_char(floor(amountamt/10) % 10)||to_char(floor(amountamt) % 10) = '00' then amountamt else 0 end) as txn_tensones00_amt_10800s
--#NAME?
,sum(case when mod(amountamt,100)=0 then 1 else 0 end) as txn_intb_cnt_10800s
from tmp1 t1
left join tmp2 t2 on t1.cardno=t2.cardno and t1.trantime-t2.trantime>0 and t1.trantime-t2.trantime<=10800
group by t1.tradnum
;

-- 行号: 1180 - 1189
-- 来源: 图片
--select count(1),count(distinct cardno),count(distinct cust_id) from SASSJLIB.td003649_y1rc_acct_main;
drop table if exists SASSJLIB.td003649_y1rc_acct_main;
create table SASSJLIB.td003649_y1rc_acct_main
as
select
cardno,stm_dt as stm_dt,cust_id
,to_char(date(stm_dt)-interval '2 day','yyyy-mm-dd') as stm_dt -- 注意：日期回溯2天
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs
group by cardno,cust_id,stm_dt
;

-- 行号: 1192 - 1219
-- 来源: 图片, (合并)
1208
269
--select count(1) from SASSJLIB.td003649_y1rc_txn_base_15d_par1;
drop table if exists SASSJLIB.td003649_y1rc_txn_base_15d_par1;
create table SASSJLIB.td003649_y1rc_txn_base_15d_par1
as
with tmp1 as (
select
t1.stm_dt
,t1.cust_id
,t1.cardno
from SASSJLIB.td003649_y1rc_acct_main t1
), tmp2 as (
select *
,case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end as d_ye
from SASSJLIB.td003649_y1rc_acct_main t1
-- 此处源码可能有截断，疑似引用的是 txn0_trainwb_gs_tmp4_4
), tmp2 as ( -- 修正：实际逻辑
select *
,case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end as d_ye
,case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end as c_ye
,amountamt/(case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end) as d_ye_prop
,amountamt/(case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end) as c_ye_prop
,case when province<>'广东' then 1 when province is not null then 0 end as f_dffloc
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_4
)
select
t1.cardno as cardno_txn_base_15d_par1
,t1.stm_dt as stm_dt_txn_base_15d_par1
1208
,sum(case when dcflag=1 then 1 else 0 end) as txn_c_cnt_15d
from tmp1 t1
left join tmp2 t2 on t1.cardno=t2.cardno and date(t1.stm_dt)-date(t2.stm_dt)>=0 and date(t1.stm_dt)-date(t2.stm_dt)<15
group by t1.cardno,t1.stm_dt
;

-- 行号: 1221 - 1304
-- 来源: 图片,,, (合并)
269
--select count(1) from SASSJLIB.td003649_y1rc_txn_base_180d_par1;
drop table if exists SASSJLIB.td003649_y1rc_txn_base_180d_par1;
create table SASSJLIB.td003649_y1rc_txn_base_180d_par1
as
with tmp1 as (
select
t1.stm_dt
,t1.cust_id
,t1.cardno
from SASSJLIB.td003649_y1rc_acct_main t1
), tmp2 as (
select *
,case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end as d_ye
,case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end as c_ye
,amountamt/(case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end) as d_ye_prop
,amountamt/(case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end) as c_ye_prop
,case when province<>'广东' then 1 when province is not null then 0 end as f_dffloc
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_4
)
select
t1.cardno as cardno_txn_base_180d_par1
,t1.stm_dt as stm_dt_txn_base_180d_par1
--#NAME?
,min(case when dcflag=0 then d_ye end) as txn_dye_min_180d
--#NAME?
,avg(case when dcflag=0 then d_ye_prop else null end) as txn_dyeprop_avg_180d
--#NAME?
,sum(case when dcflag=0 and d_ye_prop<=0.05 then 1 else 0 end)/nullif(sum(1-dcflag),0) as txn_dyeprop05_ratio_180d
--#NAME?
,count(distinct case when fdfhh=1 and f_diff_name_62=1 and dcflag=1 then t2.dfhmc else null end) as txn_c_62_ndfhh_cnt_180d
--#NAME?
,sum(case when fhh=1 and dcflag=1 then t2.amountamt else 0 end) as txn_call_nhh_txn_amt_180d
--ADD 3
,sum(case when t2.bz like '%提现%' and t2.hh in ('23','00','01','02','03','04') then 1 else 0 end) as txn_bz65_zsh_cnt_180d
--#NAME?
,count(distinct case when fdfhh=1 and f_diff_name_62=1 and dcflag=1 then t2.dfhmc else null end) as txn_c_62_ndfhh_cnt_180d -- 注意：重复字段，图49中为1251行
--ADD 3
,sum(case when fhh=1 and dcflag=1 then t2.amountamt else 0 end) as txn_call_nhh_txn_amt_180d -- 重复
,sum(case when t2.bz like '%提现%' and t2.hh in ('23','00','01','02','03','04') then 1 else 0 end) as txn_bz65_zsh_cnt_180d -- 重复
,sum(case when t2.bz like '%提现%' then 1 else 0 end)/nullif(count(t2.cardno),0) as txn_bz65_c_ratio_180d
--ADD 2
,count(distinct case when fnwp2=1 and f_diff_name=1 and dcflag=1 then dfzhmc end) as txn_c_nwp_dis_cnt_180d
--#NAME?
,sum(case when fnwp2=1 and f_diff_name=1 and dcflag=1 and (substr(dfzh,1,2)='62' or substr(dfzh,1,4)='2088') then 1 else 0 end)/nullif(sum(case when f_diff_name=1 and dcflag=0 and (substr(dfzh,1,2)='62' or substr(dfzh,1,4)='2088') then 1 else 0 end),0) as txn_fnwp2_d_ratio_180d
--ADD 1
,sum(case when f_diff_name=1 and fnwp2=1 and dcflag=1 then 1 else 0 end)/nullif(sum(case when f_diff_name=1 and dcflag=1 then 1 else 0 end),0) as txn_fnwp2_ratio_180d
--ADD 6
,sum(case when f_diff_name=1 and fnwp2=1 and dcflag=0 and (substr(dfzh,1,2)='62' or substr(dfzh,1,4)='2088') then 1 else 0 end)/nullif(sum(case when f_diff_name=1 and dcflag=0 and (substr(dfzh,1,2)='62' or substr(dfzh,1,4)='2088') then 1 else 0 end),0) as txn_fnwp2_ratio_180d -- 变量名重复
--#NAME?
,sum(case when f_diff_name=1 and t2.hh in ('01','02','03','04','05') and dcflag=0 and (substr(dfzh,1,2)='62' or substr(dfzh,1,4)='2088') then 1 else 0 end)/nullif(sum(case when f_diff_name=1 and dcflag=0 and (substr(dfzh,1,2)='62' or substr(dfzh,1,4)='2088') then 1 else 0 end),0) as txn_zhdik_cnt_180d
--ADD 7
,count(distinct case when fnwp2=1 and f_diff_name=1 and dcflag=0 and province<>'广东' then dfzhmc end) as txn_d_nwp_dis_cnt_180d
--ADD 8
,count(distinct case when substr(custname,1,2)=substr(dfzhmc,1,2) and substr(dfzh,1,4)<>'6223' and substr(dfzh,1,2)='62' then dfzh end) as txn_c_62_ndfhh_cnt_180d -- 注意变量名，图50行1271为 txn_d_nwp...应为 txn_dfdfk_cnt_180d
--ADD 10
,sum(case when t2.bz like '%代付代付退款%' then 1 else 0 end) as txn_dfdfk_cnt_180d
--ADD 10
,count(distinct case when fnwp2=1 and f_diff_name=1 and dcflag=1 and province<>'广东' then dfzhmc end) as txn_c_nwp_dffloc_dis_cnt_180d
--ADD 11
,sum(case when t2.abstr like '%折转贷记卡%' then 1 else 0 end) as txn_zhdjk_cnt_180d
--ADD 12
,count(distinct t2.stm_dt) as txn_atvd_cnt_180d
--#NAME?
,max(amountamt) as txn_amt_max_180d
,max(case when dcflag=1 then amountamt else 0 end) as txn_amt_c_max_180d
--#NAME?
,sum(case when dcflag=0 and (bz like '%商品%' or bz like '%商户%') then 1 else 0 end)/nullif(sum(1-dcflag),0) as txn_wg_d_ratio_180d
,sum(case when dcflag=1 and (bz like '%商品%' or bz like '%商户%') then 1 else 0 end)/nullif(sum(dcflag),0) as txn_wg_c_ratio_180d
--#NAME?
,sum(case when t2.dcflag=0 and t2.bz like '%商品%' and t2.lag_bz like '%提现%' and t2.lag_dcflag=1 and t2.trantime-t2.lag_trantime<300 then 1 else 0 end) as txn_tx_xf_cnt_180d
-- 0
,sum(case when t2.abstr like '%取款%' or t2.abstr like '%现金%') and dcflag=0 then t2.amountamt else 0 end) as txn_qx_d_180d
,sum(case when t2.abstr like '%取款%' or t2.abstr like '%现金%') and dcflag=0 then 1 else 0 end)/nullif(sum(1-dcflag),0) as txn_qx_d_ratio_180d
,sum(case when t2.abstr like '%取款%' or t2.abstr like '%现金%') and dcflag=0 then 1 else 0 end) as txn_qx_d_cnt_180d
-- 1212
,sum(case when t2.bz like '%提现%' and t2.dcflag=1 then 1 else 0 end) as txn_bz65_cnt_180d
,sum(case when t2.bz like '%提现%' and t2.dcflag=1 then 1 else 0 end)/nullif(sum(t2.dcflag),0) as txn_bz65_cc_ratio_180d
,sum(case when t2.bz like '%抖音%' and t2.dcflag=1 then 1 else 0 end) as txn_bzdy_cnt_180d
,sum(case when t2.bz like '%抖音%' and t2.dcflag=1 then 1 else 0 end)/nullif(sum(t2.dcflag),0) as txn_bzdy_c_ratio_180d
,sum(case when t2.bz like '%提现%' and t2.dcflag=1 then 1 else 0 end)/nullif(sum(t2.dcflag),0) as txn_bz65dy_cnt_180d -- 变量名疑似笔误，图50行1300
,sum(case when t2.bz like '%提现%' and t2.bz like '%抖音%' and t2.dcflag=1 then 1 else 0 end)/nullif(sum(t2.dcflag),0) as txn_bz65dy_c_ratio_180d
,count(distinct case when t2.fnwp2=1 and t2.f_diff_name=1 and t2.dcflag=0 and (substr(t2.dfzh,1,2)='62' or substr(t2.dfzh,1,4)='2088') then dfzhmc end) as txn_d_zfb_cd_nwp_dis_cnt_180d
from tmp1 t1
left join tmp2 t2 on t1.cardno=t2.cardno and date(t1.stm_dt)-date(t2.stm_dt)>=0 and date(t1.stm_dt)-date(t2.stm_dt)<178
group by t1.cardno,t1.stm_dt
;

-- 行号: 1305 - 1334
-- 来源: 图片, (合并)
269
--select count(1) from SASSJLIB.td003649_y1rc_txn_base_360d_par1;
drop table if exists SASSJLIB.td003649_y1rc_txn_base_360d_par1;
create table SASSJLIB.td003649_y1rc_txn_base_360d_par1
as
with tmp1 as (
select
t1.stm_dt
,t1.cust_id
,t1.cardno
from SASSJLIB.td003649_y1rc_acct_main t1
), tmp2 as (
select *
,case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end as d_ye
,case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end as c_ye
,amountamt/(case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end) as d_ye_prop
,amountamt/(case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end) as c_ye_prop
,case when province<>'广东' then 1 when province is not null then 0 end as f_dffloc
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3
)
select
t1.cardno as cardno_txn_base_360d_par1
,t1.stm_dt as stm_dt_txn_base_360d_par1
--#NAME?
--ADD 13
,min(zhye) as txn_zhye_min_360d
from tmp1 t1
left join tmp2 t2 on t1.cardno=t2.cardno and date(t1.stm_dt)-date(t2.stm_dt)>=0 and date(t1.stm_dt)-date(t2.stm_dt)<358
group by t1.cardno,t1.stm_dt
;

-- 行号: 1335 - 1374
-- 来源: 图片, (合并)
269
--select count(1) from SASSJLIB.td003649_y1rc_txn_base_15d180d_gap_par1;
drop table if exists SASSJLIB.td003649_y1rc_txn_base_15d180d_gap_par1;
create table SASSJLIB.td003649_y1rc_txn_base_15d180d_gap_par1
as
with tmp1 as (
select
t1.stm_dt
,t1.cust_id
,t1.cardno
from SASSJLIB.td003649_y1rc_acct_main t1
), tmp2 as (
select *
,case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end as d_ye
,case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end as c_ye
,amountamt/(case when (zhye+amountamt)>0 then (zhye+amountamt) else 0.1 end) as d_ye_prop
,amountamt/(case when (zhye-amountamt)>0 then (zhye-amountamt) else 0.1 end) as c_ye_prop
,case when province<>'广东' then 1 when province is not null then 0 end as f_dffloc
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp4_3
)
select
t1.cardno as cardno_txn_base_15d180d_gap_par1
,t1.stm_dt as stm_dt_txn_base_15d180d_gap_par1
--#NAME?
,sum(case when t2.bz like '%财付通%' then amountamt else 0 end) as txn_bz28_amt_15d180d_gap
--#NAME?
,sum(case when t2.bz like '%商品%' then amountamt else 0 end) as txn_bz85_amt_15d180d_gap
--#NAME?
,avg(case when dcflag=0 then d_ye end) as txn_dye_avg_15d180d_gap
--#NAME?
,sum(case when dcflag=1 and c_ye_prop>=0.95 then 1 else 0 end)/nullif(sum(dcflag),0) as txn_cyeprop95_ratio_15d180d_gap
,sum(case when dcflag=1 and c_ye_prop>=0.95 then 1 else 0 end) as txn_cyeprop95_cnt_15d180d_gap
,min(case when dcflag=0 then d_ye_prop else null end) as txn_dyeprop_min_15d180d_gap
,sum(case when fabstr=1 and f_diff_name=1 and dcflag=1 then t2.amountamt else 0 end) as txn_c_nabstr_txn_amt_15d180d_gap
--ADD 9
,count(distinct t2.stm_dt) as txn_atvd_cnt_15d180d_gap
from tmp1 t1
left join tmp2 t2 on t1.cardno=t2.cardno and date(t1.stm_dt)-date(t2.stm_dt)>=14 and date(t1.stm_dt)-date(t2.stm_dt)<178
group by t1.cardno,t1.stm_dt
;

-- 行号: 1376 - 1392
-- 来源: 图片, (合并)
1051
--select count(1) from SASSJLIB.td003649_y1rc_txn_base_ratio_15d180d;
drop table if exists SASSJLIB.td003649_y1rc_txn_base_ratio_15d180d;
create table SASSJLIB.td003649_y1rc_txn_base_ratio_15d180d
as
select
t0.tradnum as tradnum_ratio_15d180d
--#NAME?
,txn_intk_c as txn_intk_cnt_ratio_15d180d -- 此处原代码被截断，推测为 txn_intk_c_nt_ratio_15d180d
--#NAME?
,txn_dyeprop as txn_dyeprop_min_ratio_15d180d
--#NAME?
,txn_c_nab as txn_c_nabstr_txn_amt_ratio_15d180d
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t0
inner join SASSJLIB.td003649_y1rc_txn_base_1296000s_par1 t1 on t0.tradnum=t1.tradnum_txn_base_1296000s_par1
inner join SASSJLIB.td003649_y1rc_txn_base_15d180d_gap_par1 t4 on t0.cardno=t4.cardno_txn_base_15d180d_gap_par1
;

-- 行号: 1394 - 1421
-- 来源: 图片,,
--#NAME?
269
--select count(1) from SASSJLIB.td003649_y1rc_txn_frqoppo_ind_trainwb_gs_180d;
drop table if exists SASSJLIB.td003649_y1rc_txn_frqoppo_ind_trainwb_gs_180d;
create table SASSJLIB.td003649_y1rc_txn_frqoppo_ind_trainwb_gs_180d
as
with tmp1 as (
select cardno,trantime,amountamt,dfzh,stm_dt
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2
where f_diff_name=1
), tmp2 as (
select
t1.cardno,t1.stm_dt,t2.dfzh
,count(t2.cardno) as cnt
from SASSJLIB.td003649_y1rc_acct_main t1
left join tmp1 t2
on t1.cardno = t2.cardno
and date(t1.stm_dt) - date(t2.stm_dt) between 0 and (180-1)
group by t1.cardno,t1.stm_dt,t2.dfzh
)
select
t1.cardno as cardno_frqoppo
,t1.stm_dt as stm_dt_frqoppo
,avg(case when cnt>1 then 1 else 0 end) as txn_repeatoppo_ratio_180d
from tmp2 t1
group by t1.cardno,t1.stm_dt
;

-- 行号: 1422 - 1467
-- 来源: 图片,, (合并)
--时间间隔 跨天
269 244
--select count(1),count(distinct cardno) from SASSJLIB.td003649_y1rc_txn_ti_trainwb_gs_180d;
drop table if exists SASSJLIB.td003649_y1rc_txn_ti_trainwb_gs_180d;
create table SASSJLIB.td003649_y1rc_txn_ti_trainwb_gs_180d
as
with tmp1 as (
select
t1.cardno,t1.stm_dt
,t2.trantime as trantime2
,t2.tradnum as tradnum2
,t2.dcflag
,t2.amountamt
,t2.f_diff_name
,t2.f_diff_name_62
from SASSJLIB.td003649_y1rc_acct_main t1
left join SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2 t2
on t1.cardno = t2.cardno
and date(t1.stm_dt) - date(t2.stm_dt) between 0 and (180-1)
), tmp2 as (
select
t1.cardno,t1.stm_dt
,t1.trantime2
,t1.tradnum2
,t1.dcflag
,t1.amountamt
,t1.f_diff_name_62
,count(1) over (partition by t1.cardno,t1.stm_dt) as total_cnt
,lag(t1.amountamt,1) over (partition by t1.cardno,t1.stm_dt order by t1.trantime2,t1.tradnum2) as pre_amt
,lag(t1.dcflag,1) over (partition by t1.cardno,t1.stm_dt order by t1.trantime2,t1.tradnum2) as pre_dcflag
,t1.trantime2 - lag(t1.trantime2,1) over (partition by t1.cardno,t1.stm_dt order by t1.trantime2,t1.tradnum2) as diff_tm
from tmp1 t1
),tmp3 as (
select
t1.*
,dense_rank() over (partition by t1.cardno,t1.stm_dt order by t1.diff_tm) as dk_dfftm
from tmp2 t1
)
select
t1.cardno as cardno_ti
,t1.stm_dt as stm_dt_ti
,avg(case when (dk_dfftm/total_cnt)<0.015 and (dk_dfftm/total_cnt)>0.005 then diff_tm end) as txn_ti_p01_180d
from tmp3 t1
group by t1.cardno,t1.stm_dt
;
-- 行号: 1468 - 1504
-- 来源: 图片,, (合并)
--3092 244
--select count(1),count(distinct cardno) from SASSJLIB.td003649_y1rc_txn_equalamt_trainwb_gs_180d;
drop table if exists SASSJLIB.td003649_y1rc_txn_equalamt_trainwb_gs_180d;
create table SASSJLIB.td003649_y1rc_txn_equalamt_trainwb_gs_180d
as
with t1 as (
select
t1.cardno
,t1.stm_dt
,t2.tradnum as tradnum2
,t2.trantime as trantime2
,t2.dcflag
,t2.amountamt
,case when t2.dcflag = lag(t2.dcflag,1) over (partition by t1.cardno, t1.stm_dt order by t2.trantime, t2.tradnum) then 0 else 1 end as flag_change
left join SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2 t2
on t1.cardno = t2.cardno
and date(t1.stm_dt) - date(t2.stm_dt) between 0 and (180-1)
), t2 as (
select
cardno
,stm_dt
,tradnum2
,trantime2
,dcflag
,amountamt
,sum(flag_change) over (partition by cardno, stm_dt order by trantime2,tradnum2) as group_id
from t1
), t3 as (
select
cardno
,stm_dt
,dcflag
,group_id
,sum(amountamt) as amountamt
,count(amountamt) as cnt
,max(trantime2) as trantime2
from t2
group by
cardno
,stm_dt
,dcflag
,group_id
),t4 as ( -- 图行1511
select
cardno
,stm_dt
,trantime2
,group_id
,amountamt
,cnt
,sum(case when dcflag = 1 then 1 else 0 end) over (partition by cardno, stm_dt order by trantime2, group_id) as group1_id
,sum(case when dcflag = 0 then 1 else 0 end) over (partition by cardno, stm_dt order by trantime2, group_id) as group2_id
--par4
-- 行号: 1505 - 1554
-- 来源: 图片, (合并)
from t3
),t5 as (
select
cardno
,stm_dt
,group_id
,max(amountamt) over (partition by cardno, stm_dt, group1_id) as amn1
,max(amountamt) over (partition by cardno, stm_dt, group2_id) as amt2
,max(cnt) over (partition by cardno, stm_dt, group1_id) as cnt1
,max(cnt) over (partition by cardno, stm_dt, group2_id) as cnt2
,amountamt/nullif(lag(amountamt,1) over (partition by cardno, stm_dt, group1_id order by trantime2, group_id),0) as mnm1
,amountamt/nullif(lag(amountamt,1) over (partition by cardno, stm_dt, group2_id order by trantime2, group_id),0) as mnm2
from t4
)
select
cardno as cardno_equalamt
,stm_dt as stm_dt_equalamt
,greatest(sum(case when mnm1 between 0.80 and 1.20 then 1 else 0 end),sum(case when mnm2 between 0.80 and 1.20 then 1 else 0 end)) as acct_equalamt_cnt_180d
,greatest(max(case when mnm1 between 0.80 and 1.20 then amt1 else 0 end),max(case when mnm2 between 0.80 and 1.20 then amt2 else 0 end)) as acct_equalamt_maxamt_180d
,greatest(max(case when mnm1 between 0.80 and 1.20 then cnt1 else 0 end),max(case when mnm2 between 0.80 and 1.20 then cnt2 else null end)) as acct_equalamt_maxcnt_180d
,least(stddev_pop(mnm1),stddev_pop(mnm2)) as acct_equalamt_mnm_std_180d
,greatest(avg(mnm1),avg(mnm2)) as acct_equalamt_mnm_avg_180d
,greatest(max(mnm1),max(mnm2)) as acct_equalamt_mnm_max_180d
,least(min(mnm1),min(mnm2)) as acct_equalamt_mnm_min_180d
from t5
group by
cardno
,stm_dt
;
#NAME?
1208
3092 244
--select count(1),count(distinct cardno) from SASSJLIB.td003649_y1rc_txn_equalamt_trainwb_gs_15d;
drop table if exists SASSJLIB.td003649_y1rc_txn_equalamt_trainwb_gs_15d;
create table SASSJLIB.td003649_y1rc_txn_equalamt_trainwb_gs_15d
as
with t1 as (
select
t1.cardno
,t1.stm_dt
,t2.tradnum as tradnum2
,t2.trantime as trantime2
,t2.dcflag
,t2.amountamt
,case when t2.dcflag = lag(t2.dcflag,1) over (partition by t1.cardno, t1.stm_dt order by t2.trantime, t2.tradnum) then 0 else 1 end as flag_change
from SASSJLIB.td003649_y1rc_acct_main t1
left join SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2 t2
on t1.cardno = t2.cardno
and date(t1.stm_dt) - date(t2.stm_dt) between 0 and (15-1)
), t2 as (
select
cardno
,stm_dt
,tradnum2
,trantime2
,dcflag
,amountamt
,sum(flag_change) over (partition by cardno, stm_dt order by trantime2,tradnum2) as group_id
from t1
), t3 as (
select
cardno
,stm_dt
,dcflag
,group_id
,sum(amountamt) as amountamt
,count(amountamt) as cnt
,max(trantime2) as trantime2
from t2
group by
cardno
,stm_dt
,dcflag
,group_id
),t4 as (
select
cardno
,stm_dt
,trantime2
,group_id
,amountamt
,cnt
,sum(case when dcflag = 1 then 1 else 0 end) over (partition by cardno, stm_dt order by trantime2, group_id) as group1_id
,sum(case when dcflag = 0 then 1 else 0 end) over (partition by cardno, stm_dt order by trantime2, group_id) as group2_id
from t3
),t5 as (
select
cardno
,stm_dt
,group_id
,max(amountamt) over (partition by cardno, stm_dt, group1_id) as amt1
,max(amountamt) over (partition by cardno, stm_dt, group2_id) as amt2
,max(cnt) over (partition by cardno, stm_dt, group1_id) as cnt1
,max(cnt) over (partition by cardno, stm_dt, group2_id) as cnt2
,amountamt/nullif(lag(amountamt,1) over (partition by cardno, stm_dt, group1_id order by trantime2, group_id),0) as mnm1
,amountamt/nullif(lag(amountamt,1) over (partition by cardno, stm_dt, group2_id order by trantime2, group_id),0) as mnm2
from t4
)
select
cardno as cardno_equalamt
,stm_dt as stm_dt_equalamt
,greatest(sum(case when mnm1 between 0.80 and 1.20 then 1 else 0 end),sum(case when mnm2 between 0.80 and 1.20 then 1 else 0 end)) as acct_equalamt_cnt_15d
,greatest(max(case when mnm1 between 0.80 and 1.20 then amt1 else 0 end),max(case when mnm2 between 0.80 and 1.20 then amt2 else 0 end)) as acct_equalamt_maxamt_15d
,greatest(max(case when mnm1 between 0.80 and 1.20 then cnt1 else 0 end),max(case when mnm2 between 0.80 and 1.20 then cnt2 else null end)) as acct_equalamt_maxcnt_15d
,least(stddev_pop(mnm1),stddev_pop(mnm2)) as acct_equalamt_mnm_std_15d
,greatest(avg(mnm1),avg(mnm2)) as acct_equalamt_mnm_avg_15d
,greatest(max(mnm1),max(mnm2)) as acct_equalamt_mnm_max_15d
,least(min(mnm1),min(mnm2)) as acct_equalamt_mnm_min_15d
from t5
group by
cardno
,stm_dt
;
#NAME?
--select count(1) from SASSJLIB.td003649_y1rc_acct_kline_trainwb_gs;
drop table if exists SASSJLIB.td003649_y1rc_acct_kline_trainwb_gs;
create table SASSJLIB.td003649_y1rc_acct_kline_trainwb_gs
as
with tmp1 as (
select
t1.cardno
,t1.stm_dt
,max(case when rk_ye = 1 then zhye end) as daily_close
from (select * from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_gs_tmp2 where date(stm_dt)>=(date('2022-07-01'))) t1
group by
t1.cardno
,t1.stm_dt
), tmp2 as (
select
cardno
,stm_dt
,daily_close
from tmp1
), tmp3 as (
select
cardno
,stm_dt
,daily_close
--#NAME?
,avg(daily_close) over (partition by cardno order by stm_dt rows between 13 preceding and current row) as ma14
--#NAME?
,avg(daily_close) over (partition by cardno order by stm_dt rows between 11 preceding and current row) - avg(daily_close)
over (partition by cardno order by stm_dt rows between 25 preceding and current row) as macd
from tmp2
)
select
cardno
,stm_dt
--#NAME?
,macd
--#NAME?
,case when daily_close > 1.05 * ma14 then 1 else 0 end as high_close_flag
from tmp3
;
#NAME?
269
--select count(1) from SASSJLIB.td003649_y1rc_acct_kline_trainwb_gs_90d;
drop table if exists SASSJLIB.td003649_y1rc_acct_kline_trainwb_gs_90d;
create table SASSJLIB.td003649_y1rc_acct_kline_trainwb_gs_90d
as
select
t1.cardno as cardno_kline
,t1.stm_dt as stm_dt_kline
--#NAME?
,avg(case when macd < 0 then 1 else 0 end) as acct_kline_macd_negative_ratio_90d
--#NAME?
,sum(case when high_close_flag = 1 then 1 else 0 end) as acct_kline_high_close_cnt_90d
--#NAME?
,avg(case when high_close_flag = 1 then 1 else 0 end) as acct_kline_high_close_ratio_90d
from SASSJLIB.td003649_y1rc_acct_main t1
left join SASSJLIB.td003649_y1rc_acct_kline_trainwb_gs t2
on t1.cardno = t2.cardno
and date(t1.stm_dt) - date(t2.stm_dt) between 0 and (90-1)
group by
t1.cardno
,t1.stm_dt
;
---------------------------------ip---------------------------------
---------------------------------180d---------------------------------
889735
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_180d_1;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_180d_1;
create table SASSJLIB.td003649_y1rc_trck_ip_180d_1
as
select
t1.cardno,t1.stm_dt,t2.TX_DESC,t2.ip
from SASSJLIB.td003649_y1rc_acct_main t1
left join (
select ip,TX_DESC,TX_TIME,cust_id
from SASSJLIB.td003649_y1rc_trck_flw
where trim(TX_DESC) in (
'当前用户交易记录Excel生成跨中心编排'
,'查询转账手续费金额'
,'网上银行设备首次登录标志查询'
,'导航标签产品查询'
,'查询用户老带新活动奖励'
,'限额管理详情信息查询'
,'海外人才客户白名单标志查询'
,'总资产查询'
,'交易签名申请'
,'该微服务用于跨中心编排私行客户标识查询的管理。'
,'成长规则成长值发放跨中心编排'
,'客户贷款还款详情查询跨中心编排'
,'支持银行列表查询'
,'支付中心转账文档转换签章'
,'绑卡账户安全锁信息列表查询跨中心编排'
,'面核标志查询'
,'税融预申请查询'
,'客户贷款核心账单查询跨中心编排'
,'本微服务用于跨中心编排了贷款账户信息查询的操作'
,'贷款产品首页信息查询跨中心编排'
,'限额管理信息修改'
,'查询证件是否过期'
)
) t2
on t1.cust_id=t2.cust_id
and date(t1.stm_dt) - date(SUBSTR(TO_CHAR(t2.TX_TIME),1,10)) between 0 and (180-1)
group by t1.cardno,t1.stm_dt,t2.TX_DESC,t2.ip
;
269
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_180d;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_180d;
create table SASSJLIB.td003649_y1rc_trck_ip_180d
as
select
t1.cardno as cardno_txn_track_ip_180d
,t1.stm_dt as stm_dt_txn_track_ip_180d
--#NAME?
,sum(case when TX_DESC='本微服务用于跨中心编排了贷款账户信息查询的操作' then 1 end) as trck_pg10046_ip_cnt_180d
--#NAME?
,sum(case when TX_DESC='查询证件是否过期' then 1 end) as trck_pg10012_ip_cnt_180d
--#NAME?
--ADD 4
,sum(case when TX_DESC='限额管理详情信息查询' then 1 end) as trck_pg10089_ip_cnt_180d
--#NAME?
--ADD 5
,sum(case when TX_DESC='总资产查询' then 1 end) as trck_pg10021_ip_cnt_180d
from SASSJLIB.td003649_y1rc_trck_ip_180d_1 t1
group by t1.cardno,t1.stm_dt
;

-- 行号: 1762 - 1804
-- 来源: 图片, (合并)
-------------------------ip---------------------------
-------------------------gap---------------------------
889735
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_15d180d_gap_1;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_15d180d_gap_1;
create table SASSJLIB.td003649_y1rc_trck_ip_15d180d_gap_1
as
select
t1.cardno,t1.stm_dt,t2.TX_DESC,t2.ip
from SASSJLIB.td003649_y1rc_acct_main t1
left join (
select ip,TX_DESC,TX_TIME,cust_id
from SASSJLIB.td003649_y1rc_trck_flw
where trim(TX_DESC) in (
'当前用户交易记录Excel生成跨中心编排'
,'查询转账手续费金额'
,'网上银行设备首次登录标志查询'
,'导航标签产品查询'
,'查询用户老带新活动奖励'
,'限额管理详情信息查询'
,'海外人才客户白名单标志查询'
,'总资产查询'
,'交易签名申请'
,'该微服务用于跨中心编排私行客户标识查询的管理。'
,'成长规则成长值发放跨中心编排'
,'客户贷款还款详情查询跨中心编排'
,'支持银行列表查询'
,'支付中心转账文档转换签章'
,'绑卡账户安全锁信息列表查询跨中心编排'
,'面核标志查询'
,'税融预申请查询'
,'客户贷款核心账单查询跨中心编排'
,'本微服务用于跨中心编排了贷款账户信息查询的操作'
,'贷款产品首页信息查询跨中心编排'
,'限额管理信息修改'
,'查询证件是否过期'
)
) t2
on t1.cust_id=t2.cust_id
and date(t1.stm_dt) - date(SUBSTR(TO_CHAR(t2.TX_TIME),1,10)) between 14 and (180-1)
group by t1.cardno,t1.stm_dt,t2.TX_DESC,t2.ip
;
269
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_15d180d_gap;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_15d180d_gap;
create table SASSJLIB.td003649_y1rc_trck_ip_15d180d_gap
as
select
t1.cardno as cardno_txn_track_ip_15d180d_gap
,t1.stm_dt as stm_dt_txn_track_ip_15d180d_gap
,sum(case when TX_DESC='限额管理详情信息查询' then 1 end) as trck_pg10089_ip_cnt_15d180d_gap
from SASSJLIB.td003649_y1rc_trck_ip_15d180d_gap_1 t1
group by t1.cardno,t1.stm_dt
;

-- 行号: 1815 - 1878
-- 来源: 图片, (合并)
--------------------------IP--------------------------
--------------------------实时-------------------------
--------------------------1209600S-------------------------
8059670
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_1209600s_1;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_1209600s_1;
create table SASSJLIB.td003649_y1rc_trck_ip_1209600s_1
as
select
t1.tradnum,t2.TX_DESC,t2.ip
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
left join (
select ip,TX_DESC,TX_TIME,cust_id
from SASSJLIB.td003649_y1rc_trck_flw
where trim(TX_DESC) in (
'当前用户交易记录Excel生成跨中心编排'
,'查询转账手续费金额'
,'网上银行设备首次登录标志查询'
,'导航标签产品查询'
,'查询用户老带新活动奖励'
,'限额管理详情信息查询'
,'海外人才客户白名单标志查询'
,'总资产查询'
,'交易签名申请'
,'该微服务用于跨中心编排私行客户标识查询的管理。'
,'成长规则成长值发放跨中心编排'
,'客户贷款还款详情查询跨中心编排'
,'支持银行列表查询'
,'支付中心转账文档转换签章'
,'绑卡账户安全锁信息列表查询跨中心编排'
,'面核标志查询'
,'税融预申请查询'
,'客户贷款核心账单查询跨中心编排'
,'本微服务用于跨中心编排了贷款账户信息查询的操作'
,'贷款产品首页信息查询跨中心编排'
,'限额管理信息修改'
,'查询证件是否过期'
)
) t2
on t1.cust_id=t2.cust_id
and t1.trantime-cast(extract(epoch from to_timestamp(TO_CHAR(t2.TX_TIME),'yyyy-mm-dd hh24:mi:ss')) as int)>0
and t1.trantime-cast(extract(epoch from to_timestamp(TO_CHAR(t2.TX_TIME),'yyyy-mm-dd hh24:mi:ss')) as int)<=1209600
group by t1.tradnum,t2.TX_DESC,t2.ip
;
1051
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_1209600s;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_1209600s;
create table SASSJLIB.td003649_y1rc_trck_ip_1209600s
as
select
t1.tradnum as tradnum_ip_1209600s
#NAME?
,sum(case when TX_DESC='查询用户老带新活动奖励' then 1 end) as trck_pg10038_ip_cnt_1209600s
#NAME?
,sum(case when TX_DESC='限额管理详情信息查询' then 1 end) as trck_pg10089_ip_cnt_1209600s
#NAME?
,sum(case when TX_DESC='面核标志查询' then 1 end) as trck_pg10098_ip_cnt_1209600s
#NAME?
,sum(case when TX_DESC='总资产查询' then 1 end) as trck_pg10021_ip_cnt_1209600s
from SASSJLIB.td003649_y1rc_trck_ip_1209600s_1 t1
group by t1.tradnum
;

-- 行号: 1880 - 1934
-- 来源: 图片, (合并)
---------------------------259200S---------------------------
2259942
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_259200s_1;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_259200s_1;
create table SASSJLIB.td003649_y1rc_trck_ip_259200s_1
as
select
t1.tradnum,t2.TX_DESC,t2.ip
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
left join (
select ip,TX_DESC,TX_TIME,cust_id
from SASSJLIB.td003649_y1rc_trck_flw
where trim(TX_DESC) in (
'当前用户交易记录Excel生成跨中心编排'
,'查询转账手续费金额'
,'网上银行设备首次登录标志查询'
,'导航标签产品查询'
,'查询用户老带新活动奖励'
,'限额管理详情信息查询'
,'海外人才客户白名单标志查询'
,'总资产查询'
,'交易签名申请'
,'该微服务用于跨中心编排私行客户标识查询的管理。'
,'成长规则成长值发放跨中心编排'
,'客户贷款还款详情查询跨中心编排'
,'支持银行列表查询'
,'支付中心转账文档转换签章'
,'绑卡账户安全锁信息列表查询跨中心编排'
,'面核标志查询'
,'税融预申请查询'
,'客户贷款核心账单查询跨中心编排'
,'本微服务用于跨中心编排了贷款账户信息查询的操作'
,'贷款产品首页信息查询跨中心编排'
,'限额管理信息修改'
,'查询证件是否过期'
)
) t2
on t1.cust_id=t2.cust_id
and t1.trantime-cast(extract(epoch from to_timestamp(TO_CHAR(t2.TX_TIME),'yyyy-mm-dd hh24:mi:ss')) as int)>0
and t1.trantime-cast(extract(epoch from to_timestamp(TO_CHAR(t2.TX_TIME),'yyyy-mm-dd hh24:mi:ss')) as int)<=259200
group by t1.tradnum,t2.TX_DESC,t2.ip
;
1051
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_259200s;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_259200s;
create table SASSJLIB.td003649_y1rc_trck_ip_259200s
as
select
t1.tradnum as tradnum_ip_259200s
#NAME?
,sum(case when TX_DESC='查询用户老带新活动奖励' then 1 end) as trck_pg10038_ip_cnt_259200s
,sum(case when TX_DESC='总资产查询' then 1 end) as trck_pg10021_ip_cnt_259200s
,sum(case when TX_DESC='限额管理详情信息查询' then 1 end) as trck_pg10089_ip_cnt_259200s
from SASSJLIB.td003649_y1rc_trck_ip_259200s_1 t1
group by t1.tradnum
;

-- 行号: 1936 - 1999
-- 来源: 图片, (合并)
---------------------------ip---------------------------
---------------------------实时-------------------------
---------------------------7200S-------------------------
8059670
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_7200s_1;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_7200s_1;
create table SASSJLIB.td003649_y1rc_trck_ip_7200s_1
as
select
t1.tradnum,t2.TX_DESC,t2.ip
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
left join (
select ip,TX_DESC,TX_TIME,cust_id
from SASSJLIB.td003649_y1rc_trck_flw
where trim(TX_DESC) in (
'当前用户交易记录Excel生成跨中心编排'
,'查询转账手续费金额'
,'网上银行设备首次登录标志查询'
,'导航标签产品查询'
,'查询用户老带新活动奖励'
,'限额管理详情信息查询'
,'海外人才客户白名单标志查询'
,'总资产查询'
,'交易签名申请'
,'该微服务用于跨中心编排私行客户标识查询的管理。'
,'成长规则成长值发放跨中心编排'
,'客户贷款还款详情查询跨中心编排'
,'支持银行列表查询'
,'支付中心转账文档转换签章'
,'绑卡账户安全锁信息列表查询跨中心编排'
,'面核标志查询'
,'税融预申请查询'
,'客户贷款核心账单查询跨中心编排'
,'本微服务用于跨中心编排了贷款账户信息查询的操作'
,'贷款产品首页信息查询跨中心编排'
,'限额管理信息修改'
,'查询证件是否过期'
)
) t2
on t1.cust_id=t2.cust_id
and t1.trantime-cast(extract(epoch from to_timestamp(TO_CHAR(t2.TX_TIME),'yyyy-mm-dd hh24:mi:ss')) as int)>0
and t1.trantime-cast(extract(epoch from to_timestamp(TO_CHAR(t2.TX_TIME),'yyyy-mm-dd hh24:mi:ss')) as int)<=7200
group by t1.tradnum,t2.TX_DESC,t2.ip
;
1051
--select count(1) from SASSJLIB.td003649_y1rc_trck_ip_7200s;
drop table if exists SASSJLIB.td003649_y1rc_trck_ip_7200s;
create table SASSJLIB.td003649_y1rc_trck_ip_7200s
as
select
t1.tradnum as tradnum_ip_7200s
#NAME?
,sum(case when TX_DESC='查询用户老带新活动奖励' then 1 end) as trck_pg10038_ip_cnt_7200s
#NAME?
,sum(case when TX_DESC='限额管理详情信息查询' then 1 end) as trck_pg10089_ip_cnt_7200s
#NAME?
,sum(case when TX_DESC='面核标志查询' then 1 end) as trck_pg10098_ip_cnt_7200s
#NAME?
,sum(case when TX_DESC='总资产查询' then 1 end) as trck_pg10021_ip_cnt_7200s
from SASSJLIB.td003649_y1rc_trck_ip_7200s_1 t1
group by t1.tradnum
;

-- 行号: 2000 - 2014
-- 来源: 图片
--select count(1),count(distinct cardno_custinfo) from SASSJLIB.td003649_y1rc_acct_custinfo;
drop table if exists SASSJLIB.td003649_y1rc_acct_custinfo;
create table SASSJLIB.td003649_y1rc_acct_custinfo
as
select
t1.tradnum as tradnum_custinfo
,t1.cardno as cardno_custinfo
,t1.stm_dt as stm_dt_custinfo
,t1.cust_id as cust_id_custinfo
,t2.OPEN_DATE as open_date_acctinfo
,date(t1.stm_dt)-date(t2.BRTH_DT))/365) as age
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
left join MDMDALIB.IP t2 on t1.cust_id=t2.ENTP_IP_CODE
;
--par5
-- 行号: 2015 - 2029
-- 来源: 图片
--1051 31
--select count(1),count(distinct tradnum_acctinfo),count(distinct cardno_acctinfo) from SASSJLIB.td003649_y1rc_acct_acctinfo;
drop table if exists SASSJLIB.td003649_y1rc_acct_acctinfo;
create table SASSJLIB.td003649_y1rc_acct_acctinfo
as
select
t1.tradnum as tradnum_acctinfo
,t1.cardno as cardno_acctinfo
,t1.stm_dt as stm_dt_acctinfo
,t1.cust_id as cust_id_acctinfo
,t2.OPEN_DATE as open_date_acctinfo
,date(t1.stm_dt)-date(t2.OPEN_DATE) as open_days
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t1
left join FDMDALIB.F_AGT_CDEP_ACCT t2 on t1.cardno=t2.ACCOUNT_ID
;

-- 行号: 2030 - 2100
-- 来源: 图片,, (合并)
--select count(1) from SASSJLIB.td003649_y1rc_j;
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_j;
drop table if exists SASSJLIB.td003649_y1rc_j;
create table SASSJLIB.td003649_y1rc_j
as
select
t0.*
,case when trck_pg10089_ip_cnt_1209600s is not null then trck_pg10089_ip_cnt_1209600s else 0 end as trck_pg10089_ip_cnt_1209600s
,age
,txn_zhye_min_259200s
,case when trck_pg10098_ip_cnt_1209600s is not null then trck_pg10098_ip_cnt_1209600s else 0 end as trck_pg10098_ip_cnt_1209600s
,txn_c_62_ndfhh_cnt_180d
,txn_in_avg_259200s
,txn_bz80_amt_1296000s
,txn_strest_prop_1296000s
,open_days
,txn_cye_max_1296000s
,txn_zhye_std_259200s
,txn_dyeprop_avg_180d
,txn_repeatoppo_ratio_180d
,txn_ti_p01_180d
,txn_dye_avg_15d180d_gap
,txn_tranchan16_amt_1296000s
,txn_c_nabstr_txn_amt_1296000s
,txn_cyeprop_max_1296000s
,txn_zhye_avg_259200s
,txn_dye_avg_15d180d_gap -- 重复列，照抄
,txn_tranchan16_amt_1296000s -- 重复列，照抄
,txn_c_nabstr_txn_amt_1296000s -- 重复列，照抄
,txn_cyeprop_max_1296000s -- 重复列，照抄
,txn_zhye_avg_259200s -- 重复列，照抄
,txn_cyeprop95_ratio_15d180d_gap
,txn_call_nhh_txn_amt_180d
,txn_amt_1296000s
,txn_bz85_amt_15d180d_gap
,case when trck_pg10038_ip_cnt_1209600s is not null then trck_pg10038_ip_cnt_1209600s else 0 end as trck_pg10038_ip_cnt_1209600s
,case when trck_pg10046_ip_cnt_180d is not null then trck_pg10046_ip_cnt_180d else 0 end as trck_pg10046_ip_cnt_180d
,txn_dye_min_180d
,acct_kline_high_close_cnt_90d
,txn_min_amt_259200s
,txn_dyeprop05_ratio_180d
,txn_intb_cnt_10800s
,txn_dyeprop_min_ratio_15d180d
,acct_kline_high_close_ratio_90d
,txn_cd_amt_ratio_1296000s
,txn_min_amt_1296000s
,txn_intk_cnt_15d180d -- 疑似截断，图片显示为 txn_intk_cnt...
,txn_zhye_std_1296000s
,txn_c_nabstr_txn_amt_ratio_15d180d
,txn_cyeprop_std_1296000s
,acct_kline_macd_negative_ratio_90d
,case when trck_pg10012_ip_cnt_180d is not null then trck_pg10012_ip_cnt_180d else 0 end as trck_pg10012_ip_cnt_180d
,txn_dyeprop_std_1296000s
,amountamt
,txn_stwork_prop_1296000s
,txn_bz28_amt_15d180d_gap
,txn_tensones00_amt_10800s
,txn_stsleep_prop_1296000s
,txn_c_max_amt_259200s
,case when trck_pg10038_ip_cnt_259200s is not null then trck_pg10038_ip_cnt_259200s else 0 end as trck_pg10038_ip_cnt_259200s
,txn_adj_c_zhye_min_7200s
,txn_fnm_c_nwp_cnt_3d
,case when trck_pg10021_ip_cnt_1209600s is not null then trck_pg10021_ip_cnt_1209600s else 0 end as trck_pg10021_ip_cnt_1209600s
,txn_fnm_c_nwp_bank_dffloc_dfzhmc_cnt_7200s
,txn_fnm_c_nwp_bank_window_cnt_7200s
,txn_in_amt_259200s
,txn_out_amt_259200s
,txn_fnwp2_ratio_180d
,txn_c_nwp_dis_cnt_180d
,txn_bz65_zsh_cnt_180d
,trck_pg10089_ip_cnt_180d
,trck_pg10021_ip_cnt_180d
,txn_fnwp2_d_ratio_180d
,txn_d_nwp_dis_cnt_180d
,txn_ie_dfzh_dis_cnt_180d
,txn_dfdfk_cnt_180d
,txn_c_nwp_dffloc_dis_cnt_180d
,txn_zhdjk_cnt_180d

-- 行号: 2105 - 2167
-- 来源: 图片
,txn_atvd_cnt_180d
,txn_smlprob_bank_amt_259200s
,txn_atvd_cnt_15d180d_gap
,txn_zhye_min_360d
,txn_cnt_1296000s
,txn_zhye_min_1296000s
,txn_fnm_c_kh_cnt_7200s
,txn_st_d_ratio_180d
,txn_d_zfb_cd_nwp_dis_cnt_180d
,txn_bz65_c_ratio_180d
,txn_amt_max_180d
,txn_amt_c_max_180d
,txn_wg_d_ratio_180d
,txn_wg_c_ratio_180d
,txn_lingchen_cnt_180d
,txn_tx_xf_cnt_180d
,txn_loan_xf_cnt_180d
,txn_loan_xf_cnt_1296000s
,trck_pg10089_ip_cnt_7200s
,txn_qx_d_180d
,txn_qx_d_180d -- 重复
,txn_qx_d_cnt_180d
,txn_fnm_c_nwp_bank_dffloc_cnt_3d
,txn_fund_cnt_1296000s
,txn_fund_amt_1296000s
,txn_fnm_c_nwp_bank_dffloc_cnt_3d -- 重复
,txn_fnm_c_nwp_bank_dffloc_pro_cnt_3d
,txn_fund_cnt_3d
,txn_fund_amt_3d
,txn_bz65_cnt_1296000s
,txn_bz65_zsh_cnt_1296000s
,txn_bz65_c_ratio_1296000s
,txn_bz65_zsh_amt_1296000s
,txn_bz65_cnt_3d
,txn_bz65_zsh_cnt_3d
,txn_bz65_c_ratio_3d
,txn_bz65_zsh_amt_3d
,trck_pg10021_ip_cnt_259200s
,trck_pg10089_ip_cnt_259200s
,txn_sttime_prop_1296000s
,txn_smlprob_bank_amt_1296000s
,acct_equalamt_cnt_15d
,txn_c_cnt_15d
,txn_bz65_cnt_7200s
,txn_cye_min_1296000s
,txn_c_ind_amt_ratio_1296000s
,txn_tot_amt_1296000s
,acct_equalamt_cnt_15d -- 重复
,txn_c_cnt_15d -- 重复
,txn_cye_min_1296000s -- 重复
,txn_c_ind_amt_ratio_1296000s -- 重复
,txn_fnm_c_nwp_dfzhmc_cnt_1800s
,txn_fnm_dfzhmc_cnt_1296000s
,txn_fnm_dfzhmc_dffloc_cnt_1296000s
,txn_bz65_cnt_180d
,txn_bz65_cc_ratio_180d
,txn_bzdy_cnt_180d
,txn_bzdy_c_ratio_180d
,txn_bz65dy_cnt_180d
,txn_bz65dy_c_ratio_180d
from SASSJLIB.td003649_y1rc_rt_txn0_trainwb_label_gs t0
-- 行号: 2167 - 2212
-- 来源: 图片
inner join SASSJLIB.td003649_y1rc_txn_bank_cnt_3d t16 on t0.tradnum=t16.tradnum_txn_3d
-- 0
inner join SASSJLIB.td003649_y1rc_txn_bank_cnt_7200s t15 on t0.tradnum=t15.tradnum_txn_7200s
-- 0
inner join SASSJLIB.td003649_y1rc_trck_ip_180d t5 on t0.cardno=t5.cardno_txn_track_ip_180d and date(t0.stm_dt)-date(t5.stm_dt_txn_track_ip_180d)=0
-- 0
inner join SASSJLIB.td003649_y1rc_trck_ip_1209600s t4 on t0.tradnum=t4.tradnum_ip_1209600s
-- 0
inner join SASSJLIB.td003649_y1rc_trck_ip_259200s t3 on t0.tradnum=t3.tradnum_ip_259200s
-- 0
inner join SASSJLIB.td003649_y1rc_acct_acctinfo t1 on t0.tradnum=t1.tradnum_acctinfo
-- 0
inner join SASSJLIB.td003649_y1rc_acct_custinfo t2 on t0.tradnum=t2.tradnum_custinfo
-- 0
inner join SASSJLIB.td003649_y1rc_acct_kline_trainwb_gs_90d t6 on t0.cardno=t6.cardno_kline and date(t0.stm_dt)-date(t6.stm_dt_kline)=0
-- 0
inner join SASSJLIB.td003649_y1rc_txn_ti_trainwb_gs_180d t7 on t0.cardno=t7.cardno_ti and date(t0.stm_dt)-date(t7.stm_dt_ti)=0
-- 0
inner join SASSJLIB.td003649_y1rc_txn_frqoppo_ind_trainwb_gs_180d t8 on t0.cardno=t8.cardno_frqoppo and date(t0.stm_dt)-date(t8.stm_dt_frqoppo)=0
-- 0
inner join SASSJLIB.td003649_y1rc_txn_base_10800s_par1 t12 on t0.tradnum=t12.tradnum_txn_base_10800s_par1
-- 0
inner join SASSJLIB.td003649_y1rc_txn_base_1296000s_par1 t14 on t0.tradnum=t14.tradnum_txn_base_1296000s_par1
-- 0
inner join SASSJLIB.td003649_y1rc_txn_base_360d_par1 t11 on t0.cardno=t11.cardno_txn_base_360d_par1 and date(t0.stm_dt)-date(t11.stm_dt_txn_base_360d_par1)=0
-- 0
inner join SASSJLIB.td003649_y1rc_txn_base_15d180d_gap_par1 t10 on t0.cardno=t10.cardno_txn_base_15d180d_gap_par1 and date(t0.stm_dt)-date(t10.stm_dt_txn_base_15d180d_gap_par1)=0
-- 0
inner join SASSJLIB.td003649_y1rc_txn_base_259200s_par1 t13 on t0.tradnum=t13.tradnum_txn_base_259200s_par1
-- 0
inner join SASSJLIB.td003649_y1rc_txn_base_ratio_15d180d t9 on t0.tradnum=t9.tradnum_ratio_15d180d
-- 0
inner join SASSJLIB.td003649_y1rc_trck_ip_7200s t30 on t0.tradnum=t30.tradnum_ip_7200s
-- 0
inner join SASSJLIB.td003649_y1rc_txn_equalamt_trainwb_gs_180d t50 on t0.cardno=t50.cardno_equalamt and date(t0.stm_dt)-date(t50.stm_dt_equalamt)=0
-- 0
inner join SASSJLIB.td003649_y1rc_txn_bank_cnt_1800s t53 on t0.tradnum=t53.tradnum_txn_1800s
;

-- 行号: 2213 - 2230
-- 来源: 图片
-------------------------r5---------------------------
-- 15040 9678
-- 149 104
drop table if exists SASSJLIB.td003649_y1rc_r5;
create table SASSJLIB.td003649_y1rc_r5
as
select
*
,1 as dk
,'r5' as ruleid
from SASSJLIB.td003649_y1rc_j
where (abstr not like '%冲销%' or abstr is null)
and dcflag=0
and lag_dcflag=1
--#NAME?
and txn_adj_c_zhye_min_7200s<50000
--#NAME?
and txn_fnm_c_nwp_bank_dffloc_dfzhmc_cnt_7200s>0
;
-------------------------r1---------------------------
-- 42397 14744

-- 行号: 2232 - 2254
-- 来源: 图片,
-- 345 148
drop table if exists SASSJLIB.td003649_y1rc_r1;
create table SASSJLIB.td003649_y1rc_r1
as
select
*
,1 as dk
,'r1' as ruleid
from SASSJLIB.td003649_y1rc_j
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and (
nvl(trck_pg10089_ip_cnt_1209600s,0)>1
)
;

-- 行号: 2256 - 2280
-- 来源: 图片,
-------------------------r2---------------------------
-- 42397 14744
-- 345 148
drop table if exists SASSJLIB.td003649_y1rc_r2;
create table SASSJLIB.td003649_y1rc_r2
as
select
*
,1 as dk
,'r2' as ruleid
from SASSJLIB.td003649_y1rc_j
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and (
(txn_fnm_c_nwp_cnt_3d>0 and bz like '%支付宝%' and nvl(trck_pg10021_ip_cnt_1209600s,0)+nvl(trck_pg10089_ip_cnt_1209600s,0)>0)
)
;

-- 行号: 2281 - 2306
-- 来源: 图片,,
-------------------------r3---------------------------
-- 42397 14744
-- 345 148
drop table if exists SASSJLIB.td003649_y1rc_r3;
create table SASSJLIB.td003649_y1rc_r3
as
select
*
,1 as dk
,'r3' as ruleid
from SASSJLIB.td003649_y1rc_j
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and (
(province<>'广东' and nvl(trck_pg10021_ip_cnt_1209600s,0)+nvl(trck_pg10089_ip_cnt_1209600s,0)>0)
)
;

-- 行号: 2307 - 2330
-- 来源: 图片
-------------------------r4---------------------------
-- 42397 14744
-- 345 148
drop table if exists SASSJLIB.td003649_y1rc_r4;
create table SASSJLIB.td003649_y1rc_r4
as
select
*
,1 as dk
,'r4' as ruleid
from SASSJLIB.td003649_y1rc_j
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and (
bz like '%支付宝%' and substr(dfzh,5,3)='052'
)
;

-- 行号: 2332 - 2351
-- 来源: 图片
-------------------------r6---------------------------
-- 284 270
-- 8 6
drop table if exists SASSJLIB.td003649_y1rc_r6;
create table SASSJLIB.td003649_y1rc_r6
as
select
*
,1 as dk
,'r6' as ruleid
from SASSJLIB.td003649_y1rc_j
where (abstr not like '%冲销%' or abstr is null)
and dcflag=0
and lag_dcflag=1
and txn_adj_c_zhye_min_7200s<50000
and txn_fnm_c_nwp_bank_window_cnt_7200s>0
and abstr='现金'
and amountamt>=10000
and zhye<1500
and nvl(trck_pg10021_ip_cnt_1209600s,0)+nvl(trck_pg10089_ip_cnt_1209600s,0)>0
;

-- 行号: 2353 - 2373
-- 来源: 图片,
-------------------------r7---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r7;
drop table if exists SASSJLIB.td003649_y1rc_r7;
create table SASSJLIB.td003649_y1rc_r7
as
select
*
,1 as dk
,'r7' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where (abstr not like '%冲销%' or abstr is null)
and dcflag=0
and lag_dcflag=1
and txn_adj_c_zhye_min_1800s<50000
and txn_fnm_c_nwp_bank_dffloc_dfzhmc_cnt_1800s>0
and trantime-lag_trantime<1800
and nvl(trck_pg10089_ip_cnt_1209600s,0)>0
and lag_amountamt>=1000
and bz not like '%贷款提前归还%'
and bz not like '%还款%'
and nvl(txn_bz65_amt_1296000s,0)>0
;

-- 行号: 2375 - 2394
-- 来源: 图片
-------------------------r8---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r8;
drop table if exists SASSJLIB.td003649_y1rc_r8;
create table SASSJLIB.td003649_y1rc_r8
as
select
*
,1 as dk
,'r8' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where (abstr not like '%冲销%' or abstr is null)
and dcflag=1
and zhye-amountamt<50000
and fnwp2=1
and f_diff_name=1
and nvl(txn_fnwp2_ratio_180d,0)>0.33
and nvl(txn_c_nwp_dis_cnt_180d,0)>18
and nvl(txn_bz65_zsh_cnt_180d,0)>3
and nvl(trck_pg10089_ip_cnt_180d,0)>0
and nvl(trck_pg10021_ip_cnt_180d,0)>30
;

-- 行号: 2396 - 2418
-- 来源: 图片,
-------------------------r9---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r9;
drop table if exists SASSJLIB.td003649_y1rc_r9;
create table SASSJLIB.td003649_y1rc_r9
as
select
*
,1 as dk
,'r9' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where (abstr not like '%冲销%' or abstr is null)
and dcflag=0
and lag_dcflag=1
and txn_adj_c_zhye_min_7200s<50000
and txn_fnm_c_nwp_bank_dffloc_dfzhmc_cnt_7200s>0
and trantime-lag_trantime<1800
and lag_amountamt>=1000
and bz not like '%贷款提前归还%'
and bz not like '%还款%'
and nvl(txn_bz65_amt_1296000s,0)>0
and nvl(trck_pg10089_ip_cnt_180d,0)>1
and mod(round(amountamt,-2),5000)=0
and round(lag_amountamt,-2)=round(amountamt,-2)
;

-- 行号: 2420 - 2439
-- 来源: 图片
-------------------------r10---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r10;
drop table if exists SASSJLIB.td003649_y1rc_r10;
create table SASSJLIB.td003649_y1rc_r10
as
select
*
,1 as dk
,'r10' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where (abstr not like '%冲销%' or abstr is null)
and dcflag=1
and zhye-amountamt<50000
and fnwp2=1
and f_diff_name=1
and nvl(txn_fnwp2_d_ratio_180d,0)>0.7
and nvl(txn_d_nwp_dis_cnt_180d,0)>20
and txn_bz65_amt_1296000s>0
and province<>'广东'
and zhye-amountamt<1500
;

-- 行号: 2441 - 2461
-- 来源: 图片,
-------------------------r11---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r11;
drop table if exists SASSJLIB.td003649_y1rc_r11;
create table SASSJLIB.td003649_y1rc_r11
as
select
*
,1 as dk
,'r11' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where (abstr not like '%冲销%' or abstr is null)
and dcflag=1
and zhye-amountamt<50000
and fnwp2=1
and f_diff_name=1
and province<>'广东'
and trck_pg10089_ip_cnt_180d>0
and txn_smlprob_bank_amt_259200s>0
and txn_atvd_cnt_15d180d_gap=0
;

-- 行号: 2462 - 2478
-- 来源: 图片
-------------------------r12---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r12;
drop table if exists SASSJLIB.td003649_y1rc_r12;
create table SASSJLIB.td003649_y1rc_r12
as
select
*
,1 as dk
,'r12' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where (abstr not like '%冲销%' or abstr is null)
and dcflag=0
and lag_dcflag=1
and txn_adj_c_zhye_min_7200s<50000
and txn_fnm_c_nwp_bank_dffloc_dfzhmc_cnt_7200s>0
and abstr like '%转贷记卡%'
and txn_dfdfk_cnt_180d>0
;
-- 行号: 2480 - 2505
-- 来源: 图片,
-------------------------r13---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r13;
drop table if exists SASSJLIB.td003649_y1rc_r13;
create table SASSJLIB.td003649_y1rc_r13
as
select
*
,1 as dk
,'r13' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where (abstr not like '%冲销%' or abstr is null)
and (abstr like '%现金%' or abstr like '%取现%')
and dcflag=0
and amountamt>=1000
and (lag_in_dfhmc not like '%顺德%' or lag_in_dfhmc is null)
and (lag_in_dfhmc not like '%佛山%' or lag_in_dfhmc is null)
and (lag_in_city<>'佛山' or lag_in_city is null)
and lag_amountamt>=10000
and lag_dcflag=1
and txn_adj_c_zhye_min_7200s<300
and txn_fnm_c_kh_cnt_7200s>0
and txn_cnt_1296000s<5
and txn_atvd_cnt_180d<5
and trck_pg10021_ip_cnt_180d>0
;
--par6
-------------------------r14---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r14;
drop table if exists SASSJLIB.td003649_y1rc_r14;
create table SASSJLIB.td003649_y1rc_r14
as
select
*
,1 as dk
,'r14' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (dfhmc not like '%顺德%' or dfhmc is null)
and (dfhmc not like '%佛山%' or dfhmc is null)
and (city<>'佛山' or city is null)
and ((substr(to_char(dfzh),1,2)='62' and (length(dfzh)=16 or length(dfzh)=19)))
and trck_pg10021_ip_cnt_180d>30
and txn_st_d_ratio_180d>0.2
and txn_bz65_zsh_cnt_180d>=12
and txn_fnwp2_d_ratio_180d>0.8
;

-- 行号: 2530 - 2549
-- 来源: 图片
-------------------------r15---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r15;
drop table if exists SASSJLIB.td003649_y1rc_r15;
create table SASSJLIB.td003649_y1rc_r15
as
select
*
,1 as dk
,'r15' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and province<>'广东'
and f_diff_name=1
and txn_c_nwp_dis_cnt_180d=0
and mod(round(amountamt,-3),5000)=0
and round(amountamt,-3)-amountamt>=0
and zhye-amountamt<300
and nvl(trck_pg10089_ip_cnt_180d,0)+nvl(trck_pg10089_ip_cnt_1209600s,0)>0
;

-- 行号: 2551 - 2577
-- 来源: 图片
-------------------------r16---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r16;
drop table if exists SASSJLIB.td003649_y1rc_r16;
create table SASSJLIB.td003649_y1rc_r16
as
select
*
,1 as dk
,'r16' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and province<>'广东'
and f_diff_name=1
and txn_wg_d_ratio_180d>0.50
and txn_wg_c_ratio_180d<0.33
and nvl(trck_pg10021_ip_cnt_1209600s,0)>0
and mod(round(amountamt,-3),5000)=0
and round(amountamt,-3)-amountamt>=0
and zhye-amountamt<300
;

-- 行号: 2579 - 2604
-- 来源: 图片
-------------------------r17---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r17;
drop table if exists SASSJLIB.td003649_y1rc_r17;
create table SASSJLIB.td003649_y1rc_r17
as
select
*
,1 as dk
,'r17' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and province<>'广东'
and f_diff_name=1
and txn_wg_d_ratio_180d>0.50
and txn_wg_c_ratio_180d>0
and nvl(trck_pg10021_ip_cnt_1209600s,0)>0
and mod(round(amountamt,-3),5000)=0
and round(amountamt,-3)-amountamt>=0
and zhye-amountamt<300
and txn_lingchen_cnt_180d>0
and nvl(trck_pg10089_ip_cnt_1209600s,0)>0
;

-- 行号: 2606 - 2626
-- 来源: 图片
-------------------------r18---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r18;
drop table if exists SASSJLIB.td003649_y1rc_r18;
create table SASSJLIB.td003649_y1rc_r18
as
select
*
,1 as dk
,'r18' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and province<>'广东'
and f_diff_name=1
--and txn_wg_d_ratio_180d>0.50
and txn_wg_c_ratio_180d>0
and nvl(trck_pg10021_ip_cnt_1209600s,0)>0
and mod(round(amountamt,-3),5000)=0
and round(amountamt,-3)-amountamt>=0
and zhye-amountamt<300
and txn_lingchen_cnt_180d>0
and nvl(trck_pg10089_ip_cnt_1209600s,0)>0
;

-- 行号: 2628 - 2654
-- 来源: 图片
-------------------------r19---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r19;
drop table if exists SASSJLIB.td003649_y1rc_r19;
create table SASSJLIB.td003649_y1rc_r19
as
select
*
,1 as dk
,'r19' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and province<>'广东'
and f_diff_name=1
--and txn_wg_d_ratio_180d>0.50
and txn_wg_c_ratio_180d>0
and nvl(trck_pg10021_ip_cnt_1209600s,0)>0
and mod(round(amountamt,-3),5000)=0
and round(amountamt,-3)-amountamt>=0
and zhye-amountamt<300
and txn_lingchen_cnt_180d>0
and nvl(trck_pg10089_ip_cnt_1209600s,0)>0
;

-- 行号: 2656 - 2680
-- 来源: 图片
-------------------------r20---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r20;
drop table if exists SASSJLIB.td003649_y1rc_r20;
create table SASSJLIB.td003649_y1rc_r20
as
select
*
,1 as dk
,'r20' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and province<>'广东'
and txn_wg_d_ratio_180d>0.7
and txn_wg_c_ratio_180d>0.03
and txn_tx_xf_cnt_1296000s>0
and txn_bz65_c_ratio_180d>0.15
and txn_atvd_cnt_180d>70
and trck_pg10012_ip_cnt_180d=0
;

-- 行号: 2682 - 2703
-- 来源: 图片
-------------------------r21---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r21;
drop table if exists SASSJLIB.td003649_y1rc_r21;
create table SASSJLIB.td003649_y1rc_r21
as
select
*
,1 as dk
,'r21' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and (
nvl(trck_pg10089_ip_cnt_7200s,0)>0
)
and (province<>'广东' or province is null)
and dfhmc not like '%广东%'
and dfhmc not like '%顺德%'
and dfhmc not like '%佛山%'
and dfhmc not like '%江门%'
and dfhmc not like '%东莞%'
and dfhmc not like '%深圳%'
and dfhmc not like '%广州%'
and dfhmc not like '%肇庆%'
and dfhmc not like '%珠海%'
and dfhmc not like '%中山%'
and dfhmc not like '%湛江%'
and dfhmc not like '%云浮%'
and dfhmc not like '%惠州%'
and bz not like '%委托实时贷记%'
;

-- 行号: 2705 - 2732
-- 来源: 图片
-------------------------r22---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r22;
drop table if exists SASSJLIB.td003649_y1rc_r22;
create table SASSJLIB.td003649_y1rc_r22
as
select
*
,1 as dk
,'r22' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<5000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
or
((substr(to_char(dfzh),1,2)='62' and (length(dfzh)=16 or length(dfzh)=19)))
)
and province<>'广东'
and txn_qx_d_180d>10000
and txn_qx_d_cnt_180d>1
and txn_qx_d_ratio_180d>0.3
and amountamt>1000
;

-- 行号: 2734 - 2759
-- 来源: 图片
-------------------------r23---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r23;
drop table if exists SASSJLIB.td003649_y1rc_r23;
create table SASSJLIB.td003649_y1rc_r23
as
select
*
,1 as dk
,'r23' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<1500
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and province<>'广东'
and nvl(trck_pg10089_ip_cnt_1209600s,0)+nvl(trck_pg10089_ip_cnt_180d,0)>0
and amountamt>=1000
and txn_smlprob_bank_amt_259200s>0
;

-- 行号: 2761 - 2784
-- 来源: 图片
-------------------------r24---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r24;
drop table if exists SASSJLIB.td003649_y1rc_r24;
create table SASSJLIB.td003649_y1rc_r24
as
select
*
,1 as dk
,'r24' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<5000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%' or bz like '%普通贷记来帐%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and txn_fnm_c_nwp_dfzhmc_cnt_1800s>0
and txn_bz65_zsh_cnt_1296000s>0
and acct_equalamt_cnt_15d>3
and bz like '%跨行网银贷记%'
;

-- 行号: 2786 - 2808
-- 来源: 图片
-------------------------r25---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r25;
drop table if exists SASSJLIB.td003649_y1rc_r25;
create table SASSJLIB.td003649_y1rc_r25
as
select
*
,1 as dk
,'r25' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and bz like '%跨行网银贷记%'
and txn_qx_d_180d>=100000
and txn_qx_d_cnt_180d>=5
and txn_c_ind_amt_ratio_1296000s>0.99
and txn_cd_amt_ratio_1296000s>=0.9
and txn_cd_amt_ratio_1296000s<=1.1
;

-- 行号: 2810 - 2829
-- 来源: 图片
-------------------------r26---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r26;
drop table if exists SASSJLIB.td003649_y1rc_r26;
create table SASSJLIB.td003649_y1rc_r26
as
select
*
,1 as dk
,'r26' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and province<>'广东'
and txn_c_cnt_15d=acct_equalamt_cnt_15d
and txn_c_cnt_15d>1
and txn_fnwp2_ratio_180d>=0.66
and txn_sttime_prop_1296000s>=0.34
;

-- 行号: 2831 - 2847
-- 来源: 图片
-------------------------r27---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r27;
drop table if exists SASSJLIB.td003649_y1rc_r27;
create table SASSJLIB.td003649_y1rc_r27
as
select
*
,1 as dk
,'r27' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and txn_d_zfb_cd_nwp_dis_cnt_180d>30
and txn_fnm_c_nwp_dfzhmc_cnt_1800s>=2
;

-- 行号: 2849 - 2866
-- 来源: 图片
-------------------------r28---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r28;
drop table if exists SASSJLIB.td003649_y1rc_r28;
create table SASSJLIB.td003649_y1rc_r28
as
select
*
,1 as dk
,'r28' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and province<>'广东'
and txn_smlprob_bank_amt_1296000s>1
and txn_tx_xf_cnt_180d>0
;

-- 行号: 2868 - 2886
-- 来源: 图片
-------------------------r29---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r29;
drop table if exists SASSJLIB.td003649_y1rc_r29;
create table SASSJLIB.td003649_y1rc_r29
as
select
*
,1 as dk
,'r29' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and province<>'广东'
and txn_cnt_1296000s<=10
and txn_fund_cnt_3d>1
;

-- 行号: 2887 - 2908
-- 来源: 图片
-------------------------r30---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r30;
drop table if exists SASSJLIB.td003649_y1rc_r30;
create table SASSJLIB.td003649_y1rc_r30
as
select
*
,1 as dk
,'r30' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where lag_in_dcflag=1
and lag_in_zhye-lag_in_amountamt<50000
and lag_in_fnwp2=1
and lag_in_f_diff_name=1
and lag_in_bz like '%跨行网银贷记%'
and dcflag=0
and trantime-lag_in_trantime>0
and trantime-lag_in_trantime<=7200
and txn_bz65dy_cnt_180d>1
and age>45
;

-- 行号: 2910 - 2927
-- 来源: 图片
-------------------------r31---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r31;
drop table if exists SASSJLIB.td003649_y1rc_r31;
create table SASSJLIB.td003649_y1rc_r31
as
select
*
,1 as dk
,'r31' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and province<>'广东'
and nvl(trck_pg10089_ip_cnt_7200s,0)>0
and nvl(trck_pg10089_ip_cnt_1209600s,0)>1
;

-- 行号: 2929 - 2944
-- 来源: 图片
-------------------------r32---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r32;
drop table if exists SASSJLIB.td003649_y1rc_r32;
create table SASSJLIB.td003649_y1rc_r32
as
select
*
,1 as dk
,'r32' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=0
and lag_dcflag=1
and txn_adj_c_zhye_min_7200s<50000
and txn_fnm_c_nwp_bank_dffloc_dfzhmc_cnt_7200s>0
and trck_pg10089_ip_cnt_1209600s>0
and age>65
;

-- 行号: 2946 - 2961
-- 来源: 图片
-------------------------r33---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r33;
drop table if exists SASSJLIB.td003649_y1rc_r33;
create table SASSJLIB.td003649_y1rc_r33
as
select
*
,1 as dk
,'r33' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=0
and lag_dcflag=1
and txn_adj_c_zhye_min_7200s<50000
and txn_fnm_c_nwp_bank_dffloc_dfzhmc_cnt_7200s>0
and trck_pg10089_ip_cnt_259200s>0
and trck_pg10089_ip_cnt_1209600s>1
;
-- 行号: 2963 - 2987
-- 来源: 图片
-------------------------r34---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r34;
drop table if exists SASSJLIB.td003649_y1rc_r34;
create table SASSJLIB.td003649_y1rc_r34
as
select
*
,1 as dk
,'r34' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where nvl(trck_pg10021_ip_cnt_1209600s,0)=0
and nvl(trck_pg10089_ip_cnt_1209600s,0)=0
and dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and f_diff_name=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and province<>'广东'
and open_days<1095
and age<26
and txn_fund_cnt_1296000s>=1
;

-- 行号: 2990 - 3014
-- 来源: 图片
-------------------------r35---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r35;
drop table if exists SASSJLIB.td003649_y1rc_r35;
create table SASSJLIB.td003649_y1rc_r35
as
select
*
,1 as dk
,'r35' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where nvl(trck_pg10021_ip_cnt_1209600s,0)=0
and nvl(trck_pg10089_ip_cnt_1209600s,0)=0
and dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and province<>'广东'
and open_days<1095
and age<26
and txn_fnm_c_nwp_bank_dffloc_cnt_3d>0
;
-------------------------r36---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r36;
drop table if exists SASSJLIB.td003649_y1rc_r36;
create table SASSJLIB.td003649_y1rc_r36
as
select
*
,1 as dk
,'r36' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and province<>'广东'
and txn_tx_xf_cnt_1296000s>3
and trck_pg10038_ip_cnt_1209600s>1
;
-------------------------r37---------------------------
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_r37;
drop table if exists SASSJLIB.td003649_y1rc_r37;
create table SASSJLIB.td003649_y1rc_r37
as
select
*
,1 as dk
,'r37' as ruleid
from SASSJLIB.td003649_y1rc_j t1
where dcflag=1
and zhye-amountamt<50000
and (abstr not like '%冲销%' or abstr is null)
and fnwp2=1
and (
(bz like '%跨行网银贷记%' or bz like '%转账%' or bz like '%数字人民币兑回%' or bz like '%往来款%' or bz like '%委托实时贷记%')
or
((substr(to_char(dfzh),1,2)='62' and bz not like '%普通贷记来帐%' and (length(dfzh)=16 or length(dfzh)=19)))
)
and province<>'广东'
and age<30
and trck_pg10089_ip_cnt_1209600s>0
and txn_fnwp2_d_ratio_180d>0.8
;
-- 行号: 3018 - 3107
-- 来源: 图片 -
-- 215 215 151
--select count(1),count(distinct tradnum),count(distinct cardno) from SASSJLIB.td003649_y1rc_chf1104 group by ruleid;
--select count(1),count(distinct cardno) from SASSJLIB.td003649_y1rc_chf1104;
drop table if exists SASSJLIB.td003649_y1rc_chf1104;
create table SASSJLIB.td003649_y1rc_chf1104
as
with tmp1 as (
select * from SASSJLIB.td003649_y1rc_r1
union
select * from SASSJLIB.td003649_y1rc_r2
union
select * from SASSJLIB.td003649_y1rc_r3
union
select * from SASSJLIB.td003649_y1rc_r4
union
select * from SASSJLIB.td003649_y1rc_r5
union
select * from SASSJLIB.td003649_y1rc_r6
union
select * from SASSJLIB.td003649_y1rc_r7
union
select * from SASSJLIB.td003649_y1rc_r8
union
select * from SASSJLIB.td003649_y1rc_r9
union
select * from SASSJLIB.td003649_y1rc_r10
union
select * from SASSJLIB.td003649_y1rc_r11
union
select * from SASSJLIB.td003649_y1rc_r12
union
select * from SASSJLIB.td003649_y1rc_r13
union
select * from SASSJLIB.td003649_y1rc_r14
union
select * from SASSJLIB.td003649_y1rc_r15
union
select * from SASSJLIB.td003649_y1rc_r16
union
select * from SASSJLIB.td003649_y1rc_r17
union
select * from SASSJLIB.td003649_y1rc_r18
union
select * from SASSJLIB.td003649_y1rc_r19
union
select * from SASSJLIB.td003649_y1rc_r20
union
select * from SASSJLIB.td003649_y1rc_r21
union
select * from SASSJLIB.td003649_y1rc_r22
union
select * from SASSJLIB.td003649_y1rc_r23
union
select * from SASSJLIB.td003649_y1rc_r24
union
select * from SASSJLIB.td003649_y1rc_r25
union
select * from SASSJLIB.td003649_y1rc_r26
union
select * from SASSJLIB.td003649_y1rc_r27
union
select * from SASSJLIB.td003649_y1rc_r28
union
select * from SASSJLIB.td003649_y1rc_r29
union
select * from SASSJLIB.td003649_y1rc_r30
union
select * from SASSJLIB.td003649_y1rc_r31
union
select * from SASSJLIB.td003649_y1rc_r32
union
select * from SASSJLIB.td003649_y1rc_r33
union
select * from SASSJLIB.td003649_y1rc_r34
union
select * from SASSJLIB.td003649_y1rc_r35
union
select * from SASSJLIB.td003649_y1rc_r36
union
select * from SASSJLIB.td003649_y1rc_r37
),tmp2 as (
select
distinct p.cust_id
from odmdalib.o_nehr_empbase_info t
inner join fdmdalib.f_pty_cust_cert p
on p.cert_no = t.cardno
and t.ods_data_date = to_char(current_date-2,'yyyymmdd')
and t.posistatus<>'012'
)
select
t1.*
from tmp1 t1
left join tmp2 t2 on t1.cust_id=t2.cust_id
where t2.cust_id is null
;