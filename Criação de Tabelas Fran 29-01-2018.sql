Receber o arquivo: cmr_base_04_mm_final.txt

Fazer upload do arquivo para o Juypter Notebook, depois que fazer o upload, executar esse comando no HDFS:

#criar pasta
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/CMR_FRAN/cmr_base_04_mm_final
#Copia do jupyter para o hdfs
hdfs dfs -copyFromLocal /home/fgastardi/cmr_base_04_mm_final.txt /user/hive/warehouse/dcd.db/CMR_FRAN/cmr_base_04_mm_final/cmr_base_04_mm_final.txt


create external table cmr_base_04_mm_final
(PROFISSAO_CADU string, SCORE_ACSP_19 string, QT_RTRIT_HIST_PRIM string, FLAG_APOSENTADO string, RENDA_FINAL string, QT_RTRIT_HIST string, SCORE_5PF string, IN_CARTAO_CREDITO string, IN_CHEQUE_ESPECIAL string, IN_CREDITO_PESSOAL string, IN_FINANC_CARTAO_CREDITO string, PC_UTLZ_CARTAO_12M_EXP string, PC_UTLZ_CARTAO_36M string, PC_UTLZ_CARTAO_MES string, PC_UTLZ_CHQ_LIM_GLOB_12M_EXP string, PC_UTLZ_CHQ_LIM_GLOB_36M string, PC_UTLZ_CHQ_LIM_GLOB_MES string, QT_DEFAULT_36M string, QT_DT_BASE_DEF_36M string, QT_DT_BASE_INICIO_36M string, QT_MESES_POSSUI_CARTAO_12M string, QT_MESES_POSSUI_CARTAO_36M string, QT_MESES_POSSUI_CHQ_LIM_GLOB_12M string, QT_MESES_POSSUI_CHQ_LIM_GLOB_36M string, QT_UTLZ_CARTAO_12M string, QT_UTLZ_CARTAO_MES string, QT_UTLZ_CHQ_LIM_GLOB_36M string, DT_ULTIMO_REST_12MESES string, QT_MESES_REST_12M string, QT_PRINC_RTRIT_HIST_PRIM string, CD_POSSUI_RESTR_HISTORICO string, POSSUI_PARCELADO string, PC_A_UTILIZAR_COMPR_CHQ_LIM_GLOB string, PC_A_UTILIZAR_COMPR_CARTAO string, PC_A_UTILIZAR_COMPR_CTAGARANT string, BACEN_IMO string, IN_CDCVEIC_LEASING string, IN_FINANC_IMOBILIARIO string, QT_UTLZ_CHQ_LIM_GLOB_MES string, QT_UTLZ_CHQ_LIM_GLOB_12M string, PC_UTLZ_CTAGARANTIDA_12M_EXP string, QT_MESES_POSSUI_CTAGARANTIDA_12M string, VR_SLD_MEDIO_SEMEST_00070_SM string, DT_CONTA string, CPF11_MASC string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/CMR_FRAN/cmr_base_04_mm_final';

#eliminar a primeira linha da tabela 
create table cmr_base_04_credito_final as
select cast(PROFISSAO_CADU as double) as PROFISSAO_CADU, cast(SCORE_ACSP_19 as double) as SCORE_ACSP_19, cast(QT_RTRIT_HIST_PRIM as int) as QT_RTRIT_HIST_PRIM, cast(FLAG_APOSENTADO as int) as FLAG_APOSENTADO, cast(RENDA_FINAL as double) as RENDA_FINAL, cast(QT_RTRIT_HIST as int) as QT_RTRIT_HIST, cast(SCORE_5PF as double) as SCORE_5PF, IN_CARTAO_CREDITO, IN_CHEQUE_ESPECIAL, IN_CREDITO_PESSOAL, IN_FINANC_CARTAO_CREDITO, cast(PC_UTLZ_CARTAO_12M_EXP as double) as PC_UTLZ_CARTAO_12M_EXP, cast(PC_UTLZ_CARTAO_36M as double) as PC_UTLZ_CARTAO_36M, cast(PC_UTLZ_CARTAO_MES as double) as PC_UTLZ_CARTAO_MES, cast(PC_UTLZ_CHQ_LIM_GLOB_12M_EXP as double) as PC_UTLZ_CHQ_LIM_GLOB_12M_EXP, cast(PC_UTLZ_CHQ_LIM_GLOB_36M as double) as PC_UTLZ_CHQ_LIM_GLOB_36M, cast(PC_UTLZ_CHQ_LIM_GLOB_MES as double) as PC_UTLZ_CHQ_LIM_GLOB_MES, cast(QT_DEFAULT_36M as int) as QT_DEFAULT_36M, cast(QT_DT_BASE_DEF_36M as double) as QT_DT_BASE_DEF_36M, cast(QT_DT_BASE_INICIO_36M as double) as QT_DT_BASE_INICIO_36M, cast(QT_MESES_POSSUI_CARTAO_12M as int) as QT_MESES_POSSUI_CARTAO_12M, cast(QT_MESES_POSSUI_CARTAO_36M as int) as QT_MESES_POSSUI_CARTAO_36M, cast(QT_MESES_POSSUI_CHQ_LIM_GLOB_12M as int) as QT_MESES_POSSUI_CHQ_LIM_GLOB_12M, cast(QT_MESES_POSSUI_CHQ_LIM_GLOB_36M as int) as QT_MESES_POSSUI_CHQ_LIM_GLOB_36M, cast(QT_UTLZ_CARTAO_12M as int) as QT_UTLZ_CARTAO_12M, cast(QT_UTLZ_CARTAO_MES as int) as QT_UTLZ_CARTAO_MES, cast(QT_UTLZ_CHQ_LIM_GLOB_36M as int) as QT_UTLZ_CHQ_LIM_GLOB_36M, cast(DT_ULTIMO_REST_12MESES as double) as DT_ULTIMO_REST_12MESES, cast(QT_MESES_REST_12M as double) as QT_MESES_REST_12M, cast(QT_PRINC_RTRIT_HIST_PRIM as int) as QT_PRINC_RTRIT_HIST_PRIM, CD_POSSUI_RESTR_HISTORICO, POSSUI_PARCELADO, cast(PC_A_UTILIZAR_COMPR_CHQ_LIM_GLOB as double) as PC_A_UTILIZAR_COMPR_CHQ_LIM_GLOB, cast(PC_A_UTILIZAR_COMPR_CARTAO as double) as PC_A_UTILIZAR_COMPR_CARTAO, cast(PC_A_UTILIZAR_COMPR_CTAGARANT as double) as PC_A_UTILIZAR_COMPR_CTAGARANT, BACEN_IMO, IN_CDCVEIC_LEASING, IN_FINANC_IMOBILIARIO, cast(QT_UTLZ_CHQ_LIM_GLOB_MES as int) as QT_UTLZ_CHQ_LIM_GLOB_MES, cast(QT_UTLZ_CHQ_LIM_GLOB_12M as int) as QT_UTLZ_CHQ_LIM_GLOB_12M, cast(PC_UTLZ_CTAGARANTIDA_12M_EXP as int) as PC_UTLZ_CTAGARANTIDA_12M_EXP, cast(QT_MESES_POSSUI_CTAGARANTIDA_12M as int) as QT_MESES_POSSUI_CTAGARANTIDA_12M, cast(VR_SLD_MEDIO_SEMEST_00070_SM as double) as VR_SLD_MEDIO_SEMEST_00070_SM, cast(DT_CONTA as double) as DT_CONTA, CPF11_MASC as numero_cpf_cnpj
from cmr_base_04_mm_final where PROFISSAO_CADU !='PROFISSAO_CADU';

--------------------------------------------------------------------------------------------------------------------------------

create table 7kk_full_credito_dummies as 
select *,
case when in_cartao_credito = "" then 1 else 0 end as in_cartao_credito_vazio,
case when in_cartao_credito = '0' then 1 else 0 end as in_cartao_credito_0,
case when in_cartao_credito = '1' then 1 else 0 end as in_cartao_credito_1,
case when in_cheque_especial = "" then 1 else 0 end as in_cheque_especial_vazio,
case when in_cheque_especial = '0' then 1 else 0 end as in_cheque_especial_0,
case when in_cheque_especial = '1' then 1 else 0 end as in_cheque_especial_1,
case when in_credito_pessoal = "" then 1 else 0 end as in_credito_pessoal_vazio,
case when in_credito_pessoal = '0' then 1 else 0 end as in_credito_pessoal_0,
case when in_credito_pessoal = '1' then 1 else 0 end as in_credito_pessoal_1,
case when in_financ_cartao_credito = "" then 1 else 0 end as in_financ_cartao_credito_vazio,
case when in_financ_cartao_credito = '0' then 1 else 0 end as in_financ_cartao_credito_0,
case when in_financ_cartao_credito = '1' then 1 else 0 end as in_financ_cartao_credito_1,
case when cd_possui_restr_historico = "S" then 1 else 0 end as cd_possui_restr_historico_S,
case when cd_possui_restr_historico = "N" then 1 else 0 end as cd_possui_restr_historico_N,
case when cd_possui_restr_historico = "" then 1 else 0 end as cd_possui_restr_historico_vazio,
case when POSSUI_PARCELADO = "S" then 1 else 0 end as POSSUI_PARCELADO_SIM,
case when POSSUI_PARCELADO = "N" then 1 else 0 end as POSSUI_PARCELADO_NAO,
case when POSSUI_PARCELADO = "" then 1 else 0 end as POSSUI_PARCELADO_vazio,
case when BACEN_IMO = "S" then 1 else 0 end as BACEN_IMO_SIM,
case when BACEN_IMO = "N" then 1 else 0 end as BACEN_IMO_NAO,
case when BACEN_IMO = "" then 1 else 0 end as BACEN_IMO_vazio,
case when IN_CDCVEIC_LEASING = '1' then 1 else 0 end as IN_CDCVEIC_LEASING_SIM,
case when IN_CDCVEIC_LEASING = '0' then 1 else 0 end as IN_CDCVEIC_LEASING_NAO,
case when IN_CDCVEIC_LEASING = "" then 1 else 0 end as IN_CDCVEIC_LEASING_vazio,
case when IN_FINANC_IMOBILIARIO = '1' then 1 else 0 end as IN_FINANC_IMOBILIARIO_SIM,
case when IN_FINANC_IMOBILIARIO = '0' then 1 else 0 end as IN_FINANC_IMOBILIARIO_NAO,
case when IN_FINANC_IMOBILIARIO = "" then 1 else 0 end as IN_FINANC_IMOBILIARIO_vazio
from cmr_base_04_credito_final;


create table 7kk_Log_join_var_credito as
select A.*, 
B.profissao_cadu,
B.score_acsp_19,
B.qt_rtrit_hist_prim,
B.flag_aposentado,
B.renda_final,
B.qt_rtrit_hist,
B.score_5pf,
B.in_cartao_credito,
B.in_cheque_especial,
B.in_credito_pessoal,
B.in_financ_cartao_credito,
B.pc_utlz_cartao_12m_exp,
B.pc_utlz_cartao_36m,
B.pc_utlz_cartao_mes,
B.pc_utlz_chq_lim_glob_12m_exp,
B.pc_utlz_chq_lim_glob_36m,
B.pc_utlz_chq_lim_glob_mes,
B.qt_default_36m,
B.qt_dt_base_def_36m,
B.qt_dt_base_inicio_36m,
B.qt_meses_possui_cartao_12m,
B.qt_meses_possui_cartao_36m,
B.qt_meses_possui_chq_lim_glob_12m,
B.qt_meses_possui_chq_lim_glob_36m,
B.qt_utlz_cartao_12m,
B.qt_utlz_cartao_mes,
B.qt_utlz_chq_lim_glob_36m,
B.dt_ultimo_rest_12meses,
B.qt_meses_rest_12m,
B.qt_princ_rtrit_hist_prim,
B.cd_possui_restr_historico,
B.possui_parcelado,
B.pc_a_utilizar_compr_chq_lim_glob,
B.pc_a_utilizar_compr_cartao,
B.pc_a_utilizar_compr_ctagarant,
B.bacen_imo,
B.in_cdcveic_leasing,
B.in_financ_imobiliario,
B.qt_utlz_chq_lim_glob_mes,
B.qt_utlz_chq_lim_glob_12m,
B.pc_utlz_ctagarantida_12m_exp,
B.qt_meses_possui_ctagarantida_12m,
B.vr_sld_medio_semest_00070_sm,
B.dt_conta,
B.in_cartao_credito_vazio,
B.in_cartao_credito_0,
B.in_cartao_credito_1,
B.in_cheque_especial_vazio,
B.in_cheque_especial_0,
B.in_cheque_especial_1,
B.in_credito_pessoal_vazio,
B.in_credito_pessoal_0,
B.in_credito_pessoal_1,
B.in_financ_cartao_credito_vazio,
B.in_financ_cartao_credito_0,
B.in_financ_cartao_credito_1,
B.cd_possui_restr_historico_s,
B.cd_possui_restr_historico_n,
B.cd_possui_restr_historico_vazio,
B.possui_parcelado_sim,
B.possui_parcelado_nao,
B.possui_parcelado_vazio,
B.bacen_imo_sim,
B.bacen_imo_nao,
B.bacen_imo_vazio,
B.in_cdcveic_leasing_sim,
B.in_cdcveic_leasing_nao,
B.in_cdcveic_leasing_vazio,
B.in_financ_imobiliario_sim,
B.in_financ_imobiliario_nao,
B.in_financ_imobiliario_vazio
from cmr_7kk_variaveis_log as A Left join
7kk_full_credito_dummies as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;