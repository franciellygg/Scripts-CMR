create table 7kk_uniao_6kk_final_join_y_temp as
select A.*, 
case when B.numero_cpf_cnpj is not null then 1 else 0 end as y_ci
from 7kk_uniao_6kk_final as A left join
cmr_15k_ate_fim_dezem as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;

#Passar a nova variavel "y_ci" para inteiro.
create table 7kk_uniao_6kk_final_join_y as
select numero_cpf_cnpj, numero_agencia, numero_conta, qtd_contas, mais_de_uma, id_tipo_pessoa, id_codigo_segmento, id_razao, idade_conta, idade_individuo, faixa_idade_individuo, id_codigo_segmento2, is_classic, is_exclusive, is_prime, faixa_idade_0_20, faixa_idade_21_30, faixa_idade_31_40, faixa_idade_41_50, faixa_idade_51_60, faixa_idade_61_70, faixa_idade_71_80, faixa_idade_maior80, x_uf, x_reg, x_reg_co, x_reg_ne, x_reg_n, x_reg_s, x_reg_se, x_renc, x_renc_negativo, x_renc_quartil, x_renc_q1, x_renc_q2, x_renc_q3, x_renc_q4, x_q1m_can_aa_f, x_q1m_can_aa_nf, x_q1m_can_mb_f, x_q1m_can_mb_nf, x_q1m_can_ib_f, x_q1m_can_ib_nf, x_q1m_can_ff_f, x_q1m_can_ff_nf, x_q1a_can_aa_f, x_q1a_can_aa_nf, x_q1a_can_mb_f, x_q1a_can_mb_nf, x_q1a_can_ib_f, x_q1a_can_ib_nf, x_q1a_can_ff_f, x_q1a_can_ff_nf, x_q1m_bh1_0_4_f, x_q1m_bh1_0_4_nf, x_q1m_bh1_4_8_f, x_q1m_bh1_4_8_nf, x_q1m_bh1_8_12_f, x_q1m_bh1_8_12_nf, x_q1m_bh1_12_16_f, x_q1m_bh1_12_16_nf, x_q1m_bh1_16_20_f, x_q1m_bh1_16_20_nf, x_q1m_bh1_20_24_f, x_q1m_bh1_20_24_nf, x_q1a_bh1_0_4_f, x_q1a_bh1_0_4_nf, x_q1a_bh1_4_8_f, x_q1a_bh1_4_8_nf, x_q1a_bh1_8_12_f, x_q1a_bh1_8_12_nf, x_q1a_bh1_12_16_f, x_q1a_bh1_12_16_nf, x_q1a_bh1_16_20_f, x_q1a_bh1_16_20_nf, x_q1a_bh1_20_24_f, x_q1a_bh1_20_24_nf, x_q1m_fim_semana_f, x_q1m_fim_semana_nf, x_q1a_fim_semana_f, x_q1a_fim_semana_nf, x_qconsulta_24_06h, x_qconsulta_06_12h, x_qconsulta_12_18h, x_qconsulta_18_24h, x_tmqsaques_aa_1m, x_tmqsaques_aa_1m_quartil, x_tmqsaques_aa_1m_q1, x_tmqsaques_aa_1m_q2, x_tmqsaques_aa_1m_q3, x_tmqsaques_aa_1m_q4, x_tmqsaques_aa_2m, x_tmqsaques_aa_2m_quartil, x_tmqsaques_aa_2m_q1, x_tmqsaques_aa_2m_q2, x_tmqsaques_aa_2m_q3, x_tmqsaques_aa_2m_q4, x_tmqsaques_aa_1a, x_tmqsaques_aa_1a_quartil, x_tmqsaques_aa_1a_q1, x_tmqsaques_aa_1a_q2, x_tmqsaques_aa_1a_q3, x_tmqsaques_aa_1a_q4, x_tmqtransfer_aa_1m, x_tmqtransfer_aa_1m_quartil, x_tmqtransfer_aa_1m_q1, x_tmqtransfer_aa_1m_q2, x_tmqtransfer_aa_1m_q3, x_tmqtransfer_aa_1m_q4, x_tmqtransfer_aa_2m, x_tmqtransfer_aa_2m_quartil, x_tmqtransfer_aa_2m_q1, x_tmqtransfer_aa_2m_q2, x_tmqtransfer_aa_2m_q3, x_tmqtransfer_aa_2m_q4, x_tmqtransfer_aa_1a, x_tmqtransfer_aa_1a_quartil, x_tmqtransfer_aa_1a_q1, x_tmqtransfer_aa_1a_q2, x_tmqtransfer_aa_1a_q3, x_tmqtransfer_aa_1a_q4, x_tmqinvestim_aa_1m, x_tmqinvestim_aa_1m_quartil, x_tmqinvestim_aa_1m_q1, x_tmqinvestim_aa_1m_q2, x_tmqinvestim_aa_1m_q3, x_tmqinvestim_aa_1m_q4, x_tmqinvestim_aa_2m, x_tmqinvestim_aa_2m_quartil, x_tmqinvestim_aa_2m_q1, x_tmqinvestim_aa_2m_q2, x_tmqinvestim_aa_2m_q3, x_tmqinvestim_aa_2m_q4, x_tmqinvestim_aa_1a, x_tmqinvestim_aa_1a_quartil, x_tmqinvestim_aa_1a_q1, x_tmqinvestim_aa_1a_q2, x_tmqinvestim_aa_1a_q3, x_tmqinvestim_aa_1a_q4, x_somatsaques_aa_1m, x_somatsaques_aa_1m_quartil, x_somatsaques_aa_1m_q1, x_somatsaques_aa_1m_q2, x_somatsaques_aa_1m_q3, x_somatsaques_aa_1m_q4, x_somatsaques_aa_6m, x_somatsaques_aa_6m_quartil, x_somatsaques_aa_6m_q1, x_somatsaques_aa_6m_q2, x_somatsaques_aa_6m_q3, x_somatsaques_aa_6m_q4, x_tmqlime1m, x_tmqlime1m_quartil, x_tmqlime1m_q1, x_tmqlime1m_q2, x_tmqlime1m_q3, x_tmqlime1m_q4, x_tmqlime1a, x_tmqlime1a_quartil, x_tmqlime1a_q1, x_tmqlime1a_q2, x_tmqlime1a_q3, x_tmqlime1a_q4, x_q6m_produto, x_q1a_produto, x_c6m_produto, x_c1a_produto, x_tmq6m_produto, x_tmq1a_produto, x_soma6m_produto, x_soma1a_produto, profissao_cadu, score_acsp_19, qt_rtrit_hist_prim, flag_aposentado, renda_final, qt_rtrit_hist, score_5pf, in_cartao_credito, in_cheque_especial, in_credito_pessoal, in_financ_cartao_credito, pc_utlz_cartao_12m_exp, pc_utlz_cartao_36m, pc_utlz_cartao_mes, pc_utlz_chq_lim_glob_12m_exp, pc_utlz_chq_lim_glob_36m, pc_utlz_chq_lim_glob_mes, qt_default_36m, qt_dt_base_def_36m, qt_dt_base_inicio_36m, qt_meses_possui_cartao_12m, qt_meses_possui_cartao_36m, qt_meses_possui_chq_lim_glob_12m, qt_meses_possui_chq_lim_glob_36m, qt_utlz_cartao_12m, qt_utlz_cartao_mes, qt_utlz_chq_lim_glob_36m, dt_ultimo_rest_12meses, qt_meses_rest_12m, qt_princ_rtrit_hist_prim, cd_possui_restr_historico, possui_parcelado, pc_a_utilizar_compr_chq_lim_glob, pc_a_utilizar_compr_cartao, pc_a_utilizar_compr_ctagarant, bacen_imo, in_cdcveic_leasing, in_financ_imobiliario, qt_utlz_chq_lim_glob_mes, qt_utlz_chq_lim_glob_12m, pc_utlz_ctagarantida_12m_exp, qt_meses_possui_ctagarantida_12m, vr_sld_medio_semest_00070_sm, dt_conta, in_cartao_credito_vazio, in_cartao_credito_0, in_cartao_credito_1, in_cheque_especial_vazio, in_cheque_especial_0, in_cheque_especial_1, in_credito_pessoal_vazio, in_credito_pessoal_0, in_credito_pessoal_1, in_financ_cartao_credito_vazio, in_financ_cartao_credito_0, in_financ_cartao_credito_1, cd_possui_restr_historico_s, cd_possui_restr_historico_n, possui_parcelado_sim, possui_parcelado_nao, bacen_imo_sim, bacen_imo_nao, in_cdcveic_leasing_sim, in_cdcveic_leasing_nao, in_cdcveic_leasing_vazio, in_financ_imobiliario_sim, in_financ_imobiliario_nao, in_financ_imobiliario_vazio,
cast(y_ci as int) as y_ci
from 7kk_uniao_6kk_final_join_y_temp;

drop table 7kk_uniao_6kk_final_join_y_temp purge;

#Adicionar variaveis conversao que nao estavam na tabela
create table 7kk_uniao_6kk_final_join_y_2 as
select *,
case when X_Q1M_CAN_AA_F > 0 then 1 else 0 end as X_C1M_CAN_AA_F,
case when X_Q1M_CAN_AA_NF > 0 then 1 else 0 end as X_C1M_CAN_AA_NF,
case when X_Q1M_CAN_MB_F > 0 then 1 else 0 end as X_C1M_CAN_MB_F,
case when X_Q1M_CAN_MB_NF > 0 then 1 else 0 end as X_C1M_CAN_MB_NF,
case when X_Q1M_CAN_IB_F > 0 then 1 else 0 end as X_C1M_CAN_IB_F,
case when X_Q1M_CAN_IB_NF > 0 then 1 else 0 end as X_C1M_CAN_IB_NF,
case when X_Q1M_CAN_FF_F > 0 then 1 else 0 end as X_C1M_CAN_FF_F,
case when X_Q1M_CAN_FF_NF > 0 then 1 else 0 end as X_C1M_CAN_FF_NF
from 7kk_uniao_6kk_final_join_y;

#Adicionar variaveis quantitativas que não estavam na tabela
create table zz_tabela1 as 
select numero_cpf_cnpj, 
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '1' and valor>0 then 1 else 0 end) as X_Q1M_DIA_SEG_F,
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '1' and (valor<=0 or valor is null) then 1 else 0 end) as X_Q1M_DIA_SEG_NF,
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '2' and valor>0 then 1 else 0 end) as X_Q1M_DIA_TERCA_F,
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '2' and (valor<=0 or valor is null) then 1 else 0 end) as X_Q1M_DIA_TERCA_NF,
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '3' and valor>0 then 1 else 0 end) as X_Q1M_DIA_QUARTA_F,
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '3' and (valor<=0 or valor is null) then 1 else 0 end) as X_Q1M_DIA_QUARTA_NF,
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '4' and valor>0 then 1 else 0 end) as X_Q1M_DIA_QUINTA_F,
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '4' and (valor<=0 or valor is null) then 1 else 0 end) as X_Q1M_DIA_QUINTA_NF,
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '5' and valor>0 then 1 else 0 end) as X_Q1M_DIA_SEXTA_F,
sum(case when to_date(id_data_hora) >='2017-09-01' and to_date(id_data_hora) <='2017-09-30'
 and dia_semana = '5' and (valor<=0 or valor is null) then 1 else 0 end) as X_Q1M_DIA_SEXTA_NF
from cmr_6kk_trans group by numero_cpf_cnpj;

create table 7kk_uniao_6kk_final_join_y_3 as
select A.*, 
B.X_Q1M_DIA_SEG_F, B.X_Q1M_DIA_SEG_NF, B.X_Q1M_DIA_TERCA_F, B.X_Q1M_DIA_TERCA_NF, B.X_Q1M_DIA_QUARTA_F, B.X_Q1M_DIA_QUARTA_NF, B.X_Q1M_DIA_QUINTA_F, B.X_Q1M_DIA_QUINTA_NF, B.X_Q1M_DIA_SEXTA_F, B.X_Q1M_DIA_SEXTA_NF
from 7kk_uniao_6kk_final_join_y_2 as A left join
zz_tabela1 as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;

drop table zz_tabela1 purge;

----------------------------Ainda não criei-------------------------------------------------------------------------------

create table teste_seg as 
select A.*, 
sum(case when date(id_data_hora) >= '2017-10-01' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 1 and valor>0 and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_F,
sum(case when date(id_data_hora) >= '2017-10-31' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 1 and (valor<=0 or valor is null) and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_NF
from 7kk_uniao_6kk_final as A join
cmr_15k_ate_fim_dezem as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;

create table teste_ter as 
select A.*, 
sum(case when date(id_data_hora) >= '2017-10-01' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 2 and valor>0 and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_F,
sum(case when date(id_data_hora) >= '2017-10-31' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 2 and (valor<=0 or valor is null) and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_NF
from 7kk_uniao_6kk_final as A join
cmr_15k_ate_fim_dezem as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;

create table teste_qua as 
select A.*, 
sum(case when date(id_data_hora) >= '2017-10-01' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 3 and valor>0 and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_F,
sum(case when date(id_data_hora) >= '2017-10-31' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 3 and (valor<=0 or valor is null) and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_NF
from 7kk_uniao_6kk_final as A join
cmr_15k_ate_fim_dezem as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;

create table teste_qui as 
select A.*, 
sum(case when date(id_data_hora) >= '2017-10-01' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 4 and valor>0 and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_F,
sum(case when date(id_data_hora) >= '2017-10-31' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 4 and (valor<=0 or valor is null) and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_NF
from 7kk_uniao_6kk_final as A join
cmr_15k_ate_fim_dezem as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;

create table teste_qui as 
select A.*, 
sum(case when date(id_data_hora) >= '2017-10-01' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 5 and valor>0 and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_F,
sum(case when date(id_data_hora) >= '2017-10-31' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 5 and (valor<=0 or valor is null) and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_NF
from 7kk_uniao_6kk_final as A join
cmr_15k_ate_fim_dezem as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;

create table teste_sex as 
select A.*, 
sum(case when date(id_data_hora) >= '2017-10-01' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 5 and valor>0 and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_F,
sum(case when date(id_data_hora) >= '2017-10-31' and date(id_data_hora) <='2017-12-31'
 and dia_semana = 5 and (valor<=0 or valor is null) and B.numero_cpf_cnpj is not null then 1 else 0 end) as Y_Q3M_DIA_SEG_NF
from 7kk_uniao_6kk_final as A join
cmr_15k_ate_fim_dezem as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;
