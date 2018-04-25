## No jupiter notebook, foram criados 4 arquivos que separavam apenas os cpfs de clientes propensos nas faixas horarias.
## Então passei esses arquivos para o hive com os nomes madru_f, manha_f, tarde_f e noite_f

#Criando dummie, tabela com 55317 cpfs de propensos
create table faixa_hora_madru as
select numero_cpf_cnpj, (case when numero_cpf_cnpj != "123456789" then 1 else 0 end) as madru
from madru_f;

#Criando dummie, tabela com 454906 cpfs de propensos
create table faixa_hora_manha as
select numero_cpf_cnpj, (case when numero_cpf_cnpj != "123456789" then 1 else 0 end) as manha
from manha_f;

#Criando dummie, tabela com 827801 cpfs de propensos
create table faixa_hora_tarde as
select numero_cpf_cnpj, (case when numero_cpf_cnpj != "123456789" then 1 else 0 end) as tarde
from tarde_f;

#Criando dummie, tabela com 293242 cpfs de propensos
create table faixa_hora_noite as
select numero_cpf_cnpj, (case when numero_cpf_cnpj != "123456789" then 1 else 0 end) as noite
from noite_f;

drop table madru_f purge;
drop table manha_f purge;
drop table tarde_f purge;
drop table noite_f purge;


#Fazendo join com todos os cpfs e colocar 0 para os clientes não propensos
create table faixas_hora_1 as
select A.numero_cpf_cnpj, B.madru, C.manha, D.tarde, E.noite
from 7kk_fixa_log_cred_hab_dummies_y_2 as A
left join faixa_hora_madru as B on A.numero_cpf_cnpj = B.numero_cpf_cnpj
left join faixa_hora_manha as C on A.numero_cpf_cnpj = C.numero_cpf_cnpj
left join faixa_hora_tarde as D on A.numero_cpf_cnpj = D.numero_cpf_cnpj
left join faixa_hora_noite as E on A.numero_cpf_cnpj = E.numero_cpf_cnpj;

create table faixas_hora_2 as
select numero_cpf_cnpj, 
(case when madru is not null then 1 else 0 end) as madru,
(case when manha is not null then 1 else 0 end) as manha,
(case when tarde is not null then 1 else 0 end) as tarde,
(case when noite is not null then 1 else 0 end) as noite
from faixas_hora_1;

#Verificando em quantas faixas horarias o cliente é propenso
create table faixas_hora_3 as
select numero_cpf_cnpj, madru, manha, tarde, noite,
(madru + manha + tarde + noite) as qnt_dispo
from faixas_hora_2;

#Intersecao dos 4 horarios
create table faixas_hora_4 as
select numero_cpf_cnpj, madru, manha, tarde, noite, qnt_dispo,
case when qnt_dispo = 4 then 1 else 0 end as madru_manha_tarde_noite
from faixas_hora_3;

#Intersecao de 3 horarios
create table faixas_hora_5 as
select numero_cpf_cnpj, madru, manha, tarde, noite, qnt_dispo, madru_manha_tarde_noite,
case when qnt_dispo = 3 and madru = 1 and manha = 1 and tarde = 1 then 1 else 0 end as madru_manha_tarde,
case when qnt_dispo = 3 and madru = 1 and manha = 1 and noite = 1 then 1 else 0 end as madru_manha_noite,
case when qnt_dispo = 3 and madru = 1 and tarde = 1 and noite = 1 then 1 else 0 end as madru_tarde_noite,
case when qnt_dispo = 3 and manha = 1 and tarde = 1 and noite = 1 then 1 else 0 end as manha_tarde_noite
from faixas_hora_4;

#Intersecao de 2 horarios
create table faixas_hora_6 as
select numero_cpf_cnpj, madru, manha, tarde, noite, qnt_dispo, madru_manha_tarde_noite,madru_manha_tarde, madru_manha_noite, madru_tarde_noite, manha_tarde_noite,
case when qnt_dispo = 2 and madru = 1 and manha = 1 then 1 else 0 end as madru_manha,
case when qnt_dispo = 2 and madru = 1 and tarde = 1 then 1 else 0 end as madru_tarde,
case when qnt_dispo = 2 and madru = 1 and noite = 1 then 1 else 0 end as madru_noite,
case when qnt_dispo = 2 and manha = 1 and tarde = 1 then 1 else 0 end as manha_tarde,
case when qnt_dispo = 2 and manha = 1 and noite = 1 then 1 else 0 end as manha_noite,
case when qnt_dispo = 2 and tarde = 1 and noite = 1 then 1 else 0 end as tarde_noite
from faixas_hora_5;

#Apenas em uma faixa de hora (Tabela Final)
create table faixas_hora as
select numero_cpf_cnpj, madru, manha, tarde, noite, qnt_dispo, madru_manha_tarde_noite,madru_manha_tarde, madru_manha_noite, madru_tarde_noite, manha_tarde_noite, madru_manha, madru_tarde, madru_noite, manha_tarde, manha_noite, tarde_noite,
case when qnt_dispo = 1 and madru = 1 then 1 else 0 end as madru_apenas,
case when qnt_dispo = 1 and manha = 1 then 1 else 0 end as manha_apenas,
case when qnt_dispo = 1 and tarde = 1 then 1 else 0 end as tarde_apenas,
case when qnt_dispo = 1 and noite = 1 then 1 else 0 end as noite_apenas
from faixas_hora_6;

drop table faixas_hora_1 purge;
drop table faixas_hora_2 purge;
drop table faixas_hora_3 purge;
drop table faixas_hora_4 purge;
drop table faixas_hora_5 purge;
drop table faixas_hora_6 purge;


-------------Conferindo os numeros----------------------------------------------------------------------------------------------

select count (madru_manha_tarde_noite) from faixas_hora where madru_manha_tarde_noite = 1;
select count (madru_manha_tarde) from faixas_hora where madru_manha_tarde = 1;
select count (madru_manha_noite) from faixas_hora where madru_manha_noite = 1;
select count (madru_tarde_noite) from faixas_hora where madru_tarde_noite = 1;
select count (manha_tarde_noite) from faixas_hora where manha_tarde_noite = 1;
select count (madru_manha) from faixas_hora where madru_manha = 1;
select count (madru_tarde) from faixas_hora where madru_tarde = 1;
select count (madru_noite) from faixas_hora where madru_noite = 1;
select count (manha_tarde) from faixas_hora where manha_tarde = 1;
select count (manha_noite) from faixas_hora where manha_noite = 1;
select count (tarde_noite) from faixas_hora where tarde_noite = 1;
select count (madru_apenas) from faixas_hora where madru_apenas = 1;
select count (manha_apenas) from faixas_hora where manha_apenas = 1;
select count (tarde_apenas) from faixas_hora where tarde_apenas = 1;
select count (noite_apenas) from faixas_hora where noite_apenas = 1;

+--------------------------------+
| count(madru_manha_tarde_noite) |
+--------------------------------+
| 52010                          |
+--------------------------------+
Fetched 1 row(s) in 0.56s
[d4251pad013:21000] > select count (madru_manha_tarde) from faixas_hora where madru_manha_tarde = 1;
Query: select count (madru_manha_tarde) from faixas_hora where madru_manha_tarde = 1
Query submitted at: 2018-02-21 17:21:35 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=fc457b74297f74ab:2426894100000000
+--------------------------+
| count(madru_manha_tarde) |
+--------------------------+
| 116                      |
+--------------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (madru_manha_noite) from faixas_hora where madru_manha_noite = 1;
Query: select count (madru_manha_noite) from faixas_hora where madru_manha_noite = 1
Query submitted at: 2018-02-21 17:21:36 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=5f4263e1cd1c8131:5343964600000000
+--------------------------+
| count(madru_manha_noite) |
+--------------------------+
| 67                       |
+--------------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (madru_tarde_noite) from faixas_hora where madru_tarde_noite = 1;
Query: select count (madru_tarde_noite) from faixas_hora where madru_tarde_noite = 1
Query submitted at: 2018-02-21 17:21:36 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=794160f92780d458:f803846300000000
+--------------------------+
| count(madru_tarde_noite) |
+--------------------------+
| 118                      |
+--------------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (manha_tarde_noite) from faixas_hora where manha_tarde_noite = 1;
Query: select count (manha_tarde_noite) from faixas_hora where manha_tarde_noite = 1
Query submitted at: 2018-02-21 17:21:37 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=ad40c4305cc991ce:b3b24c4600000000
+--------------------------+
| count(manha_tarde_noite) |
+--------------------------+
| 4466                     |
+--------------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (madru_manha) from faixas_hora where madru_manha = 1;
Query: select count (madru_manha) from faixas_hora where madru_manha = 1
Query submitted at: 2018-02-21 17:21:37 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=1e4f32e9d3bd8f52:fe933d9800000000
+--------------------+
| count(madru_manha) |
+--------------------+
| 236                |
+--------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (madru_tarde) from faixas_hora where madru_tarde = 1;
Query: select count (madru_tarde) from faixas_hora where madru_tarde = 1
Query submitted at: 2018-02-21 17:21:38 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=8947c7973479a1d0:34b5acfc00000000
+--------------------+
| count(madru_tarde) |
+--------------------+
| 370                |
+--------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (madru_noite) from faixas_hora where madru_noite = 1;
Query: select count (madru_noite) from faixas_hora where madru_noite = 1
Query submitted at: 2018-02-21 17:21:38 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=bc40fd664472b2cd:a3b6867800000000
+--------------------+
| count(madru_noite) |
+--------------------+
| 248                |
+--------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (manha_tarde) from faixas_hora where manha_tarde = 1;
Query: select count (manha_tarde) from faixas_hora where manha_tarde = 1
Query submitted at: 2018-02-21 17:21:39 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=da45e2219c331018:2766408000000000
+--------------------+
| count(manha_tarde) |
+--------------------+
| 41735              |
+--------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (manha_noite) from faixas_hora where manha_noite = 1;
Query: select count (manha_noite) from faixas_hora where manha_noite = 1
Query submitted at: 2018-02-21 17:21:40 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=3d4db6962c00d170:5091907d00000000
+--------------------+
| count(manha_noite) |
+--------------------+
| 10331              |
+--------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (tarde_noite) from faixas_hora where tarde_noite = 1;
Query: select count (tarde_noite) from faixas_hora where tarde_noite = 1
Query submitted at: 2018-02-21 17:21:40 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=de4766ef864e552b:5c452a2000000000
+--------------------+
| count(tarde_noite) |
+--------------------+
| 24504              |
+--------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (madru_apenas) from faixas_hora where madru_apenas = 1;
Query: select count (madru_apenas) from faixas_hora where madru_apenas = 1
Query submitted at: 2018-02-21 17:21:41 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=e5442ba79fd7dcca:2b62e93c00000000
+---------------------+
| count(madru_apenas) |
+---------------------+
| 2152                |
+---------------------+
Fetched 1 row(s) in 0.54s
[d4251pad013:21000] > select count (manha_apenas) from faixas_hora where manha_apenas = 1;
Query: select count (manha_apenas) from faixas_hora where manha_apenas = 1
Query submitted at: 2018-02-21 17:21:41 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=3d4206da605c88d6:bcec37cf00000000
+---------------------+
| count(manha_apenas) |
+---------------------+
| 345945              |
+---------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (tarde_apenas) from faixas_hora where tarde_apenas = 1;
Query: select count (tarde_apenas) from faixas_hora where tarde_apenas = 1
Query submitted at: 2018-02-21 17:21:42 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=134cc426f5383a80:21a0e90800000000
+---------------------+
| count(tarde_apenas) |
+---------------------+
| 704482              |
+---------------------+
Fetched 1 row(s) in 0.55s
[d4251pad013:21000] > select count (noite_apenas) from faixas_hora where noite_apenas = 1;
Query: select count (noite_apenas) from faixas_hora where noite_apenas = 1
Query submitted at: 2018-02-21 17:21:42 (Coordinator: https://d4251pad013.d4251d01.net:25000)
Query progress can be monitored at: https://d4251pad013.d4251d01.net:25000/query_plan?query_id=c64b153ca70b1010:c7e8311100000000
+---------------------+
| count(noite_apenas) |
+---------------------+
| 201498              |
+---------------------+


