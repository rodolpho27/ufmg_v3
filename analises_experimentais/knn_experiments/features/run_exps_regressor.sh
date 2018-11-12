#!/bin/bash +x
echo features claro 
sudo python all_datasets/fidelidade_prezao.py -db files3_claro_mig -o saida/claro_mig/
sudo mv fidelidade_prezao
sudo python all_datasets/fidelidade_prezao.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/client_services.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/client_services.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/dia_mes.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/dia_mes.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/dia_semana.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/dia_semana.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/features_clientes.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/features_clientes.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/features_ligacoes_clientes.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/features_ligacoes_clientes.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/hora.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/hora.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/mailing.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/mailing.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/qtd_prezao.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/qtd_prezao.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/mailing.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/mailing.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python all_datasets/whitelist.py -db files3_claro_mig -o saida/claro_mig/
sudo python all_datasets/whitelist.py -db files3_claro_mig_model -o saida/claro_mig_model/

#felipe
echo features claro felipe

sudo python claro_mig/features_iuri.py -db files3_claro_mig -o saida/claro_mig/
sudo python claro_mig/features_iuri.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python claro_mig/features_planos_coef_var.py -db files3_claro_mig -o saida/claro_mig/
sudo python claro_mig/features_planos_coef_var.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python claro_mig/features_planos.py -db files3_claro_mig -o saida/claro_mig/
sudo python claro_mig/features_planos.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python claro_mig/features_recarga_claro_iuri_1.py -db files3_claro_mig -o saida/claro_mig/
sudo python claro_mig/features_recarga_claro_iuri_1.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python claro_mig/features_recarga_intervalos_temp.py -db files3_claro_mig -o saida/claro_mig/
sudo python claro_mig/features_recarga_intervalos_temp.py -db files3_claro_mig_model -o saida/claro_mig_model/

sudo python claro_mig/features_recarga_lag_vivo.py -db files3_claro_mig -o saida/claro_mig/
sudo python claro_mig/features_recarga_lag_vivo.py -db files3_claro_mig_model -o saida/claro_mig_model/


sudo python claro_mig/features_recarga.py -db files3_claro_mig -o saida/claro_mig/
sudo python claro_mig/features_recarga.py -db files3_claro_mig_model -o saida/claro_mig_model/


#vivo

echo features vivo
sudo python all_datasets/vivo_idade.py -db files3_claro_mig -o saida/vivo_mig/
sudo python all_datasets/vivo_idade.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python all_datasets/vivo_mailing.py -db files3_claro_mig -o saida/vivo_mig/
sudo python all_datasets/vivo_mailing.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python all_datasets/vivo_recarga.py -db files3_claro_mig -o saida/vivo_mig/
sudo python all_datasets/vivo_recarga.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python all_datasets/vivo_status_cliente.py -db files3_claro_mig -o saida/vivo_mig/
sudo python all_datasets/vivo_status_cliente.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python vivo_mig/features_planos.py -db files3_claro_mig -o saida/vivo_mig/
sudo python vivo_mig/features_planos.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python vivo_mig/features_recarga_claro_iuri_1.py -db files3_claro_mig -o saida/vivo_mig/
sudo python vivo_mig/features_recarga_claro_iuri_1.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python vivo_mig/features_recarga_intervalos_temp.py -db files3_claro_mig -o saida/vivo_mig/
sudo python vivo_mig/features_recarga_intervalos_temp.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python vivo_mig/features_recarga_lag_vivo.py -db files3_claro_mig -o saida/vivo_mig/
sudo python vivo_mig/features_recarga_lag_vivo.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python vivo_mig/features_recarga.py -db files3_claro_mig -o saida/vivo_mig/
sudo python vivo_mig/features_recarga.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python vivo_mig/features_iuri.py -db files3_claro_mig -o saida/vivo_mig/
sudo python vivo_mig/features_iuri.py -db files3_claro_mig_model -o saida/vivo_mig_model/

sudo python vivo_mig/features_planos_coef_var.py -db files3_claro_mig -o saida/vivo_mig/
sudo python vivo_mig/features_planos_coef_var.py -db files3_claro_mig_model -o saida/vivo_mig_model/


