[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_ACAO_ORCAMENTARIA_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_ACAO_ORCAMENTARIA", DestinationColumnName = "ID_ACAO_ORCAMENTARIA"], [SourceColumnName = "NO_ACAO_ORCAMENTARIA", DestinationColumnName = "NO_ACAO_ORCAMENTARIA"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_ACAO_ORCAMENTARIA = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_ACAO_ORCAMENTARIA"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_ANEXO_DECRETO_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_ANEXO_DECRETO_SK", DestinationColumnName = "ID_ANEXO_DECRETO_SK"], [SourceColumnName = "NO_ANEXO_DECRETO", DestinationColumnName = "NO_ANEXO_DECRETO"], [SourceColumnName = "NR_ORDEM_ANEXO", DestinationColumnName = "NR_ORDEM_ANEXO"], [SourceColumnName = "NO_FORMATADO", DestinationColumnName = "NO_FORMATADO"], [SourceColumnName = "DS_ANEXO_DECRETO", DestinationColumnName = "DS_ANEXO_DECRETO"], [SourceColumnName = "IN_TIPO_ANEXO", DestinationColumnName = "IN_TIPO_ANEXO"], [SourceColumnName = "AN_EXERCICIO", DestinationColumnName = "AN_EXERCICIO"], [SourceColumnName = "IN_TIPO_LIMITE", DestinationColumnName = "IN_TIPO_LIMITE"], [SourceColumnName = "SN_AFETA_LIMITE", DestinationColumnName = "SN_AFETA_LIMITE"], [SourceColumnName = "ID_MARCA_CLASSIFICACAO_DESP_SK", DestinationColumnName = "ID_MARCA_CLASSIFICACAO_DESP_SK"], [SourceColumnName = "ID_MARCA_CLASS_PAGTO_EFET_SK", DestinationColumnName = "ID_MARCA_CLASS_PAGTO_EFET_SK"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_ANEXO_DECRETO = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_ANEXO_DECRETO"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_ANEXO_DECRETO_DESP_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "IS_ANEXO_DECRETO_DESP", DestinationColumnName = "IS_ANEXO_DECRETO_DESP"], [SourceColumnName = "ID_ANEXO_DECRETO_SK", DestinationColumnName = "ID_ANEXO_DECRETO_SK"], [SourceColumnName = "NO_ANEXO_DECRETO", DestinationColumnName = "NO_ANEXO_DECRETO"], [SourceColumnName = "NR_ORDEM_ANEXO", DestinationColumnName = "NR_ORDEM_ANEXO"], [SourceColumnName = "NO_FORMATADO", DestinationColumnName = "NO_FORMATADO"], [SourceColumnName = "DS_ANEXO_DECRETO", DestinationColumnName = "DS_ANEXO_DECRETO"], [SourceColumnName = "IN_TIPO_ANEXO", DestinationColumnName = "IN_TIPO_ANEXO"], [SourceColumnName = "AN_EXERCICIO", DestinationColumnName = "AN_EXERCICIO"], [SourceColumnName = "IN_TIPO_LIMITE", DestinationColumnName = "IN_TIPO_LIMITE"], [SourceColumnName = "SN_AFETA_LIMITE", DestinationColumnName = "SN_AFETA_LIMITE"], [SourceColumnName = "ID_MARCA_CLASSIFICACAO_DESP_SK", DestinationColumnName = "ID_MARCA_CLASSIFICACAO_DESP_SK"], [SourceColumnName = "ID_MARCA_CLASS_PAGTO_EFET_SK", DestinationColumnName = "ID_MARCA_CLASS_PAGTO_EFET_SK"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_ANEXO_DECRETO_DESP = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_ANEXO_DECRETO_DESP"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_CALENDARIO_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "DT_CALENDARIO", DestinationColumnName = "DT_CALENDARIO"], [SourceColumnName = "DIA_MES", DestinationColumnName = "DIA_MES"], [SourceColumnName = "MES_ANO", DestinationColumnName = "MES_ANO"], [SourceColumnName = "DIA", DestinationColumnName = "DIA"], [SourceColumnName = "MES", DestinationColumnName = "MES"], [SourceColumnName = "ANO", DestinationColumnName = "ANO"], [SourceColumnName = "NO_MES", DestinationColumnName = "NO_MES"], [SourceColumnName = "NO_MES_ANO_ABV", DestinationColumnName = "NO_MES_ANO_ABV"], [SourceColumnName = "SN_FIM_SEMANA", DestinationColumnName = "SN_FIM_SEMANA"], [SourceColumnName = "NO_DECENDIO", DestinationColumnName = "NO_DECENDIO"], [SourceColumnName = "VA_DECENDIO", DestinationColumnName = "VA_DECENDIO"], [SourceColumnName = "NO_FERIADO", DestinationColumnName = "NO_FERIADO"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_CALENDARIO = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_CALENDARIO"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_FONTE_RECURSO_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "CO_GRUPO_FONTE_RECURSO_DECRETO", DestinationColumnName = "CO_GRUPO_FONTE_RECURSO_DECRETO"], [SourceColumnName = "SN_FONTE_VINCULADA", DestinationColumnName = "SN_FONTE_VINCULADA"], [SourceColumnName = "SN_ABERTURA_FONTE", DestinationColumnName = "SN_ABERTURA_FONTE"], [SourceColumnName = "TX_OBSERVACAO_RELATORIO", DestinationColumnName = "TX_OBSERVACAO_RELATORIO"], [SourceColumnName = "CO_FONTE_RECURSO", DestinationColumnName = "CO_FONTE_RECURSO"], [SourceColumnName = "NO_FONTE_RECURSO", DestinationColumnName = "NO_FONTE_RECURSO"], [SourceColumnName = "DT_CRIACAO", DestinationColumnName = "DT_CRIACAO"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_FONTE_RECURSO = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_FONTE_RECURSO"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_GRUPO_DESPESA_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_GRUPO_DESPESA", DestinationColumnName = "ID_GRUPO_DESPESA"], [SourceColumnName = "NO_GRUPO_DESPESA", DestinationColumnName = "NO_GRUPO_DESPESA"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_GRUPO_DESPESA = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_GRUPO_DESPESA"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_GRUPO_FONTE_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_GRUPO_FONTE", DestinationColumnName = "ID_GRUPO_FONTE"], [SourceColumnName = "NO_GRUPO_FONTE", DestinationColumnName = "NO_GRUPO_FONTE"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_GRUPO_FONTE = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_GRUPO_FONTE"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_MARCA_CLASSIFICACAO_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_MARCA_CLASSIFICACAO_SK", DestinationColumnName = "ID_MARCA_CLASSIFICACAO_SK"], [SourceColumnName = "NO_MARCA_CLASSIFICACAO", DestinationColumnName = "NO_MARCA_CLASSIFICACAO"], [SourceColumnName = "IN_MARCA_CLASSIFICACAO", DestinationColumnName = "IN_MARCA_CLASSIFICACAO"], [SourceColumnName = "NO_MARCA_PUBLICACAO", DestinationColumnName = "NO_MARCA_PUBLICACAO"], [SourceColumnName = "IN_GRUPO_MENSAGEM_SOTN", DestinationColumnName = "IN_GRUPO_MENSAGEM_SOTN"], [SourceColumnName = "NR_PRIORIDADE", DestinationColumnName = "NR_PRIORIDADE"], [SourceColumnName = "IN_TIPO_LANCAMENTO_SUBCONTA", DestinationColumnName = "IN_TIPO_LANCAMENTO_SUBCONTA"], [SourceColumnName = "IN_TIPO_LANCAMENTO_SUBCON_DEST", DestinationColumnName = "IN_TIPO_LANCAMENTO_SUBCON_DEST"], [SourceColumnName = "SN_PRIMARIO", DestinationColumnName = "SN_PRIMARIO"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_MARCA_CLASSIFICACAO = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_MARCA_CLASSIFICACAO"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_ORGAO_DECRETO_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_ORGAO_DECRETO_SK", DestinationColumnName = "ID_ORGAO_DECRETO_SK"], [SourceColumnName = "CO_ORGAO_DECRETO", DestinationColumnName = "CO_ORGAO_DECRETO"], [SourceColumnName = "NO_ORGAO_DECRETO_ABREVIADO", DestinationColumnName = "NO_ORGAO_DECRETO_ABREVIADO"], [SourceColumnName = "NO_ORGAO_DECRETO", DestinationColumnName = "NO_ORGAO_DECRETO"], [SourceColumnName = "DT_ATUALIZACAO", DestinationColumnName = "DT_ATUALIZACAO"], [SourceColumnName = "AN_EXERCICIO", DestinationColumnName = "AN_EXERCICIO"], [SourceColumnName = "CO_NO_ORGAO_DECRETO_ABREV", DestinationColumnName = "CO_NO_ORGAO_DECRETO_ABREV"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_ORGAO_DECRETO = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_ORGAO_DECRETO"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_ORGAO_MAXIMO_UO_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "CO_ORGAO_MAXIMO_UO", DestinationColumnName = "CO_ORGAO_MAXIMO_UO"], [SourceColumnName = "NO_ORGAO_UO", DestinationColumnName = "NO_ORGAO_UO"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_ORGAO_MAXIMO_UO = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_ORGAO_MAXIMO_UO"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_RESULTADO_EOF_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_RESULTADO_EOF", DestinationColumnName = "ID_RESULTADO_EOF"], [SourceColumnName = "NO_RESULTADO_EOF", DestinationColumnName = "NO_RESULTADO_EOF"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_RESULTADO_EOF = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_RESULTADO_EOF"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_TIPO_CREDITO_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_TIPO_CREDITO", DestinationColumnName = "ID_TIPO_CREDITO"], [SourceColumnName = "NO_TIPO_CREDITO", DestinationColumnName = "NO_TIPO_CREDITO"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_TIPO_CREDITO = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_TIPO_CREDITO"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_TIPO_EXECUCAO_DESPESA_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_TIPO_EXECUCAO_DESPESA", DestinationColumnName = "ID_TIPO_EXECUCAO_DESPESA"], [SourceColumnName = "NO_TIPO_EXECUCAO_DESPESA", DestinationColumnName = "NO_TIPO_EXECUCAO_DESPESA"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_TIPO_EXECUCAO_DESPESA = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_TIPO_EXECUCAO_DESPESA"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_UNIDADE_GESTORA_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_UNIDADE_GESTORA", DestinationColumnName = "ID_UNIDADE_GESTORA"], [SourceColumnName = "NO_UNIDADE_GESTORA", DestinationColumnName = "NO_UNIDADE_GESTORA"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_UNIDADE_GESTORA = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_UNIDADE_GESTORA"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DIM_UNIDADE_ORCAMENTARIA_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ID_UNIDADE_ORCAMENTARIA", DestinationColumnName = "ID_UNIDADE_ORCAMENTARIA"], [SourceColumnName = "NO_UNIDADE_ORCAMENTARIA", DestinationColumnName = "NO_UNIDADE_ORCAMENTARIA"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared DIM_UNIDADE_ORCAMENTARIA = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "DIM_UNIDADE_ORCAMENTARIA"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FATO_EXEC_DESP_MOV_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "AN_EXECUCAO_DESPESA_MOVIMENTO", DestinationColumnName = "AN_EXECUCAO_DESPESA_MOVIMENTO"], [SourceColumnName = "ME_EXECUCAO_DESPESA_MOVIMENTO", DestinationColumnName = "ME_EXECUCAO_DESPESA_MOVIMENTO"], [SourceColumnName = "DT_EXECUCAO_DESPESA_MOVIMENTO", DestinationColumnName = "DT_EXECUCAO_DESPESA_MOVIMENTO"], [SourceColumnName = "AN_EMISSAO_NE", DestinationColumnName = "AN_EMISSAO_NE"], [SourceColumnName = "ID_EXECUCAO_DESPESA_MOVTO_SK", DestinationColumnName = "ID_EXECUCAO_DESPESA_MOVTO_SK"], [SourceColumnName = "CO_ORGAO_MAXIMO_UO", DestinationColumnName = "CO_ORGAO_MAXIMO_UO"], [SourceColumnName = "CO_ORGAO_UG", DestinationColumnName = "CO_ORGAO_UG"], [SourceColumnName = "ID_UNIDADE_GESTORA", DestinationColumnName = "ID_UNIDADE_GESTORA"], [SourceColumnName = "CO_PROGRAMA_TRABALHO", DestinationColumnName = "CO_PROGRAMA_TRABALHO"], [SourceColumnName = "ID_FUNCAO_ORCAMENTARIA", DestinationColumnName = "ID_FUNCAO_ORCAMENTARIA"], [SourceColumnName = "ID_SUBFUNCAO_ORCAMENTARIA", DestinationColumnName = "ID_SUBFUNCAO_ORCAMENTARIA"], [SourceColumnName = "ID_PROGRAMA_ORCAMENTARIO", DestinationColumnName = "ID_PROGRAMA_ORCAMENTARIO"], [SourceColumnName = "ID_ACAO_ORCAMENTARIA", DestinationColumnName = "ID_ACAO_ORCAMENTARIA"], [SourceColumnName = "ID_LOCALIZADOR_GASTO", DestinationColumnName = "ID_LOCALIZADOR_GASTO"], [SourceColumnName = "ID_PLANO_ORCAMENTARIO_SK", DestinationColumnName = "ID_PLANO_ORCAMENTARIO_SK"], [SourceColumnName = "ID_PTRES", DestinationColumnName = "ID_PTRES"], [SourceColumnName = "CO_NATUREZA_DESPESA", DestinationColumnName = "CO_NATUREZA_DESPESA"], [SourceColumnName = "CO_FONTE_RECURSO", DestinationColumnName = "CO_FONTE_RECURSO"], [SourceColumnName = "IS_FONTE_RECURSO_DETALHE", DestinationColumnName = "IS_FONTE_RECURSO_DETALHE"], [SourceColumnName = "ID_PROGRAMACAO_SELECIONADA", DestinationColumnName = "ID_PROGRAMACAO_SELECIONADA"], [SourceColumnName = "SN_EXCECAO_DECRETO", DestinationColumnName = "SN_EXCECAO_DECRETO"], [SourceColumnName = "ID_RESULTADO_LEI", DestinationColumnName = "ID_RESULTADO_LEI"], [SourceColumnName = "ID_RESULTADO_EOF", DestinationColumnName = "ID_RESULTADO_EOF"], [SourceColumnName = "SN_LEI_CALMON", DestinationColumnName = "SN_LEI_CALMON"], [SourceColumnName = "ID_TIPO_CREDITO", DestinationColumnName = "ID_TIPO_CREDITO"], [SourceColumnName = "VA_MOVIMENTO_ITEM_INFORMACAO", DestinationColumnName = "VA_MOVIMENTO_ITEM_INFORMACAO"], [SourceColumnName = "ID_TIPO_EXECUCAO_DESPESA", DestinationColumnName = "ID_TIPO_EXECUCAO_DESPESA"], [SourceColumnName = "IN_CONTROLADO", DestinationColumnName = "IN_CONTROLADO"], [SourceColumnName = "ID_INSTRUMENTO_LEGAL_SK", DestinationColumnName = "ID_INSTRUMENTO_LEGAL_SK"], [SourceColumnName = "SN_MOEDA_REAL", DestinationColumnName = "SN_MOEDA_REAL"], [SourceColumnName = "ID_MARCA_CLASSIFICACAO_SK", DestinationColumnName = "ID_MARCA_CLASSIFICACAO_SK"], [SourceColumnName = "ID_UNIDADE_ORCAMENTARIA", DestinationColumnName = "ID_UNIDADE_ORCAMENTARIA"], [SourceColumnName = "ID_grupo_MARCA_CLASSIFICACAO_SK", DestinationColumnName = "ID_grupo_MARCA_CLASSIFICACAO_SK"], [SourceColumnName = "ID_ORGAO_DECRETO_SK", DestinationColumnName = "ID_ORGAO_DECRETO_SK"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared FATO_EXEC_DESP_MOV = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "FATO_EXEC_DESP_MOV"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FATO_EXEC_DESP_SALDO_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "AN_EXECUCAO_DESPESA_SALDO", DestinationColumnName = "AN_EXECUCAO_DESPESA_SALDO"], [SourceColumnName = "AN_EMISSAO_NE", DestinationColumnName = "AN_EMISSAO_NE"], [SourceColumnName = "ID_EXECUCAO_DESPESA_SALDO_SK", DestinationColumnName = "ID_EXECUCAO_DESPESA_SALDO_SK"], [SourceColumnName = "CO_ORGAO_MAXIMO_UO", DestinationColumnName = "CO_ORGAO_MAXIMO_UO"], [SourceColumnName = "CO_ORGAO_UG", DestinationColumnName = "CO_ORGAO_UG"], [SourceColumnName = "ID_UNIDADE_GESTORA", DestinationColumnName = "ID_UNIDADE_GESTORA"], [SourceColumnName = "CO_PROGRAMA_TRABALHO", DestinationColumnName = "CO_PROGRAMA_TRABALHO"], [SourceColumnName = "ID_PLANO_ORCAMENTARIO_SK", DestinationColumnName = "ID_PLANO_ORCAMENTARIO_SK"], [SourceColumnName = "ID_PTRES", DestinationColumnName = "ID_PTRES"], [SourceColumnName = "IN_TIPO_SALDO", DestinationColumnName = "IN_TIPO_SALDO"], [SourceColumnName = "CO_NATUREZA_DESPESA", DestinationColumnName = "CO_NATUREZA_DESPESA"], [SourceColumnName = "IS_FONTE_RECURSO_DETALHE", DestinationColumnName = "IS_FONTE_RECURSO_DETALHE"], [SourceColumnName = "ID_PROGRAMACAO_SELECIONADA", DestinationColumnName = "ID_PROGRAMACAO_SELECIONADA"], [SourceColumnName = "SN_EXCECAO_DECRETO", DestinationColumnName = "SN_EXCECAO_DECRETO"], [SourceColumnName = "ID_RESULTADO_LEI", DestinationColumnName = "ID_RESULTADO_LEI"], [SourceColumnName = "ID_RESULTADO_EOF", DestinationColumnName = "ID_RESULTADO_EOF"], [SourceColumnName = "SN_LEI_CALMON", DestinationColumnName = "SN_LEI_CALMON"], [SourceColumnName = "ID_TIPO_CREDITO", DestinationColumnName = "ID_TIPO_CREDITO"], [SourceColumnName = "VA_SALDO_ITEM_INFORMACAO", DestinationColumnName = "VA_SALDO_ITEM_INFORMACAO"], [SourceColumnName = "ID_TIPO_EXECUCAO_DESPESA", DestinationColumnName = "ID_TIPO_EXECUCAO_DESPESA"], [SourceColumnName = "IN_CONTROLADO", DestinationColumnName = "IN_CONTROLADO"], [SourceColumnName = "ID_INSTRUMENTO_LEGAL_SK", DestinationColumnName = "ID_INSTRUMENTO_LEGAL_SK"], [SourceColumnName = "CO_FONTE_RECURSO", DestinationColumnName = "CO_FONTE_RECURSO"], [SourceColumnName = "SN_MOEDA_REAL", DestinationColumnName = "SN_MOEDA_REAL"], [SourceColumnName = "ID_MARCA_CLASSIFICACAO_SK", DestinationColumnName = "ID_MARCA_CLASSIFICACAO_SK"], [SourceColumnName = "SN_IND_RESSALVADA", DestinationColumnName = "SN_IND_RESSALVADA"], [SourceColumnName = "SN_IND_OBRIG_CONT_FLUXO", DestinationColumnName = "SN_IND_OBRIG_CONT_FLUXO"], [SourceColumnName = "SN_IND_COMPENSACAO_META", DestinationColumnName = "SN_IND_COMPENSACAO_META"], [SourceColumnName = "SN_IND_CRED_EXTRA_CONTROLADO", DestinationColumnName = "SN_IND_CRED_EXTRA_CONTROLADO"], [SourceColumnName = "SN_IND_SUJEITO_TETO", DestinationColumnName = "SN_IND_SUJEITO_TETO"], [SourceColumnName = "ID_IDUSO", DestinationColumnName = "ID_IDUSO"], [SourceColumnName = "ID_ORGAO_DECRETO_SK", DestinationColumnName = "ID_ORGAO_DECRETO_SK"], [SourceColumnName = "ID_FUNCAO_ORCAMENTARIA", DestinationColumnName = "ID_FUNCAO_ORCAMENTARIA"], [SourceColumnName = "ID_SUBFUNCAO_ORCAMENTARIA", DestinationColumnName = "ID_SUBFUNCAO_ORCAMENTARIA"], [SourceColumnName = "ID_PROGRAMA_ORCAMENTARIO", DestinationColumnName = "ID_PROGRAMA_ORCAMENTARIO"], [SourceColumnName = "ID_ACAO_ORCAMENTARIA", DestinationColumnName = "ID_ACAO_ORCAMENTARIA"], [SourceColumnName = "ID_LOCALIZADOR_GASTO", DestinationColumnName = "ID_LOCALIZADOR_GASTO"], [SourceColumnName = "ID_UNIDADE_ORCAMENTARIA", DestinationColumnName = "ID_UNIDADE_ORCAMENTARIA"], [SourceColumnName = "ID_GRUPO_MARCA_CLASSIF_SK", DestinationColumnName = "ID_GRUPO_MARCA_CLASSIF_SK"], [SourceColumnName = "IS_ANEXO_DECRETO_DESP", DestinationColumnName = "IS_ANEXO_DECRETO_DESP"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared FATO_EXEC_DESP_SALDO = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "FATO_EXEC_DESP_SALDO"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FATO_LIMITES_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "IN_PERIODO", DestinationColumnName = "IN_PERIODO"], [SourceColumnName = "VA_VALOR", DestinationColumnName = "VA_VALOR"], [SourceColumnName = "VA_ANTERIOR", DestinationColumnName = "VA_ANTERIOR"], [SourceColumnName = "VA_VARIACAO", DestinationColumnName = "VA_VARIACAO"], [SourceColumnName = "VA_DESPESA_PAGA", DestinationColumnName = "VA_DESPESA_PAGA"], [SourceColumnName = "VA_RESTOS_PAGAR", DestinationColumnName = "VA_RESTOS_PAGAR"], [SourceColumnName = "VA_PAGAMENTO_EFET_ORG_DEC", DestinationColumnName = "VA_PAGAMENTO_EFET_ORG_DEC"], [SourceColumnName = "VA_TRANSFERENCIA_EXTERIOR", DestinationColumnName = "VA_TRANSFERENCIA_EXTERIOR"], [SourceColumnName = "DT_ATO", DestinationColumnName = "DT_ATO"], [SourceColumnName = "ID_LIMITE_EMPENHO_PAGTO_SK", DestinationColumnName = "ID_LIMITE_EMPENHO_PAGTO_SK"], [SourceColumnName = "ID_LIMITE_EMPENHO_PAGTO_VAL_SK", DestinationColumnName = "ID_LIMITE_EMPENHO_PAGTO_VAL_SK"], [SourceColumnName = "ID_ORGAO_DECRETO_SK", DestinationColumnName = "ID_ORGAO_DECRETO_SK"], [SourceColumnName = "ID_ANEXO_DECRETO_SK", DestinationColumnName = "ID_ANEXO_DECRETO_SK"], [SourceColumnName = "ID_INSTRUMENTO_LEGAL_LIMITE_SK", DestinationColumnName = "ID_INSTRUMENTO_LEGAL_LIMITE_SK"], [SourceColumnName = "SN_VIGENTE", DestinationColumnName = "SN_VIGENTE"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared FATO_LIMITES = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "FATO_LIMITES"]}[Data]
in
  #"Navegação 2";
[BindToDefaultDestination = true]
shared FATO_LIMITES_PAGEFET = let
  Origem = Oracle.Database("exastncl03-tlm0m-scan.prosubnet.vcndados.oraclevcn.com/DWPROD.prosubnet.vcndados.oraclevcn.com", [HierarchicalNavigation = true]),
  #"Navegação 1" = Origem{[Schema = "DEV_SIGEFI"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Name = "FATO_LIMITES_PAGEFET"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Aux - orgaos_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "CO_ORGAO_DECRETO", DestinationColumnName = "CO_ORGAO_DECRETO"], [SourceColumnName = "ID_ORGAO_DECRETO_SK", DestinationColumnName = "ID_ORGAO_DECRETO_SK"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"Aux - orgaos" = let
    Fonte = Excel.Workbook(File.Contents("\\cruzeiro.stn.intra\Dados_ANALITICOS\DADOS-DAS-AREAS\COFIN\DPOF\Aux orgaos.xlsx"), null, true),
    Table1_Table = Fonte{[Item="Table1",Kind="Table"]}[Data],
    #"Tipo Alterado" = Table.TransformColumnTypes(Table1_Table,{{"CO_ORGAO_DECRETO", type text}, {"ID_ORGAO_DECRETO_SK", type text}})
in
    #"Tipo Alterado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Aux - Limites_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Anexo Genef", DestinationColumnName = "Anexo Genef"], [SourceColumnName = "Anexo Gepof", DestinationColumnName = "Anexo Gepof"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"Aux - Limites" = let
    Fonte = Excel.Workbook(File.Contents("\\cruzeiro.stn.intra\Dados_ANALITICOS\DADOS-DAS-AREAS\COFIN\DPOF\Aux Limites.xlsx"), null, true),
    Sheet1_Sheet = Fonte{[Item="Sheet1",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(Sheet1_Sheet, [PromoteAllScalars=true]),
  #"Tipo de coluna alterado" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos", {{"Anexo Genef", type text}, {"Anexo Gepof", type text}})
in
    #"Tipo de coluna alterado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Limites provisórios_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Orgãos Código", DestinationColumnName = "Orgãos Código"], [SourceColumnName = "Orgãos Nome", DestinationColumnName = "Orgãos Nome"], [SourceColumnName = "Anexo", DestinationColumnName = "Anexo"], [SourceColumnName = "Valor", DestinationColumnName = "Valor"], [SourceColumnName = "Mes", DestinationColumnName = "Mes"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"Limites provisórios" = let
    Fonte = Excel.Workbook(File.Contents("\\cruzeiro.stn.intra\Dados_ANALITICOS\DADOS-DAS-AREAS\COFIN\DPOF\Limites longo.xlsx"), null, true),
    Sheet1_Sheet = Fonte{[Item="Sheet1",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(Sheet1_Sheet, [PromoteAllScalars=true]),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos",{{"Orgãos Código", type text}, {"Orgãos Nome", type text}, {"Anexo", type text}, {"Valor", type number}, {"Mes", type text}})
in
    #"Tipo Alterado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Aux - Mês_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Cód. Mês-1", DestinationColumnName = "Cód. Mês-1"], [SourceColumnName = "Cód. Mês-2", DestinationColumnName = "Cód. Mês-2"], [SourceColumnName = "Nome Mês", DestinationColumnName = "Nome Mês"], [SourceColumnName = "Cód. Nome Mês", DestinationColumnName = "Cód. Nome Mês"], [SourceColumnName = "Mês", DestinationColumnName = "Mês"], [SourceColumnName = "Data", DestinationColumnName = "Data"], [SourceColumnName = "Cód. Nome Mês Completo", DestinationColumnName = "Cód. Nome Mês Completo"], [SourceColumnName = "Mês 2023", DestinationColumnName = "Mês 2023"], [SourceColumnName = "Data 2023", DestinationColumnName = "Data 2023"], [SourceColumnName = "IN_PERIODO", DestinationColumnName = "IN_PERIODO"], [SourceColumnName = "Mês_1", DestinationColumnName = "Mês_1"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"Aux - Mês" = let
    Fonte = Excel.Workbook(File.Contents("\\cruzeiro.stn.intra\Dados_ANALITICOS\DADOS-DAS-AREAS\COFIN\Painel por Órgão\Mês.xlsx"), null, true),
    Planilha1_Sheet = Fonte{[Item="Planilha1",Kind="Sheet"]}[Data],
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(Planilha1_Sheet, [PromoteAllScalars=true]),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos",{{"Cód. Mês-1", Int64.Type}, {"Cód. Mês-2", Int64.Type}, {"Nome Mês", type text}, {"Cód. Nome Mês", type text}, {"Mês", type text}, {"Data", type date}, {"Cód. Nome Mês Completo", type text}, {"Mês 2023", type text}, {"Data 2023", type date}, {"IN_PERIODO", Int64.Type}, {"Mês_1", type text}})
in
    #"Tipo Alterado";
shared #"Data de atualização" = let
    Fonte = DateTime.ToText(#datetime(Date.Year(DateTime.FixedLocalNow()), Date.Month(DateTime.FixedLocalNow()), Date.Day(DateTime.FixedLocalNow()),Time.Hour(DateTime.FixedLocalNow()), Time.Minute(DateTime.FixedLocalNow()), Time.Second(DateTime.FixedLocalNow())))
in
    Fonte;
shared DIM_ACAO_ORCAMENTARIA_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_ACAO_ORCAMENTARIA"]}[Data]
in
  TableNavigation;
shared DIM_ANEXO_DECRETO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_ANEXO_DECRETO"]}[Data]
in
  TableNavigation;
shared DIM_ANEXO_DECRETO_DESP_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_ANEXO_DECRETO_DESP"]}[Data]
in
  TableNavigation;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data]{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data];
shared DIM_CALENDARIO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_CALENDARIO"]}[Data]
in
  TableNavigation;
shared DIM_FONTE_RECURSO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_FONTE_RECURSO"]}[Data]
in
  TableNavigation;
shared DIM_GRUPO_DESPESA_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_GRUPO_DESPESA"]}[Data]
in
  TableNavigation;
shared DIM_GRUPO_FONTE_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_GRUPO_FONTE"]}[Data]
in
  TableNavigation;
shared DIM_MARCA_CLASSIFICACAO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_MARCA_CLASSIFICACAO"]}[Data]
in
  TableNavigation;
shared DIM_ORGAO_DECRETO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_ORGAO_DECRETO"]}[Data]
in
  TableNavigation;
shared DIM_ORGAO_MAXIMO_UO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_ORGAO_MAXIMO_UO"]}[Data]
in
  TableNavigation;
shared DIM_RESULTADO_EOF_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_RESULTADO_EOF"]}[Data]
in
  TableNavigation;
shared DIM_TIPO_CREDITO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_TIPO_CREDITO"]}[Data]
in
  TableNavigation;
shared DIM_TIPO_EXECUCAO_DESPESA_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_TIPO_EXECUCAO_DESPESA"]}[Data]
in
  TableNavigation;
shared DIM_UNIDADE_GESTORA_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_UNIDADE_GESTORA"]}[Data]
in
  TableNavigation;
shared DIM_UNIDADE_ORCAMENTARIA_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "DIM_UNIDADE_ORCAMENTARIA"]}[Data]
in
  TableNavigation;
shared FATO_EXEC_DESP_MOV_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "FATO_EXEC_DESP_MOV"]}[Data]
in
  TableNavigation;
shared FATO_EXEC_DESP_SALDO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "FATO_EXEC_DESP_SALDO"]}[Data]
in
  TableNavigation;
shared FATO_LIMITES_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "FATO_LIMITES"]}[Data]
in
  TableNavigation;
shared #"Aux - orgaos_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "Aux - orgaos"]}[Data]
in
  TableNavigation;
shared #"Aux - Limites_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "Aux - Limites"]}[Data]
in
  TableNavigation;
shared #"Limites provisórios_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "Limites provisórios"]}[Data]
in
  TableNavigation;
shared #"Aux - Mês_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "ca9abfd5-a0fe-4e6b-a34f-3df02e037310"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "Aux - Mês"]}[Data]
in
  TableNavigation;
