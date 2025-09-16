CREATE TABLE [dbo].[DIM_CALENDARIO] (

	[DT_CALENDARIO] datetime2(6) NULL, 
	[DIA_MES] varchar(8000) NULL, 
	[MES_ANO] varchar(8000) NULL, 
	[DIA] decimal(38,6) NULL, 
	[MES] decimal(38,6) NULL, 
	[ANO] decimal(38,6) NULL, 
	[NO_MES] varchar(8000) NULL, 
	[NO_MES_ANO_ABV] varchar(8000) NULL, 
	[SN_FIM_SEMANA] varchar(8000) NULL, 
	[NO_DECENDIO] varchar(8000) NULL, 
	[VA_DECENDIO] decimal(38,6) NULL, 
	[NO_FERIADO] varchar(8000) NULL
);