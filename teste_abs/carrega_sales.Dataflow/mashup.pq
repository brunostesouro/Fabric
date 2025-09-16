[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "94061179-8192-4e7e-a03f-32adf4afa670"]}[Data]{[lakehouseId = "9a99f21d-4de8-4298-a01b-3447b08b9d08"]}[Data];
[BindToDefaultDestination = true]
shared sales = let
  Origem = Csv.Document(Web.Contents("https://tesouro-my.sharepoint.com/personal/bruno_silva_tesouro_gov_br/Documents/Documentos/sales.csv"), [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Cabeçalhos promovidos" = Table.PromoteHeaders(Origem, [PromoteAllScalars = true]),
  #"Tipo de coluna alterado" = Table.TransformColumnTypes(#"Cabeçalhos promovidos", {{"SalesOrderNumber", type text}, {"SalesOrderLineNumber", Int64.Type}, {"OrderDate", type date}, {"CustomerName", type text}, {"EmailAddress", type text}, {"Item", type text}, {"Quantity", Int64.Type}, {"UnitPrice", Int64.Type}, {"TaxAmount", Int64.Type}})
in
  #"Tipo de coluna alterado";
