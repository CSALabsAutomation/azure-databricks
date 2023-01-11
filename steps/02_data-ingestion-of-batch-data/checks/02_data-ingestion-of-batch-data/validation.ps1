param(
    [string]
    $Resourcegroupname
)

$WorkSpace = Get-AzResource -ResourceGroupName $Resourcegroupname -Resourcetype Microsoft.Synapse/workspaces
$WorkSpaceName = $WorkSpace.Name

$PipelineList = Get-AzSynapsePipeline -WorkspaceName $WorkSpaceName
foreach($i in $PipelineList)
{
    if ($i.Name -like 'getpostgrestables') {
        $PipelineName = $i.Name
    }
}

if($PipelineName){
    Write-Host "Pipeline is created successfully"
}

else{
    Write-Host "Pipeline is not created."
}

$AdlsAccount = Get-AzResource -ResourceGroupName $Resourcegroupname -Resourcetype Microsoft.Storage/storageAccounts
foreach($i in $AdlsAccount)
{
    if($i.Name -match 'adls\w+')
    {
        $AdlsAccountName = $i.Name
    }
}


$ctx = New-AzStorageContext -StorageAccountName $AdlsAccountName -UseConnectedAccount
$BlobCustomers = Get-AzStorageBlob -Container "data" -Blob "customers/customers.parquet" -Context $ctx
$BlobProducts = Get-AzStorageBlob -Container "data" -Blob "products/products.parquet" -Context $ctx
$BlobSalesOrders = Get-AzStorageBlob -Container "data" -Blob "sales_orders/sales_orders.parquet" -Context $ctx

if ($BlobCustomers -and $BlobProducts -and $BlobSalesOrders)
{
    Write-Host "The batch data is processed successfully."
}

else{
    Write-Host "The batch data is not processed."
}
