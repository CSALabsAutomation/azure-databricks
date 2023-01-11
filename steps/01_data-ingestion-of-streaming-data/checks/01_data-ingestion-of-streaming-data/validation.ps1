param(
     [String]
     $Resourcegroupname
)

$Namespace = Get-AzResource -ResourceGroupName $Resourcegroupname -Resourcetype Microsoft.EventHub/namespaces
$Namespacename = $Namespace.Name
   
$EventHubslist = Get-AzEventHub -NamespaceName $Namespacename -ResourceGroupName $Resourcegroupname
foreach($i in $EventHubslist)
{
        if($i.Name -like "retail.public.customers")
        {
            $eventHub1 = $i.Name
        }

        elseif($i.Name -like "retail.public.products")
        {
            $eventHub2 = $i.Name
        }

        elseif($i.Name -like "retail.public.sales_orders")
        {
            $eventHub3 = $i.Name
        }
}

if($eventHub1 -and $eventHub2 -and $eventHub3){
    Write-Host "Event Hubs for Customers, Products and Sales-Orders are successfully created."
}
else{
    Write-Host "Event Hubs are not created."
}

$ConsumerGrouplist = Get-AzEventHubConsumerGroup -NamespaceName $Namespacename -ResourceGroupName $Resourcegroupname -EventHubName retail.public.sales_orders
foreach($i in $ConsumerGrouplist)
{
        if($i.Name -like "stream-salesorder-cg")
        {
            $Jobname = $i.Name
        }
}

if($Jobname){
    Write-Host "stream-salesorder job is created successfully."
}
else{
    Write-Host "stream-salesorder job is not created."
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
$BlobList = Get-AzStorageBlob -Container "data" -Context $ctx
if(($BlobList).Count -gt 0)
{
    Write-Host "The sales-order changes is streamed and saved successfully."
}
else{
    Write-Host "The Sales-orders changes is not streamed."
}
