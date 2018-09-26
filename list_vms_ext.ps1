param([String]$outDir = ".", [String]$username, [String]$password, [String]$vcsahost, [String]$vmid = "")
Get-Module -ListAvailable PowerCLI* | Import-Module
Connect-VIServer -Server $vcsahost -User $username -Password $password | Out-Null

If ($vmid -eq "") { $vm_list = Get-VM }
Else { $vm_list = Get-VM -Id $vmid }

$vm_list | ConvertTo-Csv | Out-File "$outDir/list.csv"

foreach($vm in $vm_list) {
	$id = $vm.PersistentId
	Get-NetworkAdapter -VM $vm | ConvertTo-Csv | Out-File "$outDir/macs-$id.csv"
	$vm | Select Name, @{N="IP Address";E={@($_.guest.IPAddress[0])}} | ConvertTo-Csv | Out-File "$outDir/ips-$id.csv"
	$tag_assigments = Get-TagAssignment -Entity $vm
	$tags = foreach($tag_assigment in $tag_assigments) { $tag_assigment.tag }
	$tags | ConvertTo-Csv | Out-File "$outDir/tags-$id.csv"
}
