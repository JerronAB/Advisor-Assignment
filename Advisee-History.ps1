$rootDirectory = "//OW-PE2800/Major-Advisor-Changes/"
$allFiles = Get-ChildItem -Recurse "$rootDirectory*all*temp.*sv" #get tsv and csv's with "temp" at the end of the name
$allFiles = ($allFiles | Sort-Object -Property CreationTime)

$studentID = "002500082"
if ($StudentID.Length -gt 9) {
        # Raise an exception if the condition is met
        throw "StudentID '$StudentID' is too long. It must be 9 characters or less."
}

#Create versions with and without leading 0's
$studentIDList = (
    $studentID.PadLeft(9,"0"), 
    [convert]::ToString(([convert]::ToUint64($studentID)))
)

$found = @()
$allFiles | ForEach-Object {
    $file = $_
    Write-Output "Now parsing file: $($file.BaseName.PadRight(31," ")) from $($file.CreationTime) --" | Tee-Object -FilePath "../RunLog_$($StudentIDList[0])_temp.txt" -Append
    $fileContent = (Get-Content $file)
    foreach ($stdID in $studentIDList) { #need to use a "traditional" foreach loop here so that the break, below, works correctly
        $found += ($fileContent | Where-Object { $_ -match $stdID})
        ($fileContent | Where-Object { $_ -match $stdID}) | ForEach-Object {Write-Output "    -- $($_.Replace("`t",","))" | Tee-Object -FilePath "../RunLog_$($StudentIDList[0])_temp.txt" -Append}
        if (($fileContent | Where-Object { $_ -match $stdID}).Length -ne 0) {break} #no need to search using second StudentID format if we already found one
    }
}
$found = $found.Replace("`t",",")
$found | Set-Content -Path "../all_instances_$($StudentIDList[0])_temp.txt"