C:\"Program Files (x86)"\"Pivot Table"\Pivot.exe "C:\Users\leado\Documents\nemind Pivot Table 3\wayprokat1.npt;C:\Users\leado\Documents\nemind Pivot Table 3\wayprokat2.npt" /sendemail  "a.sokolov@aura-rent.ru;info@hellodata.ru" "БД Wayprokat синхронизирована с Вашим YandexDisk"
Start-Sleep -Seconds 180
Copy-Item -Path "C:\Users\leado\Documents\nemind Pivot Table 3\Cache\Bitrix24\way-prokat_78.sdb" -Destination "C:\Users\leado\YandexDisk\dashboard_wayprokat\db\way-prokat_78.sdb" -Force
Start-Sleep -Seconds 900
Copy-Item -Path "C:\Users\leado\Documents\nemind Pivot Table 3\Cache\Bitrix24\way-prokat_78.sdb" -Destination "C:\Users\leado\YandexDisk\dashboard_wayprokat\db\way-prokat_78.sdb" -Force
$Song = New-Object System.Media.SoundPlayer
$Song.SoundLocation = "C:\Dropbox\MTSRUN\Sound\beep.wav"
$Song.Play()