#!/bin/bash

ACT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd)"

echo $ACT_DIR

LOG_FILE="$ACT_DIR/log/logs_main.log"
exec > >(tee -a "$LOG_FILE") 2>&1

start_time=$(date +%s)

echo -e "Inicio de ejecucion: $(date)\n"

if ! mountpoint -q "$ACT_DIR/shared/10.128.156.112";
then
    
    echo "Mounting folder 10.128.156.112"
    sudo mount -t cifs //10.128.156.112/files/ "$ACT_DIR/shared/10.128.156.112" -o username=E015379,password=Martine55,vers=3.0

else
    echo "Folder mounted 10.128.156.112 succesfully"
fi

if ! mountpoint -q "$ACT_DIR/shared/10.128.108.19";
then
    
    echo "Mounting folder 10.128.108.19"
    sudo mount -t cifs //10.128.108.19/Archivos "$ACT_DIR/shared/10.128.108.19" -o username=E019588,password=Hola2403,vers=3.0

else
    echo "Folder mounted 10.128.108.19 succesfully"
fi

VENV="$ACT_DIR/venv"
source "$VENV/bin/activate"

python3 "$ACT_DIR/main.py"
deactivate

echo "Process executed"

#sudo umount -f "$ACT_DIR/shared/10.128.156.112"
#sudo umount -f "$ACT_DIR/shared/10.128.108.119"
#echo -e "Folders unmonted \n"

end_time=$(date +%s)
runtime=$((end_time-start_time))
formatted_runtime=$(date -u -d @"$runtime" +'%M minutos y %S segundos')
echo -e "Fin de ejecucion: $(date)\n"
echo -e "Tiempo de ejecucion: $formatted_runtime \n \n \n"