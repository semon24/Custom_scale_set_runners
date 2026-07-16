#!/bin/bash

MEMORY_THRESHOLD=20

MEMORY_FREE=$(free | grep Mem | awk '{print $4/$2 * 100.0}' | cut -d. -f1)

echo "$(date): Проверка памяти. Свободно: ${MEMORY_FREE}%" >> /var/log/memory-clean.log

if [ $MEMORY_FREE -lt $MEMORY_THRESHOLD ]; then
    echo "$(date): Свободной памяти ${MEMORY_FREE}% (меньше ${MEMORY_THRESHOLD}%). Запускаем очистку Docker..." >> /var/log/memory-clean.log
    
    docker image prune --all --force >> /var/log/memory-clean.log 2>&1
    
    docker buildx prune --all --force >> /var/log/memory-clean.log 2>&1
    
    echo "$(date): Очистка завершена" >> /var/log/memory-clean.log
else
    echo "$(date): Памяти достаточно (${MEMORY_FREE}%). Очистка не требуется." >> /var/log/memory-clean.log
fi