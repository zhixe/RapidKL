#!/bin/bash

while true; do
    dir="$(pwd)/data"
    cd "$dir" || exit

    paste -d " " <(find . -maxdepth 1 -name "*.csv" | awk -F/ '{print $NF}') <(find . -maxdepth 1 -name "*.csv" | awk '{print tolower($0)}' | awk -F/ '{print $NF}') |
    while read -r a b
        do
            mv -- "$a" "$b" || exit
    done
    cd - > /dev/null || exit

    # Wait for 10 seconds before the next iteration
    sleep 10
done
