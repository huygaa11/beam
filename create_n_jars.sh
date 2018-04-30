#!/bin/bash

n="$1"

echo "numbers of jar created: $n"

touch file.txt

for ((i=1; i<=n; i++)); do
  jar cf "jar${i}.jar" "file.txt"
done

rm file.txt
