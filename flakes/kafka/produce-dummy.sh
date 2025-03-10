#!/bin/bash

TOPIC="test-topic"
BROKER="localhost:9092"

# Function to generate a random name
generate_name() {
    names=("Alice" "Bob" "Charlie" "David" "Emma" "Frank" "Grace" "Hannah" "Ian" "Jack" "Kate")
    echo "${names[$RANDOM % ${#names[@]}]}"
}

# Function to generate a random UUID
generate_id() {
    date +%s
}

generate_clicks() {
    echo $(( (RANDOM % 10) + 1 ))
}

# Loop to send messages every second
while true; do
    ID=$(generate_id)
    NAME=$(generate_name)
    CLICKS=$(generate_clicks)
    CREATED_AT=$(date '+%Y-%m-%d %H:%M:%S')

    MESSAGE="{\"id\":\"$ID\", \"name\":\"$NAME\", \"clicks\":$CLICKS , \"created_at\":\"$CREATED_AT\"}"

    echo "Producing: $MESSAGE"
    echo "$MESSAGE" | kafka-console-producer.sh --broker-list $BROKER --topic $TOPIC > /dev/null

    # sleep 0.01  # Adjust this interval if needed
done
